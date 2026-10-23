"""Real time and real TCP for the Raft node, in place of the simulator's clock and network.

RaftNode only needs two things from its surroundings: something with now / rng / schedule(delay, fn)
(the simulator) and something with register / send (the network). RealClock and RealNetwork provide
the same interface on top of asyncio, so raft.py runs UNCHANGED on real machines. Times are milliseconds.

Design, deliberately simple:
  * one listening port per node; every line received is JSON;
  * a line with an "m" key is a Raft message from a peer, anything else goes to on_client (the KV server);
  * each node keeps one outgoing connection per peer and reconnects forever with a small backoff;
  * a message sent while the connection is down is simply dropped. Raft is built for lost messages,
    so there are no queues, no retries and no acknowledgements at this layer.
Everything runs on one thread (one asyncio loop), so node code never needs locks. The price is that a
slow disk write (fsync) blocks the whole node for a moment. There is no authentication or encryption:
use it on a trusted network only.
"""
import asyncio
import json
import random
import sys
import time
import traceback

from wire import WireError, decode, encode

MAX_LINE = 64 * 1024 * 1024                               # one line must hold a whole snapshot (single-chunk InstallSnapshot)
MAX_BUFFERED = 8 * 1024 * 1024                            # a peer this far behind on reading is treated as down


class RealClock:
    def __init__(self, loop, seed=None):
        self.loop, self.rng = loop, random.Random(seed)
        self._start = time.monotonic()

    @property
    def now(self):
        return (time.monotonic() - self._start) * 1000.0

    def schedule(self, delay, fn):
        return self.loop.call_later(max(delay, 0.0) / 1000.0, fn)       # the handle has cancel(), like the simulator's Timer


class RealNetwork:
    def __init__(self, addrs, my_id, on_client=None, log=None):
        self.addrs, self.my_id, self.on_client = dict(addrs), my_id, on_client
        self.log = log or (lambda text: print(text, file=sys.stderr, flush=True))
        self.nodes = {}
        self.blocked = set()                              # peers we pretend not to hear (tests use it to cut links)
        self.stats = {"sent": 0, "dropped": 0, "delivered": 0, "bad": 0}
        self._out, self._tasks, self._inbound = {}, [], set()
        self._server, self._closing = None, False

    def register(self, node):
        if node.id != self.my_id:
            raise ValueError(f"this network belongs to node {self.my_id}, not {node.id}")
        self.nodes[node.id] = node

    async def start(self):
        host, port = self.addrs[self.my_id]
        self._server = await asyncio.start_server(self._on_connection, host, port, limit=MAX_LINE)
        for peer in self.addrs:
            if peer != self.my_id:
                self._tasks.append(asyncio.ensure_future(self._supervise(peer)))

    async def close(self):
        self._closing = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        writers = list(self._out.values()) + list(self._inbound)
        for writer in writers:
            writer.close()
        self._out.clear()
        if writers:
            await asyncio.wait([asyncio.ensure_future(w.wait_closed()) for w in writers], timeout=2.0)
        if self._server is not None:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), 2.0)
            except asyncio.TimeoutError:
                pass

    # ----- sending: fire and forget

    def send(self, src, dst, msg):
        writer = self._out.get(dst)
        if dst in self.blocked or writer is None or writer.is_closing() or writer.transport.get_write_buffer_size() > MAX_BUFFERED:
            self.stats["dropped"] += 1
            return
        line = json.dumps({"from": src, "m": encode(msg)}, separators=(",", ":")).encode() + b"\n"
        writer.write(line)
        self.stats["sent"] += 1

    @staticmethod
    async def _connect(host, port, timeout=1.0):
        """open_connection with a timeout that never leaks a socket, even if we are cancelled just as it connects."""
        attempt = asyncio.ensure_future(asyncio.open_connection(host, port, limit=MAX_LINE))
        try:
            return await asyncio.wait_for(asyncio.shield(attempt), timeout)
        except BaseException:
            def close_if_it_connected(done):
                if not done.cancelled() and done.exception() is None:
                    done.result()[1].close()
            attempt.add_done_callback(close_if_it_connected)
            attempt.cancel()
            raise

    async def _supervise(self, peer):
        """Keep one outgoing connection to `peer` alive, reconnecting forever."""
        delay = 0.05
        while not self._closing:
            host, port = self.addrs[peer]
            try:
                reader, writer = await self._connect(host, port)
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.0)
                continue
            delay = 0.05
            self._out[peer] = writer
            try:
                while await reader.read(4096):            # the peer never talks back on this connection; this just notices the close
                    pass
            except OSError:
                pass
            finally:
                if self._out.get(peer) is writer:
                    del self._out[peer]
                writer.close()
            await asyncio.sleep(0.05)

    # ----- receiving

    async def _on_connection(self, reader, writer):
        self._inbound.add(writer)
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                await self._handle_line(line, writer)
        except (OSError, ValueError, asyncio.IncompleteReadError):      # reset, or a line longer than MAX_LINE
            pass
        finally:
            self._inbound.discard(writer)
            writer.close()

    async def _handle_line(self, line, writer):
        try:
            obj = json.loads(line)
        except ValueError:
            self.stats["bad"] += 1
            return
        try:
            if isinstance(obj, dict) and "m" in obj:
                self._deliver(obj)
            elif self.on_client is not None:
                await self.on_client(obj, writer)
        except WireError as e:
            self.stats["bad"] += 1
            self.log(f"ignored a bad message: {e}")
        except Exception:                                 # a bug in node code must not take the connection loop with it
            self.log("error while handling a message:\n" + traceback.format_exc())

    def _deliver(self, obj):
        src = obj.get("from")
        if not isinstance(src, int) or isinstance(src, bool) or src not in self.addrs or src == self.my_id:
            raise WireError(f"message from unknown sender {src!r}")
        if src in self.blocked:
            return
        msg = decode(obj["m"])
        node = self.nodes.get(self.my_id)
        if node is not None and node.up:
            self.stats["delivered"] += 1
            node.on_message(src, msg)
