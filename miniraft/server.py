"""One real miniraft node: a process with a Raft node, a key-value store, a TCP port and a data directory.

    python server.py --id 1 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data data/n1

Clients and peers share the one port. A client sends one JSON line per request, e.g.
{"c": "alice", "s": 1, "op": "put", "k": "x", "v": "1"}, and gets one JSON line back: {"ok": ...} or
{"error": ...}. Only the leader answers; any other node replies {"error": "not leader", "leader": id}.
A request that times out may or may not have run, so the client resends it with the SAME (c, s): the
session table makes the retry safe (exactly-once). {"op": "status"} describes the node.

Kill it with SIGKILL at any moment: every acknowledged write is on disk (RealDisk fsyncs before the
node replies), and the node rebuilds itself from its journal and snapshot when started again.
"""
import argparse
import asyncio
import json
import signal
import sys
import time

from kv import KVStore
from raft import LEADER, RaftNode
from realdisk import RealDisk
from realnet import RealClock, RealNetwork
from storage import RaftStorage
from wire import parse_cluster


def _stderr_log(text):
    print(f"{time.strftime('%H:%M:%S')} {text}", file=sys.stderr, flush=True)


class Server:
    def __init__(self, node_id, addrs, data_dir, snapshot_every=50, election_timeout=(150.0, 300.0), heartbeat=50.0,
                 request_timeout=5.0, log=_stderr_log):
        self.id, self.addrs, self.data_dir = node_id, dict(addrs), data_dir
        self.snapshot_every, self.election_timeout, self.heartbeat = snapshot_every, election_timeout, heartbeat
        self.request_timeout, self.log = request_timeout, log
        self.store = KVStore()
        self.waiting = {}                                 # (client, seq) -> futures of requests waiting for that command to apply
        self.node = self.net = self.disk = None

    async def start(self):
        self.clock = RealClock(asyncio.get_running_loop())
        self.net = RealNetwork(self.addrs, self.id, on_client=self._on_client, log=self.log)
        self.disk = RealDisk(self.data_dir)
        self.node = RaftNode(self.id, self.clock, self.net, sorted(self.addrs), self.election_timeout, self.heartbeat,
                             on_leader=self._on_leader, on_apply=self._on_apply, storage=RaftStorage(self.disk),
                             snapshot_threshold=self.snapshot_every, snapshot_fn=lambda node: self.store.snapshot(),
                             restore_fn=self._restore, on_snapshot=self._on_snapshot)
        await self.net.start()
        self.node.start()
        host, port = self.addrs[self.id]
        self.log(f"node {self.id} listening on {host}:{port}, data in {self.data_dir}, "
                 f"term {self.node.current_term}, log {self.node.base}+{len(self.node.log)} entries")

    async def stop(self):
        """Stop like a clean shutdown (the tests use it to take a node down without killing a process)."""
        if self.node is not None:
            self.node.crash()                             # cancels the timers
        for futures in self.waiting.values():
            for future in futures:
                future.cancel()
        self.waiting.clear()
        if self.net is not None:
            await self.net.close()
        if self.disk is not None:
            self.disk.close()

    # ----- callbacks from the Raft node

    def _restore(self, node, state):
        self.store = KVStore.from_snapshot(state)

    def _on_leader(self, node):
        self.log(f"node {self.id} became leader of term {node.current_term}")

    def _on_snapshot(self, node, index, term, state):
        self.log(f"node {self.id} has a snapshot up to entry {index} (log now {len(node.log)} entries)")

    def _on_apply(self, node, index, entry):
        node.applied.clear()                              # the state machine is the KV store; do not also keep every command in RAM
        command = entry.command
        if command is None:
            return                                        # a new leader's no-op
        result = self.store.apply(command)
        if not isinstance(command, dict):
            return
        for future in self.waiting.pop((command.get("c"), command.get("s")), ()):
            if not future.done():
                future.set_result(result)

    # ----- clients

    def status(self):
        n = self.node
        return {"id": self.id, "state": n.state, "term": n.current_term, "leader": n.leader_id, "commit_index": n.commit_index,
                "last_applied": n.last_applied, "base": n.base, "log_entries": len(n.log), "snapshots_taken": n.snapshots_taken,
                "snapshots_installed": n.snapshots_installed, "keys": len(self.store.data), "executed": self.store.executed}

    async def _on_client(self, request, writer):
        reply = await self._handle(request)
        writer.write(json.dumps(reply, separators=(",", ":")).encode() + b"\n")
        await writer.drain()

    async def _handle(self, request):
        if not isinstance(request, dict):
            return {"error": "bad request"}
        if request.get("op") == "status":
            return self.status()
        client, seq = request.get("c"), request.get("s")
        if (not isinstance(client, (str, int)) or isinstance(client, bool) or not isinstance(seq, int)
                or isinstance(seq, bool) or not isinstance(request.get("op"), str)):
            return {"error": "malformed command"}
        if self.node.state != LEADER:
            return {"error": "not leader", "leader": self.node.leader_id}
        future = asyncio.get_running_loop().create_future()
        waiters = self.waiting.setdefault((client, seq), [])
        waiters.append(future)                            # register first: a one-node cluster applies inside propose()
        if self.node.propose(request) is None:
            waiters.remove(future)
            return {"error": "not leader", "leader": self.node.leader_id}
        try:
            return await asyncio.wait_for(future, self.request_timeout)
        except asyncio.TimeoutError:
            return {"error": "timeout"}                   # it may still have run: the client retries with the same seq
        finally:
            if future in waiters:
                waiters.remove(future)
            if not waiters:
                self.waiting.pop((client, seq), None)


async def run(args):
    server = Server(args.id, parse_cluster(args.cluster), args.data, snapshot_every=args.snapshot_every,
                    election_timeout=tuple(float(x) for x in args.election_ms.split(",")), heartbeat=args.heartbeat_ms)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await server.start()
    await stop.wait()
    server.log(f"node {args.id} shutting down")
    await server.stop()


def main(argv=None):
    p = argparse.ArgumentParser(description="Run one miniraft node.")
    p.add_argument("--id", type=int, required=True, help="this node's id (a key of --cluster)")
    p.add_argument("--cluster", required=True, help="ID=HOST:PORT,... for every node, including this one")
    p.add_argument("--data", required=True, help="directory for the journal and the snapshot")
    p.add_argument("--snapshot-every", type=int, default=50, help="snapshot after this many applied entries (default 50)")
    p.add_argument("--election-ms", default="150,300", help="election timeout range in ms (default 150,300)")
    p.add_argument("--heartbeat-ms", type=float, default=50.0)
    args = p.parse_args(argv)
    if args.id not in parse_cluster(args.cluster):
        p.error(f"--id {args.id} is not in --cluster")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
