"""A deterministic simulator for distributed systems: virtual time, an unreliable network, crashes.

Nothing here uses real clocks, threads or sockets. Time is a number that jumps from event to event,
and every random choice comes from one seeded generator, so a run is a pure function of its seed:
a failing test can be replayed exactly, as many times as needed.

    sim = Simulator(seed=7)
    net = Network(sim, latency=(1, 5), drop=0.1)
    a, b = Node(1, sim, net), Node(2, sim, net)
    a.on_message = lambda src, msg: print(sim.now, "got", msg, "from", src)
    net.send(2, 1, "hello")
    sim.run(until=100)
"""
import copy
import heapq
import random


class SimulationError(Exception):
    pass


class Timer:
    """Handle for a scheduled callback. cancel() stops it from ever running."""

    __slots__ = ("time", "seq", "fn", "cancelled")

    def __init__(self, time, seq, fn):
        self.time, self.seq, self.fn, self.cancelled = time, seq, fn, False

    def cancel(self):
        self.cancelled = True

    def __lt__(self, other):
        return (self.time, self.seq) < (other.time, other.seq)


class Simulator:
    """Virtual clock plus an event queue. Events at the same time run in the order they were scheduled."""

    def __init__(self, seed=0):
        self.now = 0.0
        self.rng = random.Random(seed)
        self.seed = seed
        self._queue = []
        self._seq = 0
        self.events_run = 0

    def schedule(self, delay, fn):
        if delay < 0:
            raise SimulationError(f"cannot schedule in the past (delay {delay})")
        self._seq += 1
        timer = Timer(self.now + delay, self._seq, fn)
        heapq.heappush(self._queue, timer)
        return timer

    def pending(self):
        return sum(1 for t in self._queue if not t.cancelled)

    def step(self):
        """Run the next event. Returns False if there was none."""
        while self._queue:
            timer = heapq.heappop(self._queue)
            if timer.cancelled:
                continue
            self.now = timer.time
            self.events_run += 1
            timer.fn()
            return True
        return False

    def run(self, until=None, max_events=1_000_000):
        """Run events until the queue is empty or the clock would pass `until` (the clock then ends at
        `until`). max_events stops a runaway loop (a timer that re-arms forever) with an error."""
        count = 0
        while self._queue:
            head = self._queue[0]
            if head.cancelled:
                heapq.heappop(self._queue)
                continue
            if until is not None and head.time > until:
                break
            count += 1
            if count > max_events:
                raise SimulationError(f"more than {max_events} events; is something re-arming forever?")
            self.step()
        if until is not None and self.now < until:
            self.now = until


class Network:
    """Delivers messages between nodes with random latency, loss, duplication, reordering and partitions.

    Every message is deep-copied when sent, so nodes can never share mutable state by accident.
    Whether a link is usable is checked twice: when the message is sent and again when it arrives, so a
    partition that starts while a message is in flight swallows it, like a real cable pull would.
    """

    def __init__(self, sim, latency=(1.0, 5.0), drop=0.0, duplicate=0.0, trace=False):
        self.sim = sim
        self.latency, self.drop, self.duplicate = latency, drop, duplicate
        self.nodes = {}
        self._blocked = set()               # directed pairs (src, dst) that currently lose messages
        self.trace = [] if trace else None
        self.stats = {"sent": 0, "delivered": 0, "dropped": 0, "duplicated": 0, "blocked": 0}

    def register(self, node):
        if node.id in self.nodes:
            raise SimulationError(f"node {node.id} already exists")
        self.nodes[node.id] = node

    def _log(self, kind, src, dst, msg):
        if self.trace is not None:
            self.trace.append((self.sim.now, kind, src, dst, repr(msg)))

    # ----- sending

    def send(self, src, dst, msg):
        if dst not in self.nodes:
            raise SimulationError(f"no such node: {dst}")
        sender = self.nodes.get(src)
        if sender is not None and not sender.up:
            return                                           # a crashed node says nothing
        self.stats["sent"] += 1
        self._log("send", src, dst, msg)
        if (src, dst) in self._blocked:
            self.stats["blocked"] += 1
            self._log("blocked", src, dst, msg)
            return
        rng = self.sim.rng
        if rng.random() < self.drop:
            self.stats["dropped"] += 1
            self._log("drop", src, dst, msg)
            return
        copies = 1
        if rng.random() < self.duplicate:
            copies = 2
            self.stats["duplicated"] += 1
        for _ in range(copies):
            delay = rng.uniform(*self.latency)
            payload = copy.deepcopy(msg)
            self.sim.schedule(delay, lambda payload=payload: self._deliver(src, dst, payload))

    def _deliver(self, src, dst, msg):
        if (src, dst) in self._blocked:
            self.stats["blocked"] += 1
            self._log("blocked", src, dst, msg)
            return
        node = self.nodes[dst]
        if not node.up:
            self.stats["dropped"] += 1
            self._log("dead", src, dst, msg)
            return
        self.stats["delivered"] += 1
        self._log("deliver", src, dst, msg)
        node.on_message(src, msg)

    # ----- failures

    def block(self, src, dst):
        """Lose every message from src to dst (one direction only)."""
        self._blocked.add((src, dst))

    def unblock(self, src, dst):
        self._blocked.discard((src, dst))

    def partition(self, *groups):
        """Split the network: nodes can only talk inside their own group. Nodes not mentioned in any
        group are cut off from everybody. Replaces any earlier partition."""
        self.heal()
        member = {n: i for i, g in enumerate(groups) for n in g}
        for a in self.nodes:
            for b in self.nodes:
                if a != b and (a not in member or b not in member or member[a] != member[b]):
                    self._blocked.add((a, b))

    def heal(self):
        self._blocked.clear()

    def can_reach(self, src, dst):
        return (src, dst) not in self._blocked


class Node:
    """Base class for a simulated machine: message handler, named timers, crash and restart.

    Subclasses override on_message, and optionally on_start / on_crash / on_restart. Timers set with
    set_timer die with the node: crash() cancels all of them, which is what a power cut does.
    """

    def __init__(self, node_id, sim, net):
        self.id, self.sim, self.net = node_id, sim, net
        self.up = True
        self._timers = {}
        net.register(self)

    # ----- to override

    def on_message(self, src, msg):
        pass

    def on_crash(self):
        pass

    def on_restart(self):
        pass

    # ----- services

    def send(self, dst, msg):
        self.net.send(self.id, dst, msg)

    def set_timer(self, name, delay, fn):
        """Arm timer `name` (replacing a pending one of the same name)."""
        self.cancel_timer(name)

        def fire():
            self._timers.pop(name, None)
            if self.up:
                fn()
        self._timers[name] = self.sim.schedule(delay, fire)

    def cancel_timer(self, name):
        t = self._timers.pop(name, None)
        if t is not None:
            t.cancel()

    def has_timer(self, name):
        return name in self._timers

    def crash(self):
        if not self.up:
            return
        self.up = False
        for t in self._timers.values():
            t.cancel()
        self._timers.clear()
        self.on_crash()

    def restart(self):
        if self.up:
            return
        self.up = True
        self.on_restart()
