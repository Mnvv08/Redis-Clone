"""A replicated key-value store on top of Raft, with client sessions for exactly-once execution.

Raft gives every replica the same log. This file turns that log into a database and fixes the problem
that clients must RETRY: if a leader crashes after committing a command but before answering, the
client cannot tell whether it ran, so it sends it again, and the retry lands in the log a second time.
Executing it twice would break "append" and "cas". The cure (Raft thesis, chapter 6.3):

  * every client has an id and numbers its requests 1, 2, 3, ... (one outstanding request at a time);
  * every command carries (client id, sequence number);
  * the state machine remembers, per client, the LAST sequence number it executed and that request's
    result. When the same number shows up again it returns the remembered result instead of running
    the command again; an older number is stale and ignored.

The dedup happens when the command is APPLIED, not when it is proposed, so it works whichever leader
the retry went to and however many copies ended up in the log.

Commands are JSON dicts: {"c": client, "s": seq, "op": "put|get|delete|cas|append", "k": key, ...}.
Results are {"ok": value} or {"error": text}. Reads go through the log too, which is simple and
linearizable (a read-index optimisation would avoid the log append, at the cost of more rules).
"""
from raft import Cluster

OPS = ("put", "get", "delete", "cas", "append")


class KVStore:
    """The deterministic state machine. Same commands in the same order give the same state, always."""

    def __init__(self):
        self.data = {}
        self.sessions = {}            # client id -> (last seq, its result)
        self.executed = 0             # commands really executed (duplicates do not count)
        self.replays = 0              # retries answered from the session table
        self.stale = 0                # requests older than the client's latest, ignored

    def apply(self, command):
        if (not isinstance(command, dict) or not {"c", "s", "op"} <= command.keys()
                or not isinstance(command["c"], (str, int)) or isinstance(command["c"], bool)
                or not isinstance(command["s"], int) or isinstance(command["s"], bool)):
            return {"error": "malformed command"}
        client, seq = command["c"], command["s"]
        last = self.sessions.get(client)
        if last is not None:
            if seq == last[0]:
                self.replays += 1
                return last[1]                    # a retry of the request we already ran: same answer, no effect
            if seq < last[0]:
                self.stale += 1
                return {"error": "stale request"}  # the client has moved on; nobody is waiting for this
        result = self._execute(command)
        self.sessions[client] = (seq, result)
        self.executed += 1
        return result

    def _execute(self, cmd):
        op, key = cmd["op"], cmd.get("k")
        if op not in OPS:
            return {"error": f"unknown operation {op!r}"}
        if not isinstance(key, str):
            return {"error": "key must be a string"}
        if op == "get":
            return {"ok": self.data.get(key)}
        if op == "put":
            self.data[key] = cmd.get("v")
            return {"ok": None}
        if op == "delete":
            return {"ok": self.data.pop(key, None) is not None}
        if op == "cas":                            # set key to "new" only if it currently equals "old" (None = absent)
            if self.data.get(key) != cmd.get("old"):
                return {"ok": False}
            if cmd.get("new") is None:
                self.data.pop(key, None)
            else:
                self.data[key] = cmd["new"]
            return {"ok": True}
        value, extra = self.data.get(key, ""), cmd.get("v")        # append
        if not isinstance(value, str) or not isinstance(extra, str):
            return {"error": "append needs string values"}
        self.data[key] = value + extra
        return {"ok": self.data[key]}


class ClientTimeout(Exception):
    """A synchronous call gave up. The command may or may not have run."""


class KVClient:
    """One client: a numbered request at a time, retried until a replica applies it."""

    def __init__(self, cluster, client_id, retry=300.0):
        self.cluster, self.id, self.retry = cluster, client_id, retry
        self.seq = 0
        self.pending = None
        self._timer = None
        self.on_done = None
        self.retries = 0
        cluster.clients[client_id] = self

    def submit(self, op, **fields):
        """Start a request and return its record; it completes when the command is applied somewhere."""
        if self.pending is not None:
            raise RuntimeError("a client has one outstanding request at a time")
        self.seq += 1
        command = {"c": self.id, "s": self.seq, "op": op, **fields}
        now = self.cluster.sim.now
        record = {"client": self.id, "seq": self.seq, "command": command, "invoke": now,
                  "return": None, "result": None, "done": False}
        self.cluster.ops.append(record)
        self.pending = record
        self._send()
        return record

    def _send(self):
        record = self.pending
        if record is None:
            return
        self.cluster.propose(record["command"])         # nobody there? the retry timer tries again
        self._timer = self.cluster.sim.schedule(self.retry, self._retry)

    def _retry(self):
        if self.pending is not None:
            self.retries += 1
            self._send()

    def _applied(self, seq, result):
        record = self.pending
        if record is None or record["seq"] != seq:
            return                                       # an old retry or a duplicate: already answered
        record.update(result=result, done=True, **{"return": self.cluster.sim.now})
        self.pending = None
        if self._timer is not None:
            self._timer.cancel()
        if self.on_done is not None:
            self.on_done(record)

    def abandon(self):
        """Stop retrying. The request stays in history with no result: it may still have happened."""
        self.pending = None
        if self._timer is not None:
            self._timer.cancel()

    def call(self, op, timeout=20000.0, **fields):
        """Synchronous convenience: submit and run the simulation until the answer arrives."""
        record = self.submit(op, **fields)
        if not self.cluster.wait_for(lambda: record["done"], timeout):
            self.abandon()
            raise ClientTimeout(f"{self.id}#{record['seq']} {op} got no answer")
        return record["result"]


class KVCluster(Cluster):
    """A Raft cluster whose state machines are KVStores. A restarted node rebuilds its store from the log."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stores = {i: KVStore() for i in self.nodes}
        self.clients = {}
        self.ops = []                                    # every request ever made, for the linearizability checker
        self.responses = {}                              # (client, seq) -> the result from its first execution
        for node in self.nodes.values():
            node.on_restart = self._restarting(node, node.on_restart)

    def _restarting(self, node, original):
        def restart():
            self.stores[node.id] = KVStore()             # the applied state is volatile: it is replayed from the log
            original()
        return restart

    def client(self, client_id, **options):
        return KVClient(self, client_id, **options)

    def _record_apply(self, node, index, entry):
        super()._record_apply(node, index, entry)       # the Raft safety monitors still run first
        command = entry.command
        if command is None:
            return                                       # a leader's no-op
        result = self.stores[node.id].apply(command)
        if isinstance(command, dict) and isinstance(command.get("c"), (str, int)) and isinstance(command.get("s"), int):
            self.responses.setdefault((command["c"], command["s"]), result)
            client = self.clients.get(command["c"])
            if client is not None:
                client._applied(command["s"], result)

    def store_states(self):
        return {i: (s.data, s.sessions) for i, s in self.stores.items()}


class Workload:
    """A client that issues `count` random operations one after another, then stops."""

    def __init__(self, cluster, client, rng, count, keys=("a", "b", "c"), think=(1.0, 40.0)):
        self.cluster, self.client, self.rng, self.left = cluster, client, rng, count
        self.keys, self.think = keys, think
        client.on_done = lambda record: self._schedule()
        self._schedule()

    @property
    def finished(self):
        return self.left == 0 and self.client.pending is None

    def _schedule(self):
        if self.left > 0:
            self.cluster.sim.schedule(self.rng.uniform(*self.think), self._next)

    def _next(self):
        self.left -= 1
        rng, n = self.rng, self.client.seq + 1
        key = rng.choice(self.keys)
        kind = rng.choice(["append", "append", "put", "get", "get", "delete", "cas"])
        tag = f"{self.client.id}.{n};"
        if kind == "append":
            self.client.submit("append", k=key, v=tag)
        elif kind == "put":
            self.client.submit("put", k=key, v=tag)
        elif kind == "get":
            self.client.submit("get", k=key)
        elif kind == "delete":
            self.client.submit("delete", k=key)
        else:
            self.client.submit("cas", k=key, old=rng.choice([None, "", tag]), new=tag)
