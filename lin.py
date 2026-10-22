"""A linearizability checker (Wing & Gong search with memoisation, per key).

A history is linearizable if every operation can be given a single instant between its invocation and
its response such that, executed in that order on ONE plain copy of the data, every response matches.
That is exactly "the cluster behaves like one machine that never loses or reorders anything".

How the search works. Operations are tried one at a time as the NEXT point in the imagined order. An
operation may go next only if no unplaced operation had already RETURNED before it was invoked (that
would put them in the wrong real-time order). Each try runs the operation on the model; if the model's
answer differs from what the client saw, the branch dies. The search succeeds when every completed
operation is placed. States already explored (set of placed operations + model state) are remembered,
which keeps the search fast on realistic histories.

Two refinements:
  * Keys are independent registers, so each key is checked on its own (locality of linearizability).
  * An operation with no response (a client timed out) MAY have taken effect at any time after its
    invocation, or never. It is placed only if that helps. A pending get can never matter and is dropped.

The model below is written separately from kv.py on purpose: it is the specification, kv.py is the
implementation, and the checker compares what clients saw against the specification.
"""
import json
from dataclasses import dataclass, field

INF = float("inf")
ABSENT = ("absent",)


class TooComplex(Exception):
    """The search explored more states than the limit allowed."""


def model_step(state, command):
    """The specification: one plain key. state is ABSENT or ("val", v). Returns (new state, result)."""
    op = command.get("op")
    present, value = (state != ABSENT), (state[1] if state != ABSENT else None)
    if not isinstance(command.get("k"), str):
        return state, {"error": "key must be a string"}
    if op == "get":
        return state, {"ok": value}
    if op == "put":
        return ("val", command.get("v")), {"ok": None}
    if op == "delete":
        return ABSENT, {"ok": present and value is not None}
    if op == "cas":
        if value != command.get("old"):
            return state, {"ok": False}
        return (ABSENT if command.get("new") is None else ("val", command["new"])), {"ok": True}
    if op == "append":
        base, extra = (value if present else ""), command.get("v")
        if not isinstance(base, str) or not isinstance(extra, str):
            return state, {"error": "append needs string values"}
        return ("val", base + extra), {"ok": base + extra}
    return state, {"error": f"unknown operation {op!r}"}


@dataclass
class Op:
    id: int
    client: object
    seq: int
    command: dict
    invoke: float
    ret: float            # INF when the client never got a response
    result: object

    @property
    def pending(self):
        return self.ret == INF

    def __str__(self):
        c = self.command
        detail = " ".join(f"{k}={c[k]!r}" for k in ("k", "v", "old", "new") if k in c)
        end = "no response" if self.pending else f"{self.ret:g}"
        answer = "?" if self.pending else self.result
        return f"{self.client}#{self.seq} {c.get('op')} {detail} [{self.invoke:g} .. {end}] -> {answer}"


def ops_from_records(records):
    """Turn KVCluster.ops records into Op objects."""
    return [Op(i, r["client"], r["seq"], r["command"], r["invoke"], r["return"] if r["done"] else INF, r["result"])
            for i, r in enumerate(records)]


@dataclass
class Failure:
    key: object
    placed: list                 # the longest valid prefix of the order that the search found
    stuck: list                  # (op, what the specification would have answered) for the ops that could go next
    unplaced: list

    def explain(self):
        lines = [f"NOT LINEARIZABLE on key {self.key!r}.",
                 f"The longest order the search could build holds {len(self.placed)} of "
                 f"{len(self.placed) + len(self.unplaced)} operations:"]
        lines += [f"    {op}" for op in self.placed]
        lines.append("Nothing can come next. Each candidate would get a different answer than the client saw:")
        lines += [f"    {op}\n        a single copy would have answered {expected}" for op, expected in self.stuck]
        return "\n".join(lines)


@dataclass
class LinResult:
    ok: bool
    orders: dict = field(default_factory=dict)      # key -> operations in linearization order (when ok)
    failure: Failure = None
    states: int = 0

    def explain(self):
        return "linearizable" if self.ok else self.failure.explain()


def _group(ops):
    groups = {}
    for op in ops:
        if op.pending and op.command.get("op") == "get":
            continue                                  # a get nobody heard back from changes nothing
        key = op.command.get("k")
        groups.setdefault(key if isinstance(key, str) else repr(key), []).append(op)
    return groups


def check(records_or_ops, limit=2_000_000):
    ops = records_or_ops
    if ops and isinstance(ops[0], dict):
        ops = ops_from_records(ops)
    result = LinResult(True)
    for key, group in sorted(_group(ops).items()):
        order, failure, explored = _check_key(key, group, limit)
        result.states += explored
        if order is None:
            return LinResult(False, failure=failure, states=result.states)
        result.orders[key] = order
    return result


def _check_key(key, ops, limit):
    ops = sorted(ops, key=lambda o: (o.invoke, o.id))
    n = len(ops)
    required = sum(1 << i for i, o in enumerate(ops) if not o.pending)
    seen = set()
    best = {"order": [], "state": ABSENT, "mask": 0}

    def search(mask, state, order):
        if mask & required == required:
            return order
        memo = (mask, repr(state))
        if memo in seen:
            return None
        if len(seen) >= limit:
            raise TooComplex(f"more than {limit} states on key {key!r}")
        seen.add(memo)
        if len(order) > len(best["order"]):
            best.update(order=order, state=state, mask=mask)
        horizon = min(o.ret for i, o in enumerate(ops) if not mask >> i & 1 and not o.pending)
        for i, o in enumerate(ops):
            if mask >> i & 1:
                continue
            if o.invoke > horizon:
                break                                  # sorted by invoke: nothing later can go next either
            new_state, answer = model_step(state, o.command)
            if o.pending or answer == o.result:
                found = search(mask | 1 << i, new_state, order + [i])
                if found is not None:
                    return found
        return None

    found = search(0, ABSENT, [])
    if found is not None:
        return [ops[i] for i in found], None, len(seen)
    mask, state = best["mask"], best["state"]
    unplaced = [o for i, o in enumerate(ops) if not mask >> i & 1]
    horizon = min(o.ret for o in unplaced if not o.pending)
    stuck = [(o, model_step(state, o.command)[1]) for o in unplaced if o.invoke <= horizon and not o.pending]
    return None, Failure(key, [ops[i] for i in best["order"]], stuck, unplaced), len(seen)


def validate(order, ops_for_key):
    """Independent proof checker: is this order a legal linearization? (Used by tests on the checker's output.)"""
    state = ABSENT
    for later_index, op in enumerate(order):
        for earlier in order[later_index + 1:]:
            if earlier.ret < op.invoke:
                return False, f"{earlier} returned before {op} was invoked but is ordered after it"
        state, answer = model_step(state, op.command)
        if not op.pending and answer != op.result:
            return False, f"{op}: the model answers {answer}"
    placed = {id(o) for o in order}
    missing = [o for o in ops_for_key if not o.pending and id(o) not in placed]
    return (not missing), f"completed operations left out: {missing}"
