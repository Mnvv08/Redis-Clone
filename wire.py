"""The wire format: Raft messages as JSON, and the "1=host:port,2=host:port" cluster syntax.

Every message travels as one line of JSON: {"from": sender id, "m": {"t": "AppendEntries", "f": {...}}}.
decode() checks everything it is given, because bytes from a socket are not trusted: an unknown type, a
missing or unexpected field or a wrong type raises WireError instead of crashing the node later.
"""
from dataclasses import MISSING, fields

from raft import AppendEntries, AppendReply, Entry, InstallSnapshot, RequestVote, SnapshotReply, VoteReply

MESSAGES = {cls.__name__: cls for cls in (RequestVote, VoteReply, AppendEntries, AppendReply, InstallSnapshot, SnapshotReply)}


class WireError(ValueError):
    """A message could not be understood."""


def encode(msg):
    cls = MESSAGES.get(type(msg).__name__)
    if cls is not type(msg):
        raise WireError(f"cannot send {msg!r}")
    body = {f.name: getattr(msg, f.name) for f in fields(msg)}
    if isinstance(msg, AppendEntries):
        body["entries"] = [[e.term, e.command] for e in msg.entries]
    return {"t": cls.__name__, "f": body}


def _is(value, kind):
    if kind is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if kind is bool:
        return isinstance(value, bool)
    return True                                           # object, tuple: checked separately


def decode(obj):
    if not isinstance(obj, dict) or not isinstance(obj.get("f"), dict):
        raise WireError("not a message")
    cls = MESSAGES.get(obj.get("t"))
    if cls is None:
        raise WireError(f"unknown message type {obj.get('t')!r}")
    body, spec = dict(obj["f"]), {f.name: f for f in fields(cls)}
    if set(body) - set(spec):
        raise WireError(f"{cls.__name__} has no field(s) {sorted(set(body) - set(spec))}")
    for name, f in spec.items():
        if name not in body:
            if f.default is MISSING:
                raise WireError(f"{cls.__name__} is missing {name}")
        elif not _is(body[name], f.type):
            raise WireError(f"{cls.__name__}.{name} has the wrong type")
    if cls is AppendEntries:
        entries = body["entries"]
        if not isinstance(entries, list) or not all(isinstance(e, list) and len(e) == 2 and _is(e[0], int) for e in entries):
            raise WireError("AppendEntries.entries is malformed")
        body["entries"] = tuple(Entry(term, command) for term, command in entries)
    return cls(**body)


def parse_cluster(spec):
    """'1=127.0.0.1:7001,2=127.0.0.1:7002' -> {1: ('127.0.0.1', 7001), 2: ('127.0.0.1', 7002)}"""
    result = {}
    for part in spec.split(","):
        try:
            node, address = part.strip().split("=")
            host, port = address.rsplit(":", 1)
            node, port = int(node), int(port)
        except ValueError:
            raise ValueError(f"bad cluster entry {part!r}: expected ID=HOST:PORT") from None
        if node < 1 or not host or not 0 <= port < 65536 or node in result:
            raise ValueError(f"bad cluster entry {part!r}")
        result[node] = (host, port)
    return result
