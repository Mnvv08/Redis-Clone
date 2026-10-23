import json
import random

import pytest

from raft import AppendEntries, AppendReply, Entry, InstallSnapshot, RequestVote, SnapshotReply, VoteReply
from wire import MESSAGES, WireError, decode, encode, parse_cluster

SAMPLES = [
    RequestVote(3, 2, 10, 2),
    VoteReply(3, True),
    VoteReply(4, False),
    AppendEntries(2, 1, 5, 2, (Entry(2, "a"), Entry(2, None), Entry(3, {"c": "x", "s": 1, "v": [1, {"y": None}]})), 4),
    AppendEntries(2, 1, 0, 0, (), 0),
    AppendReply(2, True, 7, 0),
    AppendReply(2, False, 0, 3),
    AppendReply(2, True),
    InstallSnapshot(5, 1, 100, 4, {"data": {"k": "v"}, "sessions": [["alice", 3, {"ok": None}]], "executed": 9}),
    InstallSnapshot(5, 1, 100, 4, [[1, "a"], [2, None]]),
    SnapshotReply(5, True, 100),
    SnapshotReply(5, False),
]


def over_the_wire(msg):
    return decode(json.loads(json.dumps({"m": encode(msg)}))["m"])      # exactly what a socket would carry


@pytest.mark.parametrize("msg", SAMPLES, ids=lambda m: type(m).__name__)
def test_every_message_survives_json_unchanged(msg):
    back = over_the_wire(msg)
    assert back == msg and type(back) is type(msg)
    if isinstance(msg, AppendEntries):
        assert isinstance(back.entries, tuple) and all(isinstance(e, Entry) for e in back.entries)


def test_all_message_types_are_covered_by_the_samples():
    assert {type(m).__name__ for m in SAMPLES} == set(MESSAGES)


def test_random_messages_round_trip():
    rng = random.Random(1)
    for _ in range(300):
        entries = tuple(Entry(rng.randrange(1, 9), rng.choice([None, "x", 7, [1, 2], {"a": None}])) for _ in range(rng.randrange(0, 6)))
        msg = AppendEntries(rng.randrange(1, 9), rng.randrange(1, 6), rng.randrange(0, 50), rng.randrange(0, 9), entries, rng.randrange(0, 50))
        assert over_the_wire(msg) == msg


def test_a_message_that_is_not_one_of_ours_cannot_be_sent():
    with pytest.raises(WireError):
        encode("hello")
    with pytest.raises(WireError):
        encode(Entry(1, "a"))


BAD = [
    None, 5, "x", [], {}, {"t": "RequestVote"}, {"t": "RequestVote", "f": 5},
    {"t": "Nope", "f": {}}, {"t": 7, "f": {}}, {"t": "__class__", "f": {}},
    {"t": "VoteReply", "f": {"term": 1}},                                   # a missing field
    {"t": "VoteReply", "f": {"term": 1, "granted": True, "extra": 1}},      # an unexpected field
    {"t": "VoteReply", "f": {"term": "1", "granted": True}},                # wrong types
    {"t": "VoteReply", "f": {"term": True, "granted": True}},               # a bool is not an int
    {"t": "VoteReply", "f": {"term": 1, "granted": 1}},
    {"t": "RequestVote", "f": {"term": 1, "candidate_id": 1, "last_log_index": 1.5, "last_log_term": 1}},
    {"t": "AppendEntries", "f": {"term": 1, "leader_id": 1, "prev_log_index": 0, "prev_log_term": 0, "entries": "no", "leader_commit": 0}},
    {"t": "AppendEntries", "f": {"term": 1, "leader_id": 1, "prev_log_index": 0, "prev_log_term": 0, "entries": [[1]], "leader_commit": 0}},
    {"t": "AppendEntries", "f": {"term": 1, "leader_id": 1, "prev_log_index": 0, "prev_log_term": 0, "entries": [["x", 1]], "leader_commit": 0}},
    {"t": "AppendEntries", "f": {"term": 1, "leader_id": 1, "prev_log_index": 0, "prev_log_term": 0, "entries": [None], "leader_commit": 0}},
]


@pytest.mark.parametrize("bad", BAD)
def test_garbage_is_rejected_with_wire_error_and_nothing_else(bad):
    with pytest.raises(WireError):
        decode(bad)


def test_parse_cluster():
    assert parse_cluster("1=127.0.0.1:7001, 2=host:7002") == {1: ("127.0.0.1", 7001), 2: ("host", 7002)}
    for bad in ["", "1", "1=host", "a=host:1", "1=host:x", "0=host:1", "1=:5", "1=h:70000", "1=h:1,1=h:2", "1=h:1,,2=h:2"]:
        with pytest.raises(ValueError):
            parse_cluster(bad)
