import json

import pytest

from kv import KVCluster, KVStore, SnapshotViolation
from lin import check
from test_kv import chaos, cmd, elected, settle, verify


# ---------- the store's snapshot ----------

def busy_store():
    s = KVStore()
    s.apply(cmd("alice", 1, "put", k="x", v=[1, {"y": None}]))
    s.apply(cmd(7, 1, "append", k="log", v="abc"))                  # an integer client id
    s.apply(cmd("alice", 2, "get", k="nothing"))
    s.apply(cmd("bob", 1, "append", k="log", v=5))                  # an error result is remembered too
    s.apply(cmd("bob", 1, "append", k="log", v=5))                  # a replay
    return s


def test_a_snapshot_is_plain_json_and_restores_the_whole_store():
    s = busy_store()
    state = json.loads(json.dumps(s.snapshot()))                    # exactly what the journal will do to it
    t = KVStore.from_snapshot(state)
    assert (t.data, t.sessions, t.executed) == (s.data, s.sessions, s.executed)
    assert t.sessions[7] == (1, {"ok": "abc"}) and t.sessions["bob"] == (1, {"error": "append needs string values"})


def test_the_snapshot_is_a_copy_not_a_view():
    s = busy_store()
    state = s.snapshot()
    s.apply(cmd("alice", 3, "put", k="x", v="changed"))
    s.data["x"] = "mutated"
    assert state["data"]["x"] == [1, {"y": None}]
    t = KVStore.from_snapshot(state)
    t.data["x"].append("more")
    assert state["data"]["x"] == [1, {"y": None}]


def test_a_restored_store_still_recognises_retries_and_stale_requests():
    t = KVStore.from_snapshot(json.loads(json.dumps(busy_store().snapshot())))
    before = t.executed
    assert t.apply(cmd("bob", 1, "append", k="log", v=5)) == {"error": "append needs string values"}   # the remembered answer
    assert t.apply(cmd(7, 1, "append", k="log", v="abc")) == {"ok": "abc"}                              # not "abcabc"
    assert t.apply(cmd("alice", 1, "put", k="x", v="old")) == {"error": "stale request"}
    assert t.executed == before and t.data["log"] == "abc" and t.data["x"] == [1, {"y": None}]


def test_a_store_restored_without_its_sessions_runs_retries_twice():
    """Why the session table must be in the snapshot."""
    state = busy_store().snapshot()
    state["sessions"] = []
    t = KVStore.from_snapshot(state)
    assert t.apply(cmd(7, 1, "append", k="log", v="abc")) == {"ok": "abcabc"}                           # executed a second time


# ---------- replicated ----------

def test_exactly_once_survives_snapshots_and_a_full_restart():
    c = KVCluster(3, seed=4, snapshot_threshold=4).start()
    elected(c)
    a = c.client("alice")
    for i in range(10):
        a.call("append", k="x", v=str(i))
    settle(c)
    last = a.seq
    assert all(n.base >= 8 for n in c.nodes.values())                     # snapshots exist everywhere
    for node in c.nodes.values():
        node.crash()
    for node in c.nodes.values():
        node.restart()
    elected(c)
    leader = c.stable_leader()
    for _ in range(3):
        leader.propose(cmd("alice", last, "append", k="x", v="9"))        # the last request, retried after the restart
    settle(c)
    for store in c.stores.values():
        assert store.data == {"x": "0123456789"} and store.executed == 10


def test_a_follower_restored_from_a_snapshot_answers_retries_correctly():
    c = KVCluster(3, seed=5, snapshot_threshold=4).start()
    leader = elected(c)
    sleeper = next(n for n in c.nodes.values() if n is not leader)
    a = c.client("alice")
    a.call("append", k="x", v="a")
    sleeper.crash()
    for i in range(12):
        a.call("append", k="x", v=str(i % 10))
    sleeper.restart()
    settle(c)
    assert sleeper.snapshots_installed >= 1
    store = c.stores[sleeper.id]
    assert store.data == c.stores[leader.id].data and store.sessions == c.stores[leader.id].sessions
    before = store.executed
    leader.propose(cmd("alice", a.seq, "append", k="x", v="LAST"))        # a duplicate of the last request
    leader.propose(cmd("alice", 1, "append", k="x", v="a"))               # and a very old one
    settle(c)
    assert all(s.executed == before and "LAST" not in s.data["x"] for s in c.stores.values())


def test_a_snapshot_that_forgets_the_sessions_is_caught_by_the_monitor(monkeypatch):
    real = KVStore.snapshot

    def forgetful(self):
        state = real(self)
        state["sessions"] = []
        return state
    monkeypatch.setattr(KVStore, "snapshot", forgetful)
    c = KVCluster(3, seed=1, snapshot_threshold=3).start()
    elected(c)
    a = c.client("alice")
    with pytest.raises(SnapshotViolation):
        for i in range(10):
            a.call("append", k="x", v=str(i))


def test_a_snapshot_with_wrong_data_is_caught_by_the_monitor(monkeypatch):
    real = KVStore.snapshot

    def corrupt(self):
        state = real(self)
        state["data"]["x"] = state["data"].get("x", "") + "?"
        return state
    monkeypatch.setattr(KVStore, "snapshot", corrupt)
    c = KVCluster(3, seed=1, snapshot_threshold=3).start()
    elected(c)
    a = c.client("alice")
    with pytest.raises(SnapshotViolation):
        for i in range(10):
            a.call("append", k="x", v=str(i))


@pytest.mark.parametrize("power", [False, True])
def test_exactly_once_and_linearizability_through_chaos_with_snapshots(power):
    installed = taken = 0
    for seed in range(30):
        c, _ = chaos(seed, power, clients=4, count=10, keys=("a", "b"), snapshot_threshold=3)
        verify(c)
        assert check(c.ops).ok
        installed += sum(n.snapshots_installed for n in c.nodes.values())
        taken += sum(n.snapshots_taken for n in c.nodes.values())
    assert installed > 10 and taken > 150
