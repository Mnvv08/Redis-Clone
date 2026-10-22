import random

import pytest

import kv
from kv import KVCluster, KVStore
from lin import check, ops_from_records
from test_kv import chaos, elected, settle
from test_lin import assert_proved


def test_a_quiet_cluster_is_linearizable_and_the_proof_replays():
    c = KVCluster(3, seed=2).start()
    elected(c)
    clients = [c.client(f"c{i}") for i in range(3)]
    for i in range(10):
        for j, cl in enumerate(clients):
            cl.call(["put", "get", "append"][(i + j) % 3], k="x", v=f"{j}{i}")
    ops = ops_from_records(c.ops)
    assert_proved(check(ops), ops)


@pytest.mark.parametrize("power", [False, True])
def test_the_cluster_stays_linearizable_through_crashes_partitions_and_power_cuts(power):
    states = 0
    for seed in range(30):
        c, _ = chaos(seed, power, clients=5, count=10, keys=("a", "b"))
        ops = ops_from_records(c.ops)
        result = check(ops)
        assert_proved(result, ops)
        states += result.states
    assert states > 1000                                          # the operations really overlapped


def test_requests_that_timed_out_during_an_outage_leave_a_linearizable_history():
    c = KVCluster(3, seed=3).start()
    leader = elected(c)
    a, b = c.client("alice"), c.client("bob")
    a.call("put", k="x", v="before")
    followers = [n for n in c.nodes.values() if n is not leader]
    for n in followers:
        n.crash()
    for _ in range(3):
        try:
            a.call("append", k="x", v="!", timeout=1500)
        except kv.ClientTimeout:
            pass
    for n in followers:
        n.restart()
    elected(c)
    b.call("get", k="x")
    b.call("get", k="x")
    settle(c, 20000)
    ops = ops_from_records(c.ops)
    assert any(o.pending for o in ops)
    assert_proved(check(ops), ops)


# ---------- the checker catches real bugs ----------

def local_read(cluster, node, key, client="reader"):
    """THE BUG: answer a get from one replica's memory, without asking the leader. Fast, and wrong."""
    cluster.run_for(0.001)                                        # strictly after whatever just returned
    now = cluster.sim.now
    seq = sum(1 for r in cluster.ops if r["client"] == client) + 1
    record = {"client": client, "seq": seq, "command": {"c": client, "s": seq, "op": "get", "k": key}, "invoke": now,
              "return": now, "result": {"ok": cluster.stores[node.id].data.get(key)}, "done": True}
    cluster.ops.append(record)
    return record


def test_a_read_served_by_a_lagging_follower_is_caught():
    c = KVCluster(3, seed=1).start()
    leader = elected(c)
    follower = next(n for n in c.nodes.values() if n is not leader)
    c.client("writer").call("put", k="x", v="new")
    read = local_read(c, follower, "x")                           # the leader has applied it; the follower has not yet
    assert read["result"] == {"ok": None}
    result = check(c.ops)
    assert not result.ok
    assert "reader#1 get" in result.explain() and "would have answered {'ok': 'new'}" in result.explain()
    # the same read asked of the leader is fine
    c2 = KVCluster(3, seed=1).start()
    elected(c2)
    c2.client("writer").call("put", k="x", v="new")
    assert c2.client("reader").call("get", k="x") == {"ok": "new"}
    assert check(c2.ops).ok


def test_follower_reads_under_load_are_caught_by_the_checker():
    caught = 0
    for seed in range(20):
        c, _ = chaos(seed, False, clients=3, count=8, keys=("a", "b"))
        assert check(c.ops).ok                                    # the honest history is fine
        rng = random.Random(seed)
        # now the same kind of run with a read served from a random replica's memory after every write
        c = KVCluster(3, seed=seed, latency=(1, 30)).start()
        elected(c)
        writer = c.client("writer")
        for i in range(8):
            writer.call("put", k="a", v=str(i))
            local_read(c, rng.choice(list(c.nodes.values())), "a")
        caught += not check(c.ops).ok
    assert caught >= 15


def test_a_state_machine_that_runs_retries_twice_is_caught(monkeypatch):
    """Switch off the session table. Retries now execute twice, and clients see values that no single
    copy of the data could ever have held."""
    def no_dedup(self, command):
        if not isinstance(command, dict) or not {"c", "s", "op"} <= command.keys():
            return {"error": "malformed command"}
        return self._execute(command)
    monkeypatch.setattr(KVStore, "apply", no_dedup)
    caught = 0
    for seed in range(30):
        c, _ = chaos(seed, False, clients=3, count=10, keys=("a",))
        caught += not check(c.ops).ok
    assert caught >= 5
