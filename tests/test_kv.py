import random

import pytest

from kv import ClientTimeout, KVCluster, KVStore, Workload
from raft import Entry
from storage import PowerCut


def cmd(client, seq, op, **fields):
    return {"c": client, "s": seq, "op": op, **fields}


def elected(c, timeout=3000):
    assert c.wait_for(lambda: c.stable_leader() is not None, timeout)
    return c.stable_leader()


def settle(c, timeout=5000):
    assert c.wait_for(c.converged, timeout)


# ---------- the state machine on its own ----------

def test_put_get_delete():
    s = KVStore()
    assert s.apply(cmd("a", 1, "get", k="x")) == {"ok": None}
    assert s.apply(cmd("a", 2, "put", k="x", v=[1, {"y": 2}])) == {"ok": None}
    assert s.apply(cmd("a", 3, "get", k="x")) == {"ok": [1, {"y": 2}]}
    assert s.apply(cmd("a", 4, "delete", k="x")) == {"ok": True}
    assert s.apply(cmd("a", 5, "delete", k="x")) == {"ok": False}
    assert s.apply(cmd("a", 6, "get", k="x")) == {"ok": None}


def test_compare_and_set():
    s = KVStore()
    assert s.apply(cmd("a", 1, "cas", k="x", old=None, new="1")) == {"ok": True}       # None means "absent"
    assert s.apply(cmd("a", 2, "cas", k="x", old=None, new="2")) == {"ok": False}
    assert s.apply(cmd("a", 3, "cas", k="x", old="1", new="2")) == {"ok": True}
    assert s.data == {"x": "2"}
    assert s.apply(cmd("a", 4, "cas", k="x", old="2", new=None)) == {"ok": True}       # None as the new value deletes
    assert s.data == {}


def test_append_builds_a_string_and_returns_it():
    s = KVStore()
    assert s.apply(cmd("a", 1, "append", k="log", v="a")) == {"ok": "a"}
    assert s.apply(cmd("a", 2, "append", k="log", v="b")) == {"ok": "ab"}
    s.apply(cmd("a", 3, "put", k="n", v=5))
    assert s.apply(cmd("a", 4, "append", k="n", v="x")) == {"error": "append needs string values"}
    assert s.apply(cmd("a", 5, "append", k="log", v=7)) == {"error": "append needs string values"}


@pytest.mark.parametrize("bad", [
    None, 5, "put", [], {}, {"c": "a", "s": 1}, {"c": "a", "op": "get"}, {"s": 1, "op": "get"},
    {"c": ["a"], "s": 1, "op": "get", "k": "x"}, {"c": True, "s": 1, "op": "get", "k": "x"},
    {"c": "a", "s": "1", "op": "get", "k": "x"}, {"c": "a", "s": True, "op": "get", "k": "x"},
])
def test_malformed_commands_are_rejected_without_touching_anything(bad):
    s = KVStore()
    assert s.apply(bad) == {"error": "malformed command"}
    assert s.data == {} and s.sessions == {} and s.executed == 0


def test_bad_operations_and_keys_give_errors_but_do_not_crash():
    s = KVStore()
    assert "unknown operation" in s.apply(cmd("a", 1, "drop_table", k="x"))["error"]
    assert s.apply(cmd("a", 2, "get", k=5)) == {"error": "key must be a string"}
    assert s.apply(cmd("a", 3, "get")) == {"error": "key must be a string"}
    assert s.data == {}


def test_a_retry_runs_once_and_gets_the_original_answer():
    s = KVStore()
    first = s.apply(cmd("a", 1, "append", k="x", v="A"))
    assert s.apply(cmd("a", 1, "append", k="x", v="A")) == first == {"ok": "A"}
    assert s.apply(cmd("a", 1, "append", k="x", v="A")) == first
    assert s.data == {"x": "A"} and s.executed == 1 and s.replays == 2


def test_a_retry_gets_the_answer_it_would_have_had_not_the_current_state():
    s = KVStore()
    s.apply(cmd("a", 1, "append", k="x", v="A"))
    s.apply(cmd("b", 1, "append", k="x", v="B"))
    assert s.apply(cmd("a", 1, "append", k="x", v="A")) == {"ok": "A"}               # not "AB"
    assert s.data == {"x": "AB"}


def test_an_old_request_after_the_client_moved_on_is_ignored():
    s = KVStore()
    s.apply(cmd("a", 1, "append", k="x", v="1"))
    s.apply(cmd("a", 2, "append", k="x", v="2"))
    assert s.apply(cmd("a", 1, "append", k="x", v="1")) == {"error": "stale request"}
    assert s.data == {"x": "12"} and s.stale == 1 and s.executed == 2


def test_clients_have_independent_sessions():
    s = KVStore()
    s.apply(cmd("a", 1, "append", k="x", v="a"))
    s.apply(cmd("b", 1, "append", k="x", v="b"))                                      # same seq, different client: runs
    s.apply(cmd(7, 1, "append", k="x", v="7"))
    assert s.data == {"x": "ab7"} and s.executed == 3


def test_sequence_numbers_may_skip():
    s = KVStore()
    s.apply(cmd("a", 1, "put", k="x", v=1))
    assert s.apply(cmd("a", 9, "put", k="x", v=2)) == {"ok": None} and s.data == {"x": 2}


def test_the_state_machine_is_deterministic():
    rng = random.Random(5)
    stream = []
    seqs = {}
    for _ in range(500):
        c = rng.choice("abc")
        if rng.random() < 0.7 or c not in seqs:
            seqs[c] = seqs.get(c, 0) + 1
        s = seqs[c] if rng.random() < 0.8 else max(1, seqs[c] - rng.randrange(0, 3))
        op = rng.choice(["put", "get", "append", "cas", "delete", "bogus"])
        stream.append(cmd(c, s, op, k=rng.choice("xy"), v=rng.choice(["p", "q", 3]), old=rng.choice([None, "p"]), new="n"))
    one, two = KVStore(), KVStore()
    assert [one.apply(x) for x in stream] == [two.apply(x) for x in stream]
    assert (one.data, one.sessions) == (two.data, two.sessions)


# ---------- the replicated store ----------

def test_a_client_talks_to_the_cluster():
    c = KVCluster(3, seed=1).start()
    elected(c)
    a = c.client("alice")
    assert a.call("put", k="x", v="1") == {"ok": None}
    assert a.call("append", k="x", v="2") == {"ok": "12"}
    assert a.call("get", k="x") == {"ok": "12"}
    assert a.call("cas", k="x", old="12", new="z") == {"ok": True}
    assert a.call("delete", k="x") == {"ok": True}
    assert a.call("get", k="x") == {"ok": None}
    settle(c)
    assert len({repr(v) for v in c.store_states().values()}) == 1                    # every replica agrees


def test_one_request_at_a_time():
    c = KVCluster(3, seed=1).start()
    elected(c)
    a = c.client("alice")
    a.submit("put", k="x", v=1)
    with pytest.raises(RuntimeError):
        a.submit("put", k="x", v=2)


def test_the_same_request_proposed_many_times_executes_once():
    c = KVCluster(3, seed=2).start()
    leader = elected(c)
    command = cmd("alice", 1, "append", k="x", v="A")
    for _ in range(5):
        leader.propose(command)                                                       # five copies in the log
    settle(c)
    assert sum(1 for e in leader.log if e.command == command) == 5
    for store in c.stores.values():
        assert store.data == {"x": "A"} and store.executed == 1 and store.replays == 4


def test_a_retry_after_the_leader_crashes_still_runs_exactly_once():
    for seed in range(20):
        c = KVCluster(3, seed=seed, latency=(1, 20)).start()
        old = elected(c)
        a = c.client("alice", retry=150)
        record = a.submit("append", k="x", v="A")
        c.run_for(seed % 7 * 3)                                                       # crash at different moments
        old.crash()
        assert c.wait_for(lambda: record["done"], 20000)
        old.restart()
        settle(c, 20000)
        assert record["result"] == {"ok": "A"}
        assert all(s.data == {"x": "A"} for s in c.stores.values()), seed


def test_a_very_late_duplicate_after_the_client_moved_on_changes_nothing():
    c = KVCluster(3, seed=8).start()
    leader = elected(c)
    a = c.client("alice")
    a.call("append", k="x", v="1")
    a.call("append", k="x", v="2")
    settle(c)
    leader.propose(cmd("alice", 1, "append", k="x", v="1"))                           # a retry that arrives far too late
    settle(c)
    assert all(s.data == {"x": "12"} and s.stale == 1 and s.executed == 2 for s in c.stores.values())
    assert a.call("get", k="x") == {"ok": "12"}


def test_a_restarted_node_rebuilds_its_store_from_the_log():
    c = KVCluster(3, seed=4).start()
    leader = elected(c)
    a = c.client("alice")
    for i in range(8):
        a.call("append", k="x", v=str(i))
    settle(c)
    follower = next(n for n in c.nodes.values() if n is not leader)
    follower.crash()
    a.call("append", k="x", v="!")
    follower.restart()
    assert c.stores[follower.id].data == {}                                           # volatile: empty until replayed
    settle(c)
    assert c.stores[follower.id].data == {"x": "01234567!"} == c.stores[leader.id].data
    assert c.stores[follower.id].executed == 9                                        # replayed once, not twice


def test_a_total_power_failure_keeps_the_data_and_the_sessions():
    c = KVCluster(3, seed=6).start()
    elected(c)
    a = c.client("alice")
    a.call("put", k="x", v="before")
    last = a.call("append", k="x", v="!")
    settle(c)
    for node in c.nodes.values():
        node.crash()
    for node in c.nodes.values():
        node.restart()
    elected(c)
    b = c.client("bob")
    assert b.call("get", k="x") == {"ok": "before!"}
    leader = c.stable_leader()
    leader.propose(cmd("alice", 2, "append", k="x", v="!"))                           # alice's last request, retried late
    settle(c)
    assert all(s.data == {"x": "before!"} for s in c.stores.values()) and last == {"ok": "before!"}


def test_without_a_majority_a_call_times_out_and_the_cluster_recovers():
    c = KVCluster(3, seed=3).start()
    leader = elected(c)
    a = c.client("alice")
    others = [n for n in c.nodes.values() if n is not leader]
    for n in others:
        n.crash()
    with pytest.raises(ClientTimeout):
        a.call("put", k="x", v=1, timeout=2000)
    for n in others:
        n.restart()
    elected(c)
    assert a.call("get", k="x")["ok"] in (None, 1)                                    # the timed-out put may or may not have run
    settle(c)


def test_retries_are_counted_when_the_network_is_bad():
    c = KVCluster(3, seed=9, drop=0.4, latency=(1, 30)).start()
    elected(c)
    a = c.client("alice", retry=100)
    for i in range(10):
        a.call("append", k="x", v=str(i), timeout=60000)
    assert a.retries > 0
    settle(c, 60000)
    assert all(s.data == {"x": "0123456789"} for s in c.stores.values())


# ---------- exactly-once under chaos ----------

def reference(log):
    """What the log means, computed with no session table: the first copy of each (client, seq) runs."""
    ref, seen, results, copies = KVStore(), set(), {}, 0
    for entry in log:
        command = entry.command
        if command is None:
            continue
        copies += 1
        key = (command["c"], command["s"])
        if key not in seen:
            seen.add(key)
            results[key] = ref._execute(command)
    return ref, results, copies


def chaos(seed, power, clients=3, count=12, keys=("a", "b", "c"), **options):
    rng = random.Random(seed)

    def hook(node_id, event):
        if rng.random() < 0.003:
            raise PowerCut(c.nodes[node_id].storage.disk, event)
    c = KVCluster(rng.choice([3, 5]), seed=seed, disk_hook=hook if power else None, drop=0.1, duplicate=0.1,
                  latency=(1, 40), **options)
    c.start()
    loads = [Workload(c, c.client(f"c{i}", retry=rng.choice([80, 200, 400])), random.Random(seed * 10 + i), count, keys)
             for i in range(clients)]
    ids = list(c.nodes)
    for _ in range(120):
        action, node = rng.random(), c.nodes[rng.choice(ids)]
        if action < 0.1:
            node.crash()
        elif action < 0.22:
            node.restart()
        elif action < 0.3:
            shuffled = ids[:]
            rng.shuffle(shuffled)
            c.net.partition(set(shuffled[:len(ids) // 2]), set(shuffled[len(ids) // 2:]))
        elif action < 0.42:
            c.net.heal()
        if rng.random() < 0.15 and c.ops:                       # a very late duplicate of some old request
            old = rng.choice(c.ops)
            if old["done"]:
                c.propose(old["command"])
        c.run_for(rng.uniform(10, 200))
        c.check_logs()
    c.net.heal()
    for node in c.nodes.values():
        node.storage.disk.hook = None
        node.restart()
    assert c.wait_for(lambda: all(w.finished for w in loads), 90000), seed
    settle(c, 20000)
    return c, loads


def verify(c):
    c.check_logs()
    ref, results, copies = reference([c.committed[i] for i in sorted(c.committed)])      # snapshots may have trimmed the log
    for store in c.stores.values():
        assert store.data == ref.data
        assert store.executed == len(results)                                         # every request ran exactly once
    assert len({repr(v) for v in c.store_states().values()}) == 1
    assert c.responses == results                                                     # first execution's answer, for every request
    assert len(c.committed) == c.nodes[1].last_log_index                              # the Raft monitors saw every entry applied
    for record in c.ops:
        assert record["done"]
        assert record["result"] == results[(record["client"], record["seq"])]         # the client saw the real answer
    return copies - len(results)


@pytest.mark.parametrize("power", [False, True])
def test_every_request_runs_exactly_once_and_clients_see_the_true_answers(power):
    extra_copies = retries = stale = 0
    for seed in range(30):
        c, loads = chaos(seed, power)
        extra_copies += verify(c)
        stale += max(s.stale for s in c.stores.values())
        retries += sum(w.client.retries for w in loads)
        assert len(c.ops) == 36
    assert extra_copies > 20 and retries > 100 and stale > 5                          # duplicates really did reach the log
