import random

import pytest

from raft import Cluster
from storage import JOURNAL, PowerCut
from test_snapshot import elected


# ---------- whole clusters ----------

def count_messages(c):
    counts = {}
    original = c.net.send

    def counting(src, dst, msg):
        counts[type(msg).__name__] = counts.get(type(msg).__name__, 0) + 1
        return original(src, dst, msg)
    c.net.send = counting
    return counts


def test_a_follower_that_slept_through_everything_is_caught_up_by_snapshot():
    c = Cluster(3, seed=2, snapshot_threshold=10).start()
    counts = count_messages(c)
    leader = elected(c)
    sleeper = next(n for n in c.nodes.values() if n is not leader)
    sleeper.crash()
    for i in range(80):
        c.propose(f"v{i}")
        c.run_for(20)
    assert leader.base > 60 and sleeper.last_log_index < 10
    sleeper.restart()
    assert c.wait_for(c.converged, 5000)
    assert counts.get("InstallSnapshot", 0) >= 1
    assert sleeper.base > 0 and sleeper.applied == leader.applied                     # it jumped, and it is complete


def test_logs_stay_bounded_however_long_the_cluster_runs():
    c = Cluster(3, seed=3, snapshot_threshold=20).start()
    elected(c)
    for i in range(600):
        c.propose(f"v{i}")
        c.run_for(10)
    assert c.wait_for(c.converged, 5000)
    assert all(len(n.log) < 45 for n in c.nodes.values())
    assert all(n.base > 500 for n in c.nodes.values())
    assert all(len(n.storage.disk.read(JOURNAL)) < 6000 for n in c.nodes.values())      # 600 entries would be ~40 KB


def test_a_whole_cluster_restart_keeps_everything_that_was_snapshotted():
    for n in (1, 3, 5):
        c = Cluster(n, seed=n, snapshot_threshold=6).start()
        elected(c)
        for i in range(40):
            c.propose(f"v{i}")
            c.run_for(25)
        assert c.wait_for(c.converged, 5000)
        for node in c.nodes.values():
            node.crash()
        for node in c.nodes.values():
            node.restart()
        elected(c)
        c.propose("after")
        assert c.wait_for(c.converged, 5000)
        for node in c.nodes.values():
            assert [cmd for _, cmd in node.applied if cmd] == [f"v{i}" for i in range(40)] + ["after"]


def chaos_run(seed, power):
    rng = random.Random(seed)

    def hook(node_id, event):
        if rng.random() < 0.003:
            raise PowerCut(c.nodes[node_id].storage.disk, event)
    c = Cluster(rng.choice([3, 5]), seed=seed, disk_hook=hook if power else None, drop=0.1, duplicate=0.1,
                latency=(1, 40), snapshot_threshold=rng.choice([2, 4, 7]))
    counts = count_messages(c)
    c.start()
    ids = list(c.nodes)
    for step in range(250):
        action, node = rng.random(), c.nodes[rng.choice(ids)]
        if action < 0.1:
            node.crash()
        elif action < 0.2:
            node.restart()
        elif action < 0.27:
            shuffled = ids[:]
            rng.shuffle(shuffled)
            c.net.partition(set(shuffled[:len(ids) // 2]), set(shuffled[len(ids) // 2:]))
        elif action < 0.4:
            c.net.heal()
        else:
            try:
                for x in c.leaders():
                    x.propose(f"s{seed}-{step}")
            except PowerCut as cut:
                c._power_cut(cut)
        c.run_for(rng.uniform(5, 100))
        c.check_logs()
    c.net.heal()
    for node in c.nodes.values():
        node.storage.disk.hook = None
        node.restart()
    assert c.wait_for(lambda: c.propose("final") is not None, 20000)
    assert c.wait_for(c.converged, 20000)
    c.check_logs()
    reference = c.nodes[1].applied
    assert all(n.applied == reference for n in c.nodes.values())                         # every node's state machine agrees
    assert [cmd for _, cmd in reference] == [c.committed[i].command for i in sorted(c.committed)]
    assert reference[-1][1] == "final"
    return c, counts


@pytest.mark.parametrize("power", [False, True])
def test_chaos_with_snapshots_never_breaks_a_safety_property(power):
    installs = taken = 0
    for seed in range(40):
        c, counts = chaos_run(seed, power)
        installs += sum(n.snapshots_installed for n in c.nodes.values())
        taken += sum(n.snapshots_taken for n in c.nodes.values())
    assert installs > 100 and taken > 500                                                # snapshots really were taken and installed


def sweep_scenario(cut_at, tear, seed=5):
    count = [0]

    def hook(node_id, event):
        count[0] += 1
        if count[0] == cut_at:
            raise PowerCut(c.nodes[node_id].storage.disk, event)
    c = Cluster(3, seed=seed, disk_hook=hook, tear=tear, latency=(1, 12), snapshot_threshold=3)
    c.start()
    leader = elected(c)
    sleeper = next(n for n in c.nodes.values() if n is not leader)
    for i in range(4):
        c.propose(f"a{i}")
        c.run_for(25)
    sleeper.crash()
    for i in range(8):
        c.propose(f"b{i}")
        c.run_for(25)
    sleeper.restart()                                                                    # it must be caught up with a snapshot
    c.run_for(300)
    stable = c.stable_leader()
    if stable is not None:
        stable.crash()
    c.run_for(500)
    if stable is not None:
        stable.restart()
    for i in range(3):
        c.propose(f"c{i}")
        c.run_for(25)
    c.run_for(300)
    return c, count[0]


def finish(c):
    c.net.heal()
    for node in c.nodes.values():
        node.storage.disk.hook = None
        node.restart()
    elected(c, 8000)
    assert c.wait_for(lambda: c.propose("final") is not None, 5000)
    assert c.wait_for(c.converged, 8000)
    c.check_logs()
    assert all(n.applied == c.nodes[1].applied for n in c.nodes.values())
    assert [cmd for _, cmd in c.nodes[1].applied] == [c.committed[i].command for i in sorted(c.committed)]


@pytest.mark.parametrize("tear", [False, True])
def test_a_power_cut_at_every_disk_event_with_snapshots_is_survived(tear):
    clean, total = sweep_scenario(0, tear)
    assert total > 150 and not clean.power_cuts
    assert sum(n.snapshots_taken for n in clean.nodes.values()) > 5
    assert sum(n.snapshots_installed for n in clean.nodes.values()) >= 1               # the sleeper really was caught up by a snapshot
    for cut_at in range(1, total + 1):
        c, _ = sweep_scenario(cut_at, tear)
        assert len(c.power_cuts) == 1, cut_at
        finish(c)
