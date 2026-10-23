import random

import pytest

from raft import AppendEntries, AppendReply, Cluster, Entry, RequestVote, SafetyViolation, VoteReply
from storage import PowerCut, RaftStorage


def elected(cluster, timeout=3000):
    assert cluster.wait_for(lambda: cluster.stable_leader() is not None, timeout), "no leader was elected in time"
    return cluster.stable_leader()


def commands(node):
    return [c for _, c in node.applied if c is not None]


def durable(node):
    """What the node's disk would give back after a power cut right now."""
    term, vote, log = RaftStorage(node.storage.disk).load()
    return term, vote, [Entry(t, c) for t, c in log]


def watch_sends(cluster, problems):
    """Check, at the moment every message leaves a node, that everything it depends on is already durable."""
    original = cluster.net.send

    def checked(src, dst, msg):
        node = cluster.nodes[src]
        if node.up:
            term, vote, log = durable(node)
            if term < msg.term:
                problems.append(f"{src} sent {msg} but only term {term} is durable")
            if isinstance(msg, VoteReply) and msg.granted and (term, vote) != (msg.term, dst):
                problems.append(f"{src} granted a vote to {dst} that is not durable: {(term, vote)}")
            if isinstance(msg, RequestVote) and (term, vote) != (msg.term, src):
                problems.append(f"{src} asked for votes without durably voting for itself: {(term, vote)}")
            if isinstance(msg, AppendReply) and msg.success and log[:msg.match_index] != node.log[:msg.match_index]:
                problems.append(f"{src} acknowledged entries up to {msg.match_index} that are not durable")
            if isinstance(msg, AppendEntries):
                upto = msg.prev_log_index + len(msg.entries)
                if log[:upto] != node.log[:upto]:
                    problems.append(f"leader {src} sent entries up to {upto} that it has not stored durably")
        return original(src, dst, msg)
    cluster.net.send = checked


# ---------- single nodes ----------

def fresh(n=3, **options):
    c = Cluster(n, **options)
    return c, c.nodes[1]


def test_a_vote_survives_a_crash_so_a_second_candidate_in_the_same_term_is_refused():
    c, node = fresh()
    sent = []
    node.send = lambda dst, msg: sent.append((dst, msg))
    node.on_message(2, RequestVote(5, 2, 0, 0))
    assert sent == [(2, VoteReply(5, True))]
    node.crash()
    node.restart()
    assert node.current_term == 5 and node.voted_for == 2
    sent.clear()
    node.on_message(3, RequestVote(5, 3, 0, 0))
    assert sent == [(3, VoteReply(5, False))]


def test_the_term_survives_a_crash_so_old_messages_stay_old():
    c, node = fresh()
    sent = []
    node.send = lambda dst, msg: sent.append((dst, msg))
    node.on_message(2, AppendEntries(7, 2, 0, 0, (Entry(7, "a"),), 0))
    node.crash()
    node.restart()
    assert node.current_term == 7 and node.log == [Entry(7, "a")]
    sent.clear()
    node.on_message(3, AppendEntries(6, 3, 0, 0, (Entry(6, "evil"),), 0))
    assert sent == [(3, AppendReply(7, False))] and node.log == [Entry(7, "a")]


def test_everything_in_memory_is_rebuilt_from_the_journal():
    c, node = fresh()
    node.on_message(2, AppendEntries(3, 2, 0, 0, (Entry(1, "a"), Entry(2, "b"), Entry(3, "c")), 2))
    node.on_message(3, AppendEntries(4, 3, 2, 2, (Entry(4, "X"),), 0))              # overwrites index 3
    snapshot = (node.current_term, node.voted_for, list(node.log))
    node.crash()
    node.current_term, node.voted_for, node.log = -1, 99, ["garbage"]                # memory is lost
    node.restart()
    assert (node.current_term, node.voted_for, node.log) == snapshot
    assert node.log == [Entry(1, "a"), Entry(2, "b"), Entry(4, "X")]
    assert node.commit_index == 0 and node.applied == []                              # volatile state starts over


def test_a_power_cut_while_storing_a_vote_sends_no_reply():
    for event in ("write", "before_sync"):
        def hook(node_id, ev, event=event):
            if ev == event and armed[0]:
                raise PowerCut(c.nodes[node_id].storage.disk, ev)
        armed = [False]
        c, node = fresh(disk_hook=hook)
        armed[0] = True
        with pytest.raises(PowerCut):
            node.on_message(2, RequestVote(2, 2, 0, 0))
        assert c.net.stats["sent"] == 0                                              # no VoteReply left the machine
        node.crash()
        node.restart()
        assert node.voted_for in (None, 2)                                           # whichever is durable, it is consistent


def test_a_power_cut_while_storing_entries_sends_no_acknowledgement():
    armed = [False]

    def hook(node_id, ev):
        if armed[0] and ev == "before_sync":
            raise PowerCut(c.nodes[node_id].storage.disk, ev)
    c, node = fresh(disk_hook=hook)
    armed[0] = True
    with pytest.raises(PowerCut):
        node.on_message(2, AppendEntries(1, 2, 0, 0, (Entry(1, "a"), Entry(1, "b")), 0))
    assert c.net.stats["sent"] == 0
    node.crash()
    node.restart()
    assert node.log in ([], [Entry(1, "a")], [Entry(1, "a"), Entry(1, "b")])          # a prefix, never garbage


def test_an_idle_cluster_does_no_disk_writes_at_all():
    c = Cluster(5, seed=3).start()
    elected(c)
    c.run_for(500)
    before = {i: n.storage.disk.stats["syncs"] for i, n in c.nodes.items()}
    c.run_for(20000)                                                                   # 400 rounds of heartbeats
    assert {i: n.storage.disk.stats["syncs"] for i, n in c.nodes.items()} == before


def test_each_proposal_costs_exactly_one_sync_on_every_node():
    c = Cluster(3, seed=5).start()
    leader = elected(c)
    c.run_for(500)
    before = {i: n.storage.disk.stats["syncs"] for i, n in c.nodes.items()}
    for i in range(20):
        c.propose(f"c{i}")
        c.run_for(100)
    spent = {i: n.storage.disk.stats["syncs"] - before[i] for i, n in c.nodes.items()}
    assert spent[leader.id] == 20
    assert all(spent[i] == 20 for i in c.nodes if i != leader.id)


def test_a_command_the_journal_cannot_store_is_refused_before_anything_changes():
    c = Cluster(1).start()
    leader = elected(c)
    size = len(leader.log)
    for bad in (object(), {1, 2}, lambda: 0, b"bytes"):
        with pytest.raises(TypeError):
            leader.propose(bad)
    assert len(leader.log) == size and leader.propose({"ok": [1, 2]}) == size + 1


# ---------- whole clusters ----------

@pytest.mark.parametrize("n", [1, 3, 5])
def test_a_total_power_failure_loses_nothing_that_was_committed(n):
    for seed in range(10):
        c = Cluster(n, seed=seed).start()
        elected(c)
        for i in range(12):
            c.propose(f"v{i}")
            c.run_for(15)
        assert c.wait_for(c.converged, 3000)
        logs = {i: list(x.log) for i, x in c.nodes.items()}
        for node in c.nodes.values():
            node.crash()
        c.run_for(200)
        for node in c.nodes.values():
            node.restart()
        assert {i: x.log for i, x in c.nodes.items()} == logs
        elected(c)
        c.propose("after")
        assert c.wait_for(c.converged, 3000)
        for node in c.nodes.values():
            assert commands(node) == [f"v{i}" for i in range(12)] + ["after"]


def test_without_fsync_a_power_failure_loses_committed_data_and_the_monitors_notice():
    """The same scenario with a disk that ignores sync(): acknowledged writes vanish. This is what the
    safety monitors are for, and why every test above would fail if the code skipped a sync."""
    caught = 0
    for seed in range(20):
        c = Cluster(3, seed=seed, sync=False, tear=False).start()
        elected(c)
        for i in range(5):
            c.propose(f"v{i}")
            c.run_for(20)
        c.wait_for(c.converged, 3000)
        for node in c.nodes.values():
            node.crash()
        for node in c.nodes.values():
            node.restart()
        try:
            elected(c)
            for i in range(5):
                c.propose(f"w{i}")
                c.run_for(20)
            c.wait_for(c.converged, 3000)
            c.run_for(500)
        except SafetyViolation:
            caught += 1
    assert caught == 20


def test_nothing_is_ever_sent_before_what_it_depends_on_is_durable():
    problems = []
    for seed in range(25):
        c = Cluster(5, seed=seed, drop=0.1, duplicate=0.1, latency=(1, 30))
        watch_sends(c, problems)
        c.start()
        rng = random.Random(seed)
        for step in range(150):
            node = c.nodes[rng.randrange(1, 6)]
            action = rng.random()
            if action < 0.08:
                node.crash()
            elif action < 0.2:
                node.restart()
            elif action < 0.25:
                ids = list(c.nodes)
                rng.shuffle(ids)
                c.net.partition(set(ids[:2]), set(ids[2:]))
            elif action < 0.35:
                c.net.heal()
            else:
                for x in c.leaders():
                    x.propose(f"p{step}")
            c.run_for(rng.uniform(5, 150))
    assert problems == []


def test_restart_always_restores_exactly_what_the_node_had():
    rng = random.Random(1)
    for seed in range(30):
        c = Cluster(5, seed=seed, drop=0.1, latency=(1, 30)).start()
        for step in range(80):
            for x in c.leaders():
                x.propose(f"p{step}")
            c.run_for(rng.uniform(5, 100))
            node = c.nodes[rng.randrange(1, 6)]
            if node.up and rng.random() < 0.3:
                before = (node.current_term, node.voted_for, list(node.log))
                node.crash()
                node.restart()
                assert (node.current_term, node.voted_for, node.log) == before


# ---------- power cuts at every possible moment ----------

def scenario(cut_at, tear, seed=7):
    """Run a fixed workload; the power fails for one node at the cut_at-th disk event (0 = never)."""
    count = [0]

    def hook(node_id, event):
        count[0] += 1
        if count[0] == cut_at:
            raise PowerCut(c.nodes[node_id].storage.disk, event)
    c = Cluster(3, seed=seed, disk_hook=hook, tear=tear, latency=(1, 15))
    problems = []
    watch_sends(c, problems)
    c.start()
    elected(c)
    for i in range(6):
        c.propose(f"a{i}")
        c.run_for(30)
    leader = c.stable_leader()
    if leader is not None:
        leader.crash()                                           # force a second election
    c.run_for(600)
    if leader is not None:
        leader.restart()
    for i in range(4):
        c.propose(f"b{i}")
        c.run_for(30)
    c.run_for(500)
    return c, count[0], problems


def finish(c):
    """Bring the whole cluster back and prove it is healthy."""
    c.net.heal()
    for node in c.nodes.values():
        node.restart()
    elected(c, 5000)
    assert c.wait_for(lambda: c.propose("final") is not None, 3000)
    assert c.wait_for(c.converged, 5000)
    c.check_logs()
    reference = c.nodes[1].log
    assert all(n.log == reference for n in c.nodes.values())
    history = [c.committed[i] for i in sorted(c.committed)]
    assert reference[:len(history)] == history                    # everything ever committed is still there
    assert commands(c.nodes[1])[-1] == "final"
    assert all(n.applied == c.nodes[1].applied for n in c.nodes.values())


@pytest.mark.parametrize("tear", [False, True])
def test_a_power_cut_at_every_single_disk_event_is_survived(tear):
    clean, total, problems = scenario(0, tear)
    assert problems == [] and total > 50 and not clean.power_cuts
    for cut_at in range(1, total + 1):
        c, _, problems = scenario(cut_at, tear)
        assert len(c.power_cuts) == 1, cut_at
        assert problems == [], (cut_at, problems)
        finish(c)


def test_random_power_cuts_during_chaos_never_break_a_safety_property():
    cuts = commits = 0
    for seed in range(40):
        rng = random.Random(seed)

        def hook(node_id, event):
            if rng.random() < 0.004:
                raise PowerCut(c.nodes[node_id].storage.disk, event)
        c = Cluster(rng.choice([3, 5]), seed=seed, disk_hook=hook, drop=0.1, duplicate=0.1, latency=(1, 40))
        problems = []
        watch_sends(c, problems)
        c.start()
        ids = list(c.nodes)
        for step in range(200):
            action = rng.random()
            node = c.nodes[rng.choice(ids)]
            if action < 0.12:
                node.restart()
            elif action < 0.2:
                node.crash()
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
            c.run_for(rng.uniform(5, 120))
            c.check_logs()
        assert problems == []
        cuts += len(c.power_cuts)
        commits += len(c.committed)
        for n in c.nodes.values():
            n.storage.disk.hook = None
        finish(c)
    assert cuts > 100 and commits > 200                           # the runs really had cuts and real progress


# ---------- two orderings that only show up with a power cut at exactly the wrong moment ----------

def test_a_single_node_never_commits_an_entry_that_is_not_yet_on_disk():
    """With one node the leader alone is a majority, so committing before storing would lose acknowledged data."""
    for event in ("write", "before_sync"):
        armed = [False]

        def hook(node_id, ev, event=event):
            if armed[0] and ev == event:
                raise PowerCut(c.nodes[node_id].storage.disk, ev)
        c = Cluster(1, disk_hook=hook).start()
        leader = elected(c)
        c.run_for(100)
        armed[0] = True
        assert c.propose("lost?") is None                       # the power failed before the write was stored
        armed[0] = False
        assert c.power_cuts and not leader.up
        assert all(e.command != "lost?" for e in c.committed.values())
        leader.restart()
        elected(c)                                              # the monitors would object if anything committed had vanished
        c.propose("next")
        assert c.wait_for(c.converged, 3000)
        assert "lost?" not in commands(leader)


def test_a_power_cut_throws_away_unsynced_bytes_instead_of_flushing_them_later():
    armed = [True]

    def hook(node_id, ev):
        if armed[0] and ev == "before_sync":
            raise PowerCut(c.nodes[node_id].storage.disk, ev)
    c = Cluster(3, disk_hook=hook, tear=False)
    node = c.nodes[1]
    with pytest.raises(PowerCut):
        node.on_message(2, RequestVote(4, 2, 0, 0))
    assert node.storage.disk.pending                            # written but never synced
    node.crash()
    assert node.storage.disk.pending == {}                      # a crash empties the write cache...
    armed[0] = False
    node.restart()
    node.on_message(3, RequestVote(2, 3, 0, 0))                 # ...so a later sync cannot resurrect the old write
    assert (node.current_term, node.voted_for) == (2, 3)
    assert durable(node) == (2, 3, [])
