import random

import pytest

from raft import (FOLLOWER, LEADER, AppendEntries, AppendReply, Cluster, Entry, LeaderCompletenessViolation,
                  LogMatchingViolation, StateMachineSafetyViolation, VoteReply)


def E(term, command=None):
    return Entry(term, command)


def elected(cluster, timeout=3000):
    assert cluster.wait_for(lambda: cluster.stable_leader() is not None, timeout), "no leader was elected in time"
    return cluster.stable_leader()


def commands(node):
    """What a node's state machine holds, without the leaders' internal no-op entries."""
    return [c for _, c in node.applied if c is not None]


class Probe:
    """One node of a cluster whose outgoing messages are recorded instead of sent."""

    def __init__(self, n=3, log=(), term=0, max_entries=50):
        self.cluster = Cluster(n, max_entries=max_entries)
        self.node = self.cluster.nodes[1]
        self.node.log = list(log)
        self.node.current_term = term
        self.sent = []
        self.node.send = lambda dst, msg: self.sent.append((dst, msg))
        self.node.start()

    def deliver(self, src, msg):
        self.sent.clear()
        self.node.on_message(src, msg)
        return list(self.sent)

    def make_leader(self):
        """Win an election: ask, get one vote from node 2 (enough for 3 nodes; the test adds more for 5)."""
        node = self.node
        node._on_election_timeout()
        for voter in range(2, node.quorum + 1):
            node.on_message(voter, VoteReply(node.current_term, True))
        assert node.state == LEADER
        self.sent.clear()
        return node


def ae(term, prev, prev_term, entries=(), commit=0, leader=2):
    return AppendEntries(term, leader, prev, prev_term, tuple(entries), commit)


# ---------- a follower receiving entries ----------

def test_a_follower_appends_new_entries_and_reports_how_far_it_matches():
    p = Probe()
    (dst, reply), = p.deliver(2, ae(1, 0, 0, [E(1, "a"), E(1, "b")]))
    assert dst == 2 and reply == AppendReply(1, True, 2, 0)
    assert p.node.log == [E(1, "a"), E(1, "b")]


def test_a_follower_missing_earlier_entries_refuses_and_says_where_its_log_ends():
    p = Probe(log=[E(1, "a")])
    (_, reply), = p.deliver(2, ae(1, 3, 1, [E(1, "d")]))
    assert reply == AppendReply(1, False, 0, 2) and p.node.log == [E(1, "a")]


def test_a_follower_with_a_different_entry_before_the_new_ones_refuses_and_skips_back_over_that_term():
    p = Probe(log=[E(1), E(1), E(2, "x"), E(2, "y")])
    (_, reply), = p.deliver(2, ae(3, 4, 3, [E(3, "z")]))              # the leader thinks index 4 is term 3
    assert reply == AppendReply(3, False, 0, 3) and len(p.node.log) == 4
    (_, reply), = p.deliver(2, ae(3, 3, 1, [E(3, "z")]))              # index 3 is term 2 here, not 1
    assert reply == AppendReply(3, False, 0, 3)
    (_, reply), = p.deliver(2, ae(3, 2, 1, [E(3, "z")]))              # index 2 matches: accepted, conflict replaced
    assert reply.success and p.node.log == [E(1), E(1), E(3, "z")]


def test_a_conflicting_entry_deletes_it_and_everything_after_it():
    p = Probe(log=[E(1, "a"), E(1, "b"), E(2, "c"), E(2, "d"), E(2, "e")])
    p.deliver(2, ae(3, 2, 1, [E(3, "X"), E(3, "Y")]))
    assert p.node.log == [E(1, "a"), E(1, "b"), E(3, "X"), E(3, "Y")]


def test_entries_that_already_match_are_never_deleted_by_an_older_shorter_message():
    log = [E(1, str(i)) for i in range(5)]
    p = Probe(log=log)
    p.deliver(2, ae(1, 0, 0, log[:2]))                              # a delayed message that only knew about 2 entries
    assert p.node.log == log


def test_receiving_the_same_message_twice_changes_nothing():
    p = Probe()
    message = ae(1, 0, 0, [E(1, "a"), E(1, "b")], commit=1)
    first = p.deliver(2, message)
    state = (list(p.node.log), p.node.commit_index, list(p.node.applied))
    assert p.deliver(2, message) == first
    assert (p.node.log, p.node.commit_index, p.node.applied) == state


def test_a_message_overlapping_what_we_have_adds_only_the_new_part():
    p = Probe(log=[E(1, "a"), E(1, "b")])
    p.deliver(2, ae(1, 1, 1, [E(1, "b"), E(1, "c"), E(1, "d")]))
    assert p.node.log == [E(1, "a"), E(1, "b"), E(1, "c"), E(1, "d")]


def test_an_old_term_message_is_refused_and_leaves_the_log_alone():
    p = Probe(log=[E(2, "a")], term=2)
    (_, reply), = p.deliver(2, ae(1, 0, 0, [E(1, "evil")]))
    assert reply == AppendReply(2, False) and p.node.log == [E(2, "a")]


# ---------- the commit index on a follower ----------

def test_a_follower_commits_up_to_the_leaders_commit_but_not_beyond_what_the_message_proved():
    p = Probe()
    p.deliver(2, ae(1, 0, 0, [E(1, "a"), E(1, "b")], commit=10))
    assert p.node.commit_index == 2 and p.node.applied == [(1, "a"), (2, "b")]


def test_a_heartbeat_does_not_commit_stale_entries_beyond_its_prefix():
    """The follower holds an extra entry from a dead leader; the leader's commit index must not cover it."""
    p = Probe(log=[E(1, "a"), E(1, "b"), E(2, "STALE")], term=2)
    p.deliver(2, ae(3, 2, 1, (), commit=3))
    assert p.node.commit_index == 2 and p.node.applied == [(1, "a"), (2, "b")]


def test_the_commit_index_never_goes_back():
    p = Probe()
    p.deliver(2, ae(1, 0, 0, [E(1, "a"), E(1, "b"), E(1, "c")], commit=3))
    p.deliver(2, ae(1, 0, 0, [E(1, "a")], commit=1))                 # an old message arriving late
    assert p.node.commit_index == 3 and len(p.node.log) == 3


def test_entries_are_applied_once_and_in_order():
    p = Probe()
    p.deliver(2, ae(1, 0, 0, [E(1, "a"), E(1, "b")], commit=1))
    p.deliver(2, ae(1, 0, 0, [E(1, "a"), E(1, "b")], commit=1))
    p.deliver(2, ae(1, 2, 1, [E(1, "c")], commit=3))
    p.deliver(2, ae(1, 2, 1, [E(1, "c")], commit=3))
    assert p.node.applied == [(1, "a"), (2, "b"), (3, "c")]


def test_a_restart_forgets_the_commit_index_and_replays_the_log_when_told_again():
    p = Probe()
    p.deliver(2, ae(1, 0, 0, [E(1, "a"), E(1, "b")], commit=2))
    p.node.crash()
    p.node.restart()
    assert p.node.log == [E(1, "a"), E(1, "b")] and p.node.commit_index == 0 and p.node.applied == []
    p.deliver(2, ae(1, 2, 1, (), commit=2))
    assert p.node.applied == [(1, "a"), (2, "b")]


# ---------- a leader ----------

def test_a_new_leader_adds_a_noop_and_starts_by_sending_it_from_just_after_its_old_log():
    p = Probe(log=[E(1, "a")], term=1)
    p.node._on_election_timeout()
    p.deliver(2, VoteReply(2, True))
    assert p.node.state == LEADER and p.node.log == [E(1, "a"), E(2, None)]
    assert p.node.next_index == {2: 2, 3: 2} and p.node.match_index == {2: 0, 3: 0}
    sent = p.deliver(3, VoteReply(2, True))                          # a late extra vote: nothing happens
    assert sent == []
    p.node._heartbeat()
    messages = [m for _, m in p.sent]
    assert messages and all(m == ae(2, 1, 1, [E(2, None)], 0, leader=1) for m in messages)


def test_propose_appends_replicates_and_refuses_when_not_the_leader():
    p = Probe()
    assert p.node.propose("x") is None and p.node.log == []
    leader = p.make_leader()
    assert leader.propose("x") == 2 and leader.log[-1] == E(1, "x")
    assert sorted(dst for dst, _ in p.sent) == [2, 3]
    leader.crash()
    assert leader.propose("y") is None


def test_an_entry_commits_only_when_a_majority_stores_it():
    p = Probe(n=5)
    leader = p.make_leader()                                        # needs 3 of 5 votes
    index = leader.propose("x")                                      # indexes: 1 = no-op, 2 = x
    assert index == 2 and leader.commit_index == 0
    p.deliver(2, AppendReply(1, True, 2, 0))                         # 2 of 5 store it
    assert leader.commit_index == 0
    p.deliver(3, AppendReply(1, True, 2, 0))                         # 3 of 5: committed
    assert leader.commit_index == 2 and leader.applied == [(1, None), (2, "x")]


def test_a_single_node_commits_on_its_own():
    p = Probe(n=1)
    leader = p.make_leader()
    assert leader.commit_index == 1
    assert leader.propose("x") == 2 and leader.commit_index == 2 and leader.applied[-1] == (2, "x")


def test_old_term_entries_are_not_committed_by_counting_copies():
    """Figure 8 of the Raft paper: an entry on a majority may still be overwritten later, unless a
    newer-term entry sits on top of it. The new leader's own no-op is what unlocks it."""
    p = Probe(n=3, log=[E(1, "a"), E(2, "b")], term=2)
    p.node._on_election_timeout()
    p.deliver(2, VoteReply(3, True))
    leader = p.node
    assert leader.state == LEADER and leader.log[-1] == E(3, None)       # index 3 is the no-op
    p.deliver(2, AppendReply(3, True, 2, 0))                              # a majority now stores index 2 (term 2) ...
    assert leader.commit_index == 0                                       # ... but that alone commits nothing
    p.deliver(2, AppendReply(3, True, 3, 0))                              # the term-3 entry reaches a majority
    assert leader.commit_index == 3 and leader.applied == [(1, "a"), (2, "b"), (3, None)]


def test_a_refusal_makes_the_leader_back_up_and_resend_at_once():
    p = Probe(log=[E(1, "a"), E(1, "b"), E(1, "c")], term=1)
    leader = p.make_leader()
    assert leader.next_index[2] == 4
    sent = p.deliver(2, AppendReply(2, False, 0, 2))
    assert leader.next_index[2] == 2
    (dst, message), = sent
    assert dst == 2 and message.prev_log_index == 1 and message.entries == (E(1, "b"), E(1, "c"), E(2, None))


def test_a_stale_refusal_cannot_push_the_leader_below_what_is_known_to_match():
    p = Probe(log=[E(1, str(i)) for i in range(6)], term=1)
    leader = p.make_leader()
    p.deliver(2, AppendReply(2, True, 7, 0))
    assert leader.match_index[2] == 7 and leader.next_index[2] == 8
    p.deliver(2, AppendReply(2, False, 0, 3))                              # a refusal that was delayed in the network
    assert leader.next_index[2] == 8 and leader.match_index[2] == 7


def test_a_delayed_success_cannot_lower_match_or_next():
    p = Probe(log=[E(1, "a"), E(1, "b")], term=1)
    leader = p.make_leader()
    p.deliver(2, AppendReply(2, True, 3, 0))
    p.deliver(2, AppendReply(2, True, 1, 0))
    assert leader.match_index[2] == 3 and leader.next_index[2] == 4


def test_messages_carry_at_most_max_entries_and_the_next_batch_follows_a_success():
    p = Probe(n=3, log=[], term=1, max_entries=3)
    leader = p.make_leader()
    for c in "abcdefg":
        leader.propose(c)
    p.sent.clear()
    leader.next_index[2] = 1
    leader._replicate(2)
    (_, first), = p.sent
    assert len(first.entries) == 3 and first.prev_log_index == 0
    sent = p.deliver(2, AppendReply(leader.current_term, True, 3, 0))
    (_, second), = sent
    assert second.prev_log_index == 3 and len(second.entries) == 3


def test_replies_from_an_old_term_or_to_a_non_leader_are_ignored():
    p = Probe()
    p.node.match_index = {2: 0, 3: 0}
    p.deliver(2, AppendReply(0, True, 5, 0))                              # we are a follower
    assert p.node.match_index == {2: 0, 3: 0}
    leader = p.make_leader()
    p.deliver(2, AppendReply(0, True, 5, 0))                              # from before this term
    assert leader.match_index[2] == 0 and leader.commit_index == 0


def test_a_deposed_leader_stops_sending():
    p = Probe()
    leader = p.make_leader()
    p.deliver(2, ae(5, 0, 0))
    assert leader.state == FOLLOWER and not leader.has_timer("heartbeat")
    p.sent.clear()
    leader._heartbeat()
    assert p.sent == []


# ---------- the checks themselves must catch violations ----------

def test_the_cluster_monitor_catches_conflicting_applies():
    c = Cluster(3)
    c._record_apply(c.nodes[1], 1, E(1, "a"))
    c._record_apply(c.nodes[2], 1, E(1, "a"))
    with pytest.raises(StateMachineSafetyViolation):
        c._record_apply(c.nodes[3], 1, E(1, "DIFFERENT"))


def test_the_cluster_monitor_catches_a_leader_that_lost_a_committed_entry():
    c = Cluster(3)
    c._record_apply(c.nodes[1], 1, E(1, "a"))
    c.nodes[2].current_term = 2
    c.nodes[2].log = [E(1, "other")]
    with pytest.raises(LeaderCompletenessViolation):
        c._record_leader(c.nodes[2])
    c.nodes[3].current_term = 3
    c.nodes[3].log = []
    with pytest.raises(LeaderCompletenessViolation):
        c._record_leader(c.nodes[3])


def test_the_log_matching_check_catches_logs_that_agree_late_but_not_early():
    c = Cluster(2)
    c.nodes[1].log = [E(1, "a"), E(2, "b")]
    c.nodes[2].log = [E(1, "DIFFERENT"), E(2, "b")]
    with pytest.raises(LogMatchingViolation):
        c.check_logs()
    c.nodes[2].log = [E(1, "a"), E(3, "b")]
    c.check_logs()                                                          # different terms later: allowed


# ---------- whole clusters ----------

@pytest.mark.parametrize("n", [1, 3, 5])
def test_commands_are_replicated_committed_and_applied_everywhere_in_order(n):
    for seed in range(10):
        c = Cluster(n, seed=seed).start()
        elected(c)
        for i in range(15):
            leader, index = c.propose(f"cmd{i}")
            assert index is not None
            c.run_for(7)
        assert c.wait_for(c.converged, 3000)
        for node in c.nodes.values():
            assert commands(node) == [f"cmd{i}" for i in range(15)]
        c.check_logs()


def test_commands_survive_message_loss_duplication_and_reordering():
    for seed in range(15):
        c = Cluster(5, seed=seed, drop=0.2, duplicate=0.15, latency=(1, 60)).start()
        elected(c, 10000)
        sent = []
        for i in range(30):
            if c.wait_for(lambda: c.stable_leader() is not None, 5000):
                leader, index = c.propose(f"c{i}")
                sent.append(f"c{i}")
            c.run_for(30)
        c.net.drop = c.net.duplicate = 0.0
        assert c.wait_for(c.converged, 20000)
        final = commands(c.nodes[1])
        assert all(commands(n) == final for n in c.nodes.values())
        assert len(set(final)) == len(final)                              # nothing applied twice
        assert [x for x in sent if x in final] == final                  # and the order is the proposal order
        c.check_logs()


def test_a_follower_that_was_down_for_a_long_time_catches_up():
    c = Cluster(3, seed=8, max_entries=5).start()
    leader = elected(c)
    sleeper = next(x for x in c.nodes.values() if x is not leader)
    sleeper.crash()
    for i in range(100):
        c.propose(f"c{i}")
        c.run_for(3)
    c.run_for(500)
    assert leader.commit_index >= 100 and sleeper.log == []
    sleeper.restart()
    assert c.wait_for(c.converged, 5000)
    assert commands(sleeper) == [f"c{i}" for i in range(100)]


def test_a_crashed_follower_that_missed_entries_gets_them_after_restart_and_re_applies_everything():
    c = Cluster(3, seed=2).start()
    leader = elected(c)
    follower = next(x for x in c.nodes.values() if x is not leader)
    for i in range(5):
        c.propose(f"a{i}")
    c.run_for(300)
    before = list(follower.applied)
    assert len(before) >= 5
    follower.crash()
    for i in range(5):
        c.propose(f"b{i}")
    c.run_for(300)
    follower.restart()
    assert follower.applied == []
    assert c.wait_for(c.converged, 3000)
    assert follower.applied[:len(before)] == before and commands(follower) == [f"a{i}" for i in range(5)] + [f"b{i}" for i in range(5)]


def test_committed_entries_survive_the_leader_crashing():
    for seed in range(20):
        c = Cluster(5, seed=seed).start()
        old = elected(c)
        for i in range(5):
            c.propose(f"x{i}")
        assert c.wait_for(lambda: old.commit_index >= 6, 2000)
        old.crash()
        new_leader_found = c.wait_for(lambda: c.stable_leader() is not None and c.stable_leader() is not old, 3000)
        assert new_leader_found
        c.propose("after")
        assert c.wait_for(c.converged, 3000)
        for node in c.up_nodes():
            assert commands(node) == [f"x{i}" for i in range(5)] + ["after"]


def test_a_leader_cut_off_with_a_minority_cannot_commit_and_its_entries_are_overwritten():
    c = Cluster(5, seed=11).start()
    old = elected(c)
    c.propose("kept")
    assert c.wait_for(c.converged, 2000)
    friend = next(i for i in c.nodes if i != old.id)
    minority = {old.id, friend}
    majority = set(c.nodes) - minority
    c.net.partition(minority, majority)
    lost = [old.propose(f"lost{i}") for i in range(3)]
    assert all(lost)
    c.run_for(200)
    assert old.commit_index < min(lost)                                  # a minority can never commit
    assert c.wait_for(lambda: any(l.id in majority for l in c.leaders()), 3000)
    new = next(l for l in c.leaders() if l.id in majority)
    new.propose("new1")
    new.propose("new2")
    c.run_for(2000)
    assert old.commit_index < min(lost) and old.state == LEADER          # it still thinks it leads
    c.net.heal()
    assert c.wait_for(c.converged, 5000)
    for node in c.nodes.values():
        assert commands(node) == ["kept", "new1", "new2"]
        assert not any(e.command and str(e.command).startswith("lost") for e in node.log)
    c.check_logs()


def test_proposals_with_no_leader_are_refused():
    c = Cluster(3).start()
    assert c.propose("x") is None
    c.net.partition({1}, {2}, {3})
    c.run_for(5000)
    assert c.propose("x") is None


def test_same_seed_same_replication():
    def run(seed):
        c = Cluster(5, seed=seed, drop=0.1, duplicate=0.1).start()
        elected(c, 5000)
        for i in range(20):
            c.propose(i)
            c.run_for(25)
        c.nodes[1].crash()
        c.run_for(2000)
        return c.committed, c.history, c.net.stats
    assert run(4) == run(4) and run(4) != run(5)


# ---------- randomized chaos with proposals ----------

def chaos(seed, steps=200):
    rng = random.Random(seed)
    n = rng.choice([3, 5])
    c = Cluster(n, seed=seed, drop=0.1, duplicate=0.1, latency=(1, 40), max_entries=rng.choice([2, 50])).start()
    ids = list(c.nodes)
    for step in range(steps):
        action = rng.random()
        node = c.nodes[rng.choice(ids)]
        if action < 0.10:
            node.crash()
        elif action < 0.25:
            node.restart()
        elif action < 0.33:
            shuffled = ids[:]
            rng.shuffle(shuffled)
            cut = rng.randrange(1, n)
            c.net.partition(set(shuffled[:cut]), set(shuffled[cut:]))
        elif action < 0.45:
            c.net.heal()
        else:                                           # most steps: somebody tries to write
            for x in c.nodes.values():
                if x.up and x.state == LEADER:
                    x.propose(f"s{seed}-{step}")
        c.run_for(rng.uniform(5, 120))
        c.check_logs()
        for x in c.nodes.values():
            assert 0 <= x.last_applied <= x.commit_index <= len(x.log)
            if x.up:
                assert [i for i, _ in x.applied] == list(range(1, x.last_applied + 1))
    return c


def test_all_safety_properties_hold_under_random_crashes_partitions_loss_and_writes():
    progress = 0
    for seed in range(60):
        c = chaos(seed)                                  # the monitors raise on any violation
        progress += len(c.committed)
        for node in c.nodes.values():                    # what a node applied is a prefix of the agreed history
            for index, command in node.applied:
                assert c.committed[index].command == command
    assert progress > 60 * 10                            # and the runs were not vacuous: plenty got committed


def test_after_the_chaos_stops_everything_converges_and_new_writes_work():
    for seed in range(40):
        c = chaos(seed)
        c.net.heal()
        c.net.drop = c.net.duplicate = 0.0
        for node in c.nodes.values():
            node.restart()
        elected(c, 10000)
        assert c.wait_for(lambda: c.propose("final") is not None, 5000)
        assert c.wait_for(c.converged, 10000)
        reference = c.nodes[1].log
        assert all(n.log == reference for n in c.nodes.values())
        history = [c.committed[i] for i in sorted(c.committed)]
        assert reference[:len(history)] == history                       # nothing committed was ever lost
        assert commands(c.nodes[1])[-1] == "final"
        assert all(n.applied == c.nodes[1].applied for n in c.nodes.values())
