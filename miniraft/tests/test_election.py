import random

import pytest

from raft import (CANDIDATE, FOLLOWER, LEADER, AppendEntries, AppendReply, Cluster, ElectionSafetyViolation, Entry,
                  RaftNode, RequestVote, VoteReply)
from sim import Network, Simulator


def elected(cluster, timeout=3000):
    assert cluster.wait_for(lambda: cluster.stable_leader() is not None, timeout), "no leader was elected in time"
    return cluster.stable_leader()


def settle(cluster, duration=300):
    """Let heartbeats reach everybody."""
    cluster.run_for(duration)


# ---------- a single node in isolation (its messages are captured, not delivered) ----------

class Probe:
    """One RaftNode of a cluster whose outgoing messages are recorded instead of sent."""

    def __init__(self, n=5, node_id=1, log=(), term=0, voted_for=None):
        self.cluster = Cluster(n)
        self.node = self.cluster.nodes[node_id]
        self.node.log = list(log)
        self.node.current_term = term
        self.node.voted_for = voted_for
        self.sent = []
        self.node.send = lambda dst, msg: self.sent.append((dst, msg))
        self.node.start()

    def deliver(self, src, msg):
        self.sent.clear()
        self.node.on_message(src, msg)
        return list(self.sent)

    def replies(self):
        return [m for _, m in self.sent]


def vote_request(term, candidate=2, last_index=0, last_term=0):
    return RequestVote(term, candidate, last_index, last_term)


def test_a_vote_is_granted_and_remembered():
    p = Probe()
    (dst, reply), = p.deliver(2, vote_request(1))
    assert dst == 2 and reply == VoteReply(1, True)
    assert p.node.current_term == 1 and p.node.voted_for == 2


def test_a_second_candidate_in_the_same_term_is_refused():
    p = Probe()
    p.deliver(2, vote_request(1, candidate=2))
    (_, reply), = p.deliver(3, vote_request(1, candidate=3))
    assert reply == VoteReply(1, False) and p.node.voted_for == 2


def test_the_same_candidate_asking_twice_gets_the_same_answer():
    p = Probe()
    p.deliver(2, vote_request(1))
    (_, reply), = p.deliver(2, vote_request(1))
    assert reply == VoteReply(1, True)
    assert p.node.vote_log == [(1, 2), (1, 2)]


def test_a_request_from_an_older_term_is_refused_and_told_the_current_term():
    p = Probe(term=5)
    (_, reply), = p.deliver(2, vote_request(3))
    assert reply == VoteReply(5, False) and p.node.current_term == 5 and p.node.voted_for is None


def test_a_newer_term_clears_the_old_vote_so_a_new_vote_is_possible():
    p = Probe(term=3, voted_for=4)
    (_, reply), = p.deliver(2, vote_request(4, candidate=2))
    assert reply == VoteReply(4, True) and p.node.voted_for == 2 and p.node.current_term == 4


@pytest.mark.parametrize("mine, theirs, granted", [
    # (last_term, last_index) of the voter / the candidate. A later last term wins; on a tie the longer log wins.
    ((2, 3), (2, 3), True), ((2, 3), (2, 4), True), ((2, 3), (2, 2), False),
    ((2, 3), (3, 1), True), ((2, 3), (1, 9), False), ((0, 0), (0, 0), True), ((1, 1), (0, 0), False),
    ((0, 0), (1, 1), True),
])
def test_a_vote_needs_a_candidate_log_that_is_at_least_as_up_to_date(mine, theirs, granted):
    (my_term, my_index), (their_term, their_index) = mine, theirs
    log = [Entry(1, "x")] * (my_index - 1) + ([Entry(my_term, "x")] if my_index else [])
    p = Probe(log=log, term=3)
    assert (p.node.last_log_term, p.node.last_log_index) == (my_term, my_index)
    (_, reply), = p.deliver(2, vote_request(4, last_index=their_index, last_term=their_term))
    assert reply.granted is granted
    assert p.node.current_term == 4                  # even a refusal adopts the newer term


def test_voting_resets_the_election_timer_but_refusing_does_not():
    p = Probe(term=2, voted_for=3)
    before = p.node._timers["election"]
    p.deliver(2, vote_request(2, candidate=2))        # refused: already voted for 3
    assert p.node._timers["election"] is before
    p.deliver(2, vote_request(3, candidate=2))        # granted
    assert p.node._timers["election"] is not before


def test_a_candidate_ignores_stale_duplicate_and_refused_replies():
    p = Probe(n=5)
    p.node._on_election_timeout()                     # term 1, votes {1}
    assert p.node.state == CANDIDATE and p.node.current_term == 1
    p.deliver(5, VoteReply(0, True))                  # a granted vote, but from an older election
    assert p.node.votes == {1}
    p.deliver(3, VoteReply(1, False))                 # refused
    p.deliver(2, VoteReply(1, True))
    p.deliver(2, VoteReply(1, True))                  # a duplicate of the same vote: still only 2 of 5
    assert p.node.state == CANDIDATE and p.node.votes == {1, 2}
    p.deliver(4, VoteReply(1, True))                  # 3 of 5 is a majority
    assert p.node.state == LEADER


def test_a_vote_reply_after_losing_the_election_is_harmless():
    p = Probe(n=3)
    p.node._on_election_timeout()
    p.deliver(2, AppendEntries(1, 2, 0, 0, (), 0))     # node 2 won this term
    assert p.node.state == FOLLOWER and p.node.leader_id == 2
    p.deliver(3, VoteReply(1, True))
    assert p.node.state == FOLLOWER


def test_a_reply_with_a_newer_term_makes_a_candidate_step_down():
    p = Probe(n=3)
    p.node._on_election_timeout()
    p.deliver(2, VoteReply(7, False))
    assert p.node.state == FOLLOWER and p.node.current_term == 7 and p.node.voted_for is None


def test_a_leader_steps_down_when_it_sees_a_newer_term_in_any_message():
    for message in (vote_request(9, candidate=2), AppendEntries(9, 2, 0, 0, (), 0), AppendReply(9, False), VoteReply(9, False)):
        p = Probe(n=1)
        p.node._on_election_timeout()
        assert p.node.state == LEADER
        p.deliver(2, message)
        assert p.node.state == FOLLOWER and p.node.current_term == 9
        assert p.node.has_timer("election") and not p.node.has_timer("heartbeat")


def test_heartbeats_from_an_older_term_are_rejected_and_do_not_reset_the_timer():
    p = Probe(term=4)
    before = p.node._timers["election"]
    (_, reply), = p.deliver(2, AppendEntries(3, 2, 0, 0, (), 0))
    assert reply == AppendReply(4, False) and p.node._timers["election"] is before and p.node.leader_id is None


def test_a_heartbeat_from_the_current_or_a_newer_term_is_accepted():
    p = Probe(term=2)
    before = p.node._timers["election"]
    (_, reply), = p.deliver(3, AppendEntries(2, 3, 0, 0, (), 0))
    assert reply == AppendReply(2, True) and p.node.leader_id == 3 and p.node._timers["election"] is not before
    (_, reply), = p.deliver(4, AppendEntries(5, 4, 0, 0, (), 0))
    assert reply == AppendReply(5, True) and p.node.current_term == 5 and p.node.leader_id == 4


def test_two_leaders_in_one_term_is_detected_rather_than_ignored():
    p = Probe(n=1)
    p.node._on_election_timeout()
    with pytest.raises(ElectionSafetyViolation):
        p.deliver(2, AppendEntries(p.node.current_term, 2, 0, 0, (), 0))


def test_the_cluster_monitor_catches_two_leaders_in_the_same_term():
    c = Cluster(3)
    c.nodes[1].current_term = 4
    c._record_leader(c.nodes[1])
    c.nodes[2].current_term = 4
    with pytest.raises(ElectionSafetyViolation):
        c._record_leader(c.nodes[2])


def test_a_node_that_wins_with_nobody_else_to_ask():
    c = Cluster(1).start()
    leader = elected(c)
    assert leader.id == 1 and leader.current_term == 1


def test_a_candidate_votes_for_itself_and_asks_everyone_else():
    p = Probe(n=5)
    p.node._on_election_timeout()
    asked = sorted(dst for dst, m in p.sent if isinstance(m, RequestVote))
    assert asked == [2, 3, 4, 5] and p.node.voted_for == 1 and p.node.vote_log == [(1, 1)]
    assert all(m == RequestVote(1, 1, 0, 0) for _, m in p.sent)


# ---------- whole clusters ----------

@pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 7])
def test_every_cluster_size_elects_exactly_one_leader(n):
    for seed in range(25):
        c = Cluster(n, seed=seed).start()
        leader = elected(c)
        settle(c)
        assert [x.id for x in c.leaders()] == [leader.id]
        assert all(x.current_term == leader.current_term and x.leader_id == leader.id for x in c.nodes.values())
        assert all(x.state == FOLLOWER for x in c.nodes.values() if x is not leader)


def test_a_healthy_cluster_keeps_its_leader_forever():
    for seed in range(10):
        c = Cluster(5, seed=seed).start()
        leader = elected(c)
        term = leader.current_term
        c.run_for(20000)
        assert c.stable_leader() is leader and leader.current_term == term
        assert len(c.history) == 1


def test_election_timeouts_are_random_and_inside_the_configured_range():
    c = Cluster(5, seed=3, election_timeout=(150, 300))
    for node in c.nodes.values():
        node.start()
    due = sorted(node._timers["election"].time for node in c.nodes.values())
    assert all(150 <= t <= 300 for t in due) and len(set(due)) == 5


def test_split_votes_are_resolved_by_trying_again():
    """With timeouts only slightly spread out, several nodes often become candidates at about the same moment."""
    terms = []
    for seed in range(40):
        c = Cluster(5, seed=seed, election_timeout=(150, 160), latency=(1, 15)).start()
        leader = elected(c, timeout=20000)
        terms.append(leader.current_term)
    assert sum(1 for t in terms if t > 1) >= 10            # many first rounds were split ...
    assert all(1 <= t <= 30 for t in terms)                # ... and every one was resolved by trying again


def test_when_the_leader_crashes_the_others_elect_a_new_one():
    for seed in range(20):
        c = Cluster(5, seed=seed).start()
        old = elected(c)
        old.crash()
        assert c.wait_for(lambda: c.stable_leader() is not None and c.stable_leader() is not old, 3000)
        new = c.stable_leader()
        assert new.current_term > old.current_term and new.up


def test_a_restarted_old_leader_becomes_a_follower_of_the_new_one():
    for seed in range(20):
        c = Cluster(3, seed=seed).start()
        old = elected(c)
        old.crash()
        assert c.wait_for(lambda: any(l is not old for l in c.leaders()), 3000)
        old.restart()
        settle(c, 500)
        new = c.stable_leader()
        assert new is not old and old.state == FOLLOWER
        assert old.current_term == new.current_term and old.leader_id == new.id


def test_a_cluster_cannot_elect_without_a_majority():
    c = Cluster(5, seed=1).start()
    leader = elected(c)
    others = [x for x in c.nodes.values() if x is not leader]
    others[0].crash()
    others[1].crash()
    c.run_for(500)
    assert c.stable_leader() is leader                    # 3 of 5 are up: still fine
    others[2].crash()
    others[3].crash()
    leader.crash()
    others[0].restart()
    others[1].restart()
    c.run_for(10000)
    assert len(c.up_nodes()) == 2 and c.leaders() == []   # 2 of 5 can never win
    assert all(x.current_term > 1 for x in c.up_nodes())
    others[2].restart()
    assert elected(c)


def test_the_minority_side_of_a_partition_never_elects_and_the_majority_does():
    c = Cluster(5, seed=4).start()
    old = elected(c)
    minority = {old.id, 1 if old.id != 1 else 2}
    majority = set(c.nodes) - minority
    c.net.partition(minority, majority)
    assert c.wait_for(lambda: any(l.id in majority for l in c.leaders()), 3000)
    c.run_for(3000)
    new = [l for l in c.leaders() if l.id in majority]
    assert len(new) == 1 and new[0].current_term > old.current_term
    assert old.state == LEADER                              # it does not know yet; that is allowed
    assert old.current_term < new[0].current_term           # but it is a leader of an OLDER term
    c.net.heal()
    assert c.wait_for(lambda: old.state == FOLLOWER, 1000)
    settle(c)
    assert len(c.leaders()) == 1 and all(x.current_term == c.leaders()[0].current_term for x in c.nodes.values())


def test_no_side_of_an_even_split_can_elect():
    c = Cluster(4, seed=2).start()
    c.net.partition({1, 2}, {3, 4})
    c.run_for(10000)
    assert c.leaders() == [] and c.leader_of_term == {}
    c.net.heal()
    assert elected(c)


def test_a_node_cut_off_alone_keeps_raising_its_term_and_disrupts_the_cluster_when_it_returns():
    """Known Raft behaviour without the PreVote extension: a returning node with a huge term deposes the leader."""
    c = Cluster(3, seed=5).start()
    leader = elected(c)
    loner = next(x for x in c.nodes.values() if x is not leader)
    c.net.partition(set(c.nodes) - {loner.id}, {loner.id})
    c.run_for(5000)
    assert loner.current_term > leader.current_term + 5 and c.leaders() == [leader]
    old_term = leader.current_term
    c.net.heal()
    c.run_for(3000)
    new = elected(c)
    assert new.current_term >= loner.current_term > old_term
    assert all(x.current_term == new.current_term for x in c.nodes.values())


def test_a_stale_log_can_never_win_an_election():
    for seed in range(30):
        c = Cluster(3, seed=seed)
        for i in (1, 2):
            c.nodes[i].log = [Entry(1, "a"), Entry(1, "b")]
        c.nodes[3].log = []                                  # node 3 missed everything
        for node in c.nodes.values():
            node.current_term = 1
        c.start()
        c.run_for(3000)
        assert c.leader_of_term and 3 not in c.leader_of_term.values()
    for seed in range(30):                                   # and a longer log at an older term loses too
        c = Cluster(3, seed=seed)
        c.nodes[1].log = [Entry(1, "a"), Entry(1, "b"), Entry(1, "c")]
        c.nodes[2].log = [Entry(2, "z")]
        c.nodes[3].log = [Entry(2, "z")]
        for node in c.nodes.values():
            node.current_term = 2
        c.start()
        c.run_for(3000)
        assert c.leader_of_term and 1 not in c.leader_of_term.values()


def test_same_seed_same_elections():
    def history(seed):
        c = Cluster(5, seed=seed, drop=0.1, duplicate=0.1).start()
        c.run_for(5000)
        c.nodes[1].crash()
        c.run_for(5000)
        return c.history, c.net.stats
    assert history(9) == history(9) and history(9) != history(10)


# ---------- randomized chaos ----------

def chaos(seed, steps=250):
    rng = random.Random(seed)
    n = rng.choice([3, 5])
    c = Cluster(n, seed=seed, drop=0.1, duplicate=0.1, latency=(1, 40)).start()
    seen_term = {i: 0 for i in c.nodes}
    ids = list(c.nodes)
    for _ in range(steps):
        action = rng.random()
        node = c.nodes[rng.choice(ids)]
        if action < 0.15:
            node.crash()
        elif action < 0.35:
            node.restart()
        elif action < 0.5:
            shuffled = ids[:]
            rng.shuffle(shuffled)
            cut = rng.randrange(1, n)
            c.net.partition(set(shuffled[:cut]), set(shuffled[cut:]))
        elif action < 0.65:
            c.net.heal()
        c.run_for(rng.uniform(5, 200))
        for i, x in c.nodes.items():                         # terms never go back, not even over a crash
            assert x.current_term >= seen_term[i], (seed, i)
            seen_term[i] = x.current_term
    return c


def test_election_safety_holds_under_random_crashes_partitions_and_message_loss():
    for seed in range(60):
        c = chaos(seed)                                      # ElectionSafetyViolation would raise inside
        for node in c.nodes.values():
            per_term = {}
            for term, candidate in node.vote_log:
                assert per_term.setdefault(term, candidate) == candidate, (seed, node.id, term)
        for _, term, leader in c.history:
            assert c.leader_of_term[term] == leader


def test_the_cluster_always_recovers_once_the_chaos_stops():
    for seed in range(40):
        c = chaos(seed)
        c.net.heal()
        c.net.drop = c.net.duplicate = 0.0
        for node in c.nodes.values():
            node.restart()
        leader = elected(c, timeout=10000)
        settle(c, 500)
        assert [x.id for x in c.leaders()] == [leader.id]
        assert all(x.current_term == leader.current_term and x.leader_id == leader.id for x in c.nodes.values())
