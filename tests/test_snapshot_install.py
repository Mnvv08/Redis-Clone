import pytest

from raft import (AppendEntries, AppendReply, Cluster, ElectionSafetyViolation, Entry, InstallSnapshot, RequestVote,
                  SnapshotReply)
from storage import PowerCut
from test_snapshot import elected, feed, probe, state_of


# ---------- InstallSnapshot, the follower's side ----------

def test_a_far_behind_follower_jumps_to_the_snapshot():
    c = Cluster(3, snapshot_threshold=1000)
    node = c.nodes[1]
    sent = probe(c, node)
    node.on_message(2, InstallSnapshot(2, 2, 10, 2, state_of(10)))
    assert sent == [(2, SnapshotReply(2, True, 10))]
    assert (node.base, node.base_term, node.log, node.commit_index, node.last_applied) == (10, 2, [], 10, 10)
    assert node.applied == [(i, f"e{i}") for i in range(1, 11)] and node.current_term == 2
    node.crash()
    node.restart()                                                       # and it is durable
    assert (node.base, node.base_term, node.current_term, node.commit_index) == (10, 2, 2, 10)
    assert node.applied == [(i, f"e{i}") for i in range(1, 11)]


def test_replication_continues_from_the_snapshot_point():
    c = Cluster(3)
    node = c.nodes[1]
    sent = probe(c, node)
    node.on_message(2, InstallSnapshot(2, 2, 10, 2, state_of(10)))
    sent.clear()
    node.on_message(2, AppendEntries(2, 2, 10, 2, (Entry(2, "e11"),), 11))
    assert sent == [(2, AppendReply(2, True, 11, 0))] and node.last_applied == 11
    sent.clear()
    node.on_message(2, AppendEntries(2, 2, 10, 1, (Entry(2, "x"),), 11))      # wrong term at the boundary: refused
    assert sent[0][1].success is False and node.last_log_index == 11


def test_a_log_that_agrees_with_the_snapshot_keeps_its_tail():
    c = Cluster(3)
    node = c.nodes[1]
    sent = probe(c, node)
    feed(node, 8, commit=2)                                               # entries 1..8 (term 1), 2 committed
    node.on_message(2, InstallSnapshot(1, 2, 5, 1, state_of(5)))
    assert node.base == 5 and [e.command for e in node.log] == ["e6", "e7", "e8"] and node.last_log_index == 8
    assert sent[-1] == (2, SnapshotReply(1, True, 5))


def test_a_log_that_disagrees_with_the_snapshot_is_thrown_away():
    c = Cluster(3)
    node = c.nodes[1]
    feed(node, 8, commit=2)
    node.on_message(2, InstallSnapshot(2, 2, 5, 2, state_of(5)))          # the snapshot says entry 5 has term 2
    assert node.base == 5 and node.log == [] and node.last_log_index == 5


def test_a_snapshot_the_node_already_has_is_not_installed_again():
    c = Cluster(3)
    node = c.nodes[1]
    sent = probe(c, node)
    feed(node, 8)
    applied = list(node.applied)
    node.on_message(2, InstallSnapshot(1, 2, 5, 1, state_of(5)))
    assert sent[-1] == (2, SnapshotReply(1, True, 5))
    assert node.base == 0 and node.applied == applied and len(node.log) == 8


def test_an_old_leaders_snapshot_is_refused():
    c = Cluster(3)
    node = c.nodes[1]
    sent = probe(c, node)
    node.on_message(3, RequestVote(5, 3, 0, 0))
    sent.clear()
    node.on_message(2, InstallSnapshot(2, 2, 10, 2, state_of(10)))
    assert sent == [(2, SnapshotReply(5, False))] and node.base == 0


def test_a_newer_term_in_a_snapshot_is_adopted_and_a_candidate_steps_down():
    c = Cluster(3)
    node = c.nodes[1]
    probe(c, node)
    node._on_election_timeout()
    assert node.state == "candidate"
    node.on_message(2, InstallSnapshot(node.current_term, 2, 4, 1, state_of(4)))
    assert node.state == "follower" and node.leader_id == 2 and node.base == 4


def test_two_leaders_in_one_term_are_still_detected_through_snapshots():
    c = Cluster(3).start()
    leader = elected(c)
    with pytest.raises(ElectionSafetyViolation):
        leader.on_message(2 if leader.id != 2 else 3, InstallSnapshot(leader.current_term, 9, 5, 1, state_of(5)))


@pytest.mark.parametrize("which", [1, 2])                                 # 1 = while saving the snapshot, 2 = while rewriting the journal
@pytest.mark.parametrize("event", ["write", "before_sync", "after_sync", "before_rename", "after_rename"])
def test_a_power_cut_while_installing_sends_no_reply_and_leaves_a_consistent_node(event, which):
    armed, seen = [False], [0]

    def hook(node_id, ev):
        if armed[0] and ev == event:
            seen[0] += 1
            if seen[0] == which:
                raise PowerCut(c.nodes[node_id].storage.disk, ev)
    c = Cluster(3, disk_hook=hook)
    node = c.nodes[1]
    feed(node, 3, commit=1)
    sent_before = c.net.stats["sent"]
    armed[0] = True
    with pytest.raises(PowerCut):
        node.on_message(2, InstallSnapshot(1, 2, 6, 1, state_of(6)))
    node.crash()
    assert c.net.stats["sent"] == sent_before                                       # no acknowledgement left the machine
    armed[0] = False
    node.restart()
    assert (node.base == 0 and node.last_log_index == 3) or (node.base == 6 and node.last_log_index == 6)
    assert node.current_term == 1


def test_append_entries_that_start_inside_the_snapshot_skip_what_the_node_already_has():
    c = Cluster(3)
    node = c.nodes[1]
    sent = probe(c, node)
    node.on_message(2, InstallSnapshot(1, 2, 5, 1, state_of(5)))
    sent.clear()
    node.on_message(2, AppendEntries(1, 2, 3, 1, tuple(Entry(1, f"e{i}") for i in range(4, 9)), 8))
    assert sent == [(2, AppendReply(1, True, 8, 0))]                          # entries 4 and 5 skipped, 6..8 appended
    assert [e.command for e in node.log] == ["e6", "e7", "e8"] and node.last_applied == 8
    sent.clear()
    node.on_message(2, AppendEntries(1, 2, 1, 1, (Entry(1, "e2"), Entry(1, "e3")), 8))
    assert sent == [(2, AppendReply(1, True, 3, 0))] and node.last_log_index == 8   # all of it already covered


# ---------- InstallSnapshot, the leader's side ----------

def leader_with_snapshot():
    c = Cluster(3, snapshot_threshold=4).start()
    leader = elected(c)
    for i in range(12):
        c.propose(f"v{i}")
        c.run_for(30)
    assert c.wait_for(c.converged, 3000) and leader.base >= 8
    sent = probe(c, leader)
    return c, leader, sent


def test_the_leader_sends_a_snapshot_when_the_entries_a_follower_needs_are_gone():
    c, leader, sent = leader_with_snapshot()
    peer = leader.peers[0]
    leader.next_index[peer] = 1
    leader._replicate(peer)
    kind, msg = sent[-1][1].__class__, sent[-1][1]
    assert kind is InstallSnapshot and (msg.last_included_index, msg.last_included_term) == (leader.base, leader.base_term)
    assert msg.state == leader.snapshot_state
    leader.next_index[peer] = leader.base + 1
    leader._replicate(peer)
    assert isinstance(sent[-1][1], AppendEntries) and sent[-1][1].prev_log_index == leader.base
    assert sent[-1][1].prev_log_term == leader.base_term


def test_a_snapshot_reply_moves_the_followers_progress_forward():
    c, leader, sent = leader_with_snapshot()
    peer = leader.peers[0]
    leader.match_index[peer], leader.next_index[peer] = 0, 1
    leader.on_message(peer, SnapshotReply(leader.current_term, True, leader.base))
    assert leader.match_index[peer] == leader.base and leader.next_index[peer] == leader.base + 1


def test_stale_or_failed_snapshot_replies_change_nothing():
    c, leader, sent = leader_with_snapshot()
    peer = leader.peers[0]
    leader.match_index[peer], leader.next_index[peer] = 0, 1
    leader.on_message(peer, SnapshotReply(leader.current_term, False, 5))
    leader.on_message(peer, SnapshotReply(leader.current_term - 1, True, 5))
    assert (leader.match_index[peer], leader.next_index[peer]) == (0, 1)
