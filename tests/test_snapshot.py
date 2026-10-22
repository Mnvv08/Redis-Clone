import pytest

from raft import AppendEntries, Cluster, Entry, LeaderCompletenessViolation, StateMachineSafetyViolation
from storage import JOURNAL, SNAPSHOT, RecoveryError


def elected(c, timeout=3000):
    assert c.wait_for(lambda: c.stable_leader() is not None, timeout)
    return c.stable_leader()


def probe(c, node):
    sent = []
    node.send = lambda dst, msg: sent.append((dst, msg))
    return sent


def feed(node, count, term=1, leader=2, commit=None, start=0):
    """Give a follower `count` entries (commands 'e1', 'e2', ...) and tell it how many are committed."""
    entries = tuple(Entry(term, f"e{start + i + 1}") for i in range(count))
    node.on_message(leader, AppendEntries(term, leader, start, 0 if start == 0 else term, entries, count + start if commit is None else commit))


def state_of(n):
    return [[i, f"e{i}"] for i in range(1, n + 1)]


# ---------- one node, one snapshot ----------

def test_the_node_snapshots_when_enough_entries_have_been_applied():
    c = Cluster(3, snapshot_threshold=3)
    node = c.nodes[1]
    feed(node, 5)
    assert node.base == 3 and node.base_term == 1 and len(node.log) == 2 and node.last_log_index == 5
    assert node.snapshot_state == state_of(3)                         # the default state machine: the applied commands
    assert (node.commit_index, node.last_applied) == (5, 5)
    assert node.entry_at(4).command == "e4" and node.term_at(3) == 1 and node.term_at(5) == 1


def test_index_helpers_work_across_the_snapshot_boundary():
    c = Cluster(3)
    node = c.nodes[1]
    feed(node, 6)
    node.take_snapshot()
    assert node.base == 6 and node.log == []
    assert node.last_log_index == 6 and node.last_log_term == 1 and node.term_at(6) == 1
    feed(node, 2, start=6)
    assert node.last_log_index == 8 and node.entry_at(8).command == "e8" and node.term_at(6) == 1
    for compacted in (0, 3, 6):
        with pytest.raises(IndexError):
            node.entry_at(compacted)                                      # gone, and never silently the wrong entry
    with pytest.raises(IndexError):
        node.term_at(3)
    with pytest.raises(IndexError):
        node.entry_at(9)


def test_a_snapshot_never_covers_anything_that_is_not_applied():
    c = Cluster(3)
    node = c.nodes[1]
    feed(node, 5, commit=2)                                           # five entries, only two committed
    assert node.take_snapshot() is True and node.base == 2 and len(node.log) == 3
    assert node.take_snapshot() is False and node.base == 2           # nothing new to cover


def test_snapshots_are_off_unless_asked_for():
    c = Cluster(3)
    feed(c.nodes[1], 50)
    assert c.nodes[1].base == 0 and len(c.nodes[1].log) == 50


def test_a_restart_continues_from_the_snapshot():
    c = Cluster(3, snapshot_threshold=3)
    node = c.nodes[1]
    feed(node, 5)
    node.crash()
    node.restart()
    assert (node.base, node.base_term, len(node.log), node.last_log_index) == (3, 1, 2, 5)
    assert node.commit_index == node.last_applied == 3 and node.applied == [(i, f"e{i}") for i in range(1, 4)]
    feed(node, 0, commit=5, start=5)                                   # the leader's next heartbeat
    node.on_message(2, AppendEntries(1, 2, 5, 1, (), 5))
    assert node.last_applied == 5 and [i for i, _ in node.applied][-2:] == [4, 5]


def test_the_journal_stays_small_while_the_log_it_describes_grows():
    plain, compact = Cluster(3), Cluster(3, snapshot_threshold=10)
    for c in (plain, compact):
        for start in range(0, 200, 20):
            feed(c.nodes[1], 20, start=start)
    size = lambda c: len(c.nodes[1].storage.disk.read(JOURNAL))
    assert compact.nodes[1].base >= 190 and len(compact.nodes[1].log) < 10
    assert size(compact) * 4 < size(plain)                               # the plain journal holds all 200 entries


def test_a_restart_with_unrecoverable_files_refuses_to_start():
    c = Cluster(3, snapshot_threshold=3)
    node = c.nodes[1]
    feed(node, 5)
    node.crash()
    node.storage.disk.remove(SNAPSHOT)
    with pytest.raises(RecoveryError):
        node.restart()


# ---------- the safety monitors understand snapshots ----------

def test_a_snapshot_must_agree_with_what_was_committed_at_its_index():
    c = Cluster(3)
    feed(c.nodes[1], 5)                                                 # entries 1..5, term 1, now in c.committed
    c._on_snapshot(c.nodes[1], 3, 1, state_of(3))                       # fine
    with pytest.raises(StateMachineSafetyViolation):
        c._on_snapshot(c.nodes[1], 3, 7, state_of(3))                   # the committed entry 3 has term 1, not 7


def test_a_leader_whose_snapshot_boundary_disagrees_with_a_committed_entry_is_caught():
    c = Cluster(3)
    node = c.nodes[1]
    feed(node, 5)
    node.take_snapshot()
    c._record_leader(node)                                              # consistent: fine
    node.base_term = 9
    with pytest.raises(LeaderCompletenessViolation):
        c._record_leader(node)


def test_converged_compares_the_part_of_the_logs_both_nodes_still_hold():
    c = Cluster(3)
    one, two = c.nodes[1], c.nodes[2]
    c.nodes[3].crash()
    feed(one, 4)
    feed(two, 4, commit=2)
    two.take_snapshot()                                                 # base 2, log holds entries 3 and 4
    two.commit_index = 4
    assert c.converged()
    two.log[0] = Entry(9, "different")                                  # same length, same commit index, other content
    assert not c.converged()
