import random

import pytest

from raft import Entry
from storage import JOURNAL, SNAPSHOT, PowerCut, RaftStorage, RecoveryError, SimDisk, frame


def E(term, command=None):
    return Entry(term, command)


STATE = {"x": 1, "list": [1, 2]}


def populated(**options):
    """A journal holding term 2, a vote, and entries 1..4 with terms 1, 1, 2, 2."""
    s = RaftStorage(SimDisk(**options))
    s.save_hard_state(2, 1)
    s.save_log(1, [E(1, "a"), E(1, "b"), E(2, "c"), E(2, "d")])
    return s


def compacted():
    s = populated()
    s.save_snapshot(2, 1, STATE)
    s.compact(2, 1, 2, 1, [E(2, "c"), E(2, "d")])
    return s


def logical(r):
    return (r.term, r.voted_for, r.base, r.base_term, r.state, r.log)


# ---------- the disk: rename ----------

def test_rename_replaces_a_file_atomically():
    d = SimDisk(tear=False)
    d.write("a", b"old")
    d.sync("a")
    d.write("b", b"new")
    d.sync("b")
    d.rename("b", "a")
    assert d.read("a") == b"new" and d.read("b") == b""


def test_renaming_a_file_that_was_never_synced_loses_everything_in_a_power_cut():
    d = SimDisk(tear=False)
    d.write("a", b"old")
    d.sync("a")
    d.write("b", b"new")                                       # no sync
    d.rename("b", "a")
    d.crash()
    assert d.read("a") == b""                                  # the old version is gone and the new one never arrived


def test_a_power_cut_can_fall_on_either_side_of_a_rename():
    def run(cut):
        def hook(disk, event):
            if event == cut:
                raise PowerCut(disk, event)
        d = SimDisk(hook=hook, tear=False)
        d.hook = None
        d.write("a", b"old")
        d.sync("a")
        d.write("b", b"new")
        d.sync("b")
        d.hook = hook
        with pytest.raises(PowerCut):
            d.rename("b", "a")
        d.crash()
        return d.read("a")
    assert run("before_rename") == b"old"
    assert run("after_rename") == b"new"


def test_the_unsynced_bytes_of_a_renamed_file_travel_with_it_and_may_survive_torn():
    kept = set()
    for seed in range(40):
        d = SimDisk(random.Random(seed))
        d.write("tmp", b"0123456789")                              # never synced
        d.rename("tmp", "a")
        assert d.pending == {"a": b"0123456789"}
        d.crash()
        kept.add(len(d.read("a")))
        assert b"0123456789".startswith(d.read("a"))
    assert 0 in kept and len(kept) > 5


def test_a_rename_replaces_the_destinations_unsynced_bytes_too():
    for seed in range(40):
        d = SimDisk(random.Random(seed))
        d.write("a", b"stale unsynced bytes")                     # pending data of the file about to be replaced
        d.write("tmp", b"new")
        d.sync("tmp")
        d.rename("tmp", "a")
        d.crash()
        assert d.read("a") == b"new"                                # none of the stale bytes may reappear


def test_a_leftover_temp_file_cannot_leak_into_the_next_snapshot():
    s = RaftStorage(SimDisk(tear=False))
    s.disk.durable[SNAPSHOT + ".tmp"] = b"junk from a crashed attempt"
    s.disk.durable[JOURNAL + ".tmp"] = b"more junk"
    s.save_snapshot(2, 1, STATE)
    s.compact(2, 1, 3, None, [])
    assert logical(s.load_all()) == (3, None, 2, 1, STATE, [])


def test_remove_deletes_durable_and_pending_bytes():
    d = SimDisk()
    d.write("f", b"x")
    d.sync("f")
    d.write("f", b"y")
    d.remove("f")
    assert d.read("f") == b"" and d.pending == {}


# ---------- snapshot and compaction ----------

def test_a_snapshot_round_trips_with_the_rest_of_the_state():
    s = compacted()
    r = s.load_all()
    assert logical(r) == (2, 1, 2, 1, STATE, [(2, "c"), (2, "d")])


def test_compaction_makes_the_journal_smaller_and_it_stays_usable():
    s = populated()
    before = len(s.disk.read(JOURNAL))
    s.save_snapshot(4, 2, STATE)
    s.compact(4, 2, 2, 1, [])
    assert len(s.disk.read(JOURNAL)) < before
    s.save_log(5, [E(3, "e"), E(3, "f")])
    s.save_hard_state(3, None)
    assert logical(s.load_all()) == (3, None, 4, 2, STATE, [(3, "e"), (3, "f")])
    s.save_log(6, [E(3, "F")])
    assert s.load_all().log == [(3, "e"), (3, "F")]


def test_log_records_that_point_into_the_snapshot_are_invalid():
    s = compacted()
    s.save_log(2, [E(9, "evil")])                                # index 2 is covered by the snapshot
    assert s.load_all().log == [(2, "c"), (2, "d")]
    s.save_log(7, [E(9, "gap")])                                 # and a gap is invalid too
    assert s.load_all().log == [(2, "c"), (2, "d")]


def test_a_new_snapshot_next_to_an_old_journal_is_reconciled():
    """The power failed between replacing the snapshot and replacing the journal."""
    s = populated()
    s.save_snapshot(2, 1, STATE)                                 # journal still holds entries 1..4
    r = s.load_all()
    assert (r.base, r.base_term, r.log) == (2, 1, [(2, "c"), (2, "d")])


def test_a_snapshot_whose_term_disagrees_with_the_journal_discards_the_journals_log():
    s = populated()
    s.save_snapshot(3, 9, STATE)                                 # the journal says entry 3 has term 2
    r = s.load_all()
    assert (r.base, r.base_term, r.log) == (3, 9, [])


def test_a_snapshot_beyond_the_end_of_the_journal_discards_the_log():
    s = populated()
    s.save_snapshot(10, 2, STATE)
    assert (s.load_all().base, s.load_all().log) == (10, [])


def test_a_journal_that_starts_after_the_snapshot_is_unrecoverable():
    s = compacted()
    s.disk.durable[SNAPSHOT] = frame({"i": 1, "t": 1, "s": {}})  # an older snapshot than the journal assumes
    with pytest.raises(RecoveryError):
        s.load_all()


def test_a_snapshot_and_journal_that_disagree_about_the_boundary_are_unrecoverable():
    s = compacted()
    s.disk.durable[SNAPSHOT] = frame({"i": 2, "t": 7, "s": {}})
    with pytest.raises(RecoveryError):
        s.load_all()


def test_a_compacted_journal_without_its_snapshot_is_unrecoverable():
    s = compacted()
    s.disk.remove(SNAPSHOT)
    with pytest.raises(RecoveryError):
        s.load_all()
