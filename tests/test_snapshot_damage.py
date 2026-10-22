import random
import zlib

import pytest

from storage import HEADER, JOURNAL, SNAPSHOT, PowerCut, RaftStorage, RecoveryError, SimDisk, frame
from test_snapshot_storage import E, STATE, compacted, logical, populated


def good_snapshot():
    return frame({"i": 2, "t": 1, "s": STATE})


def silently_changed():
    """Valid JSON, valid shape, wrong content: only the checksum can tell."""
    original = frame({"i": 2, "t": 1, "s": {"x": 1}})
    return original.replace(b'"x":1', b'"x":2')


def bad_snapshots():
    good = good_snapshot()
    flipped = bytearray(good)
    flipped[-1] ^= 1
    payload = b"\xff not json"
    return [good[:-3], bytes(flipped), good + b"junk", b"\x01",
            HEADER.pack(len(payload), zlib.crc32(payload)) + payload,
            frame({"i": 2}), frame({"i": 0, "t": 1, "s": {}}), frame({"i": True, "t": 1, "s": {}}), frame([1, 2, 3]),
            silently_changed()]


@pytest.mark.parametrize("damaged", range(10))
def test_a_damaged_snapshot_is_fatal_once_the_journal_depends_on_it(damaged):
    s = compacted()
    s.disk.durable[SNAPSHOT] = bad_snapshots()[damaged]
    with pytest.raises(RecoveryError):
        s.load_all()


@pytest.mark.parametrize("damaged", range(10))
def test_a_damaged_snapshot_is_ignored_while_the_journal_is_still_complete(damaged):
    s = populated()
    s.disk.durable[SNAPSHOT] = bad_snapshots()[damaged]
    r = s.load_all()
    assert (r.base, r.state, len(r.log)) == (0, None, 4)


def test_leftover_temporary_files_from_a_crash_are_deleted():
    s = compacted()
    s.disk.durable[SNAPSHOT + ".tmp"] = b"half a snapshot"
    s.disk.durable[JOURNAL + ".tmp"] = b"half a journal"
    s.load_all()
    assert s.disk.read(SNAPSHOT + ".tmp") == b"" and s.disk.read(JOURNAL + ".tmp") == b""


def test_a_base_record_anywhere_but_first_ends_recovery():
    disk = SimDisk()
    disk.durable[JOURNAL] = frame({"h": [1, 2]}) + frame({"b": [5, 1]}) + frame({"h": [9, 9]})
    r = RaftStorage(disk).load_all()
    assert (r.term, r.voted_for, r.base) == (1, 2, 0)
    assert len(disk.read(JOURNAL)) == len(frame({"h": [1, 2]}))


@pytest.mark.parametrize("bad", [{"b": [0, 0]}, {"b": ["1", 1]}, {"b": [1]}, {"b": [1, "x"]}])
def test_an_invalid_base_record_ends_recovery(bad):
    disk = SimDisk()
    disk.durable[JOURNAL] = frame(bad) + frame({"h": [3, 1]})
    assert RaftStorage(disk).load_all().term == 0


# ---------- power cuts during a snapshot ----------

def snapshot_and_compact(s):
    s.save_snapshot(2, 1, STATE)
    s.compact(2, 1, 2, 1, [E(2, "c"), E(2, "d")])


@pytest.mark.parametrize("tear", [False, True])
def test_a_power_cut_at_every_disk_event_of_a_snapshot_leaves_the_old_or_the_new_state(tear):
    reference = populated()
    before = logical(reference.load_all())
    snapshot_and_compact(reference)
    after = logical(reference.load_all())
    count = [0]
    total = None
    for cut_at in range(1, 40):
        count[0] = 0
        armed = [False]

        def hook(disk, event):
            if armed[0]:
                count[0] += 1
                if count[0] == cut_at:
                    raise PowerCut(disk, event)
        s = populated(rng=random.Random(cut_at), hook=hook, tear=tear)
        armed[0] = True
        try:
            snapshot_and_compact(s)
        except PowerCut:
            s.disk.crash()
            assert logical(s.load_all()) in (before, after), cut_at
            s.save_log(5, [E(3, "more")])                          # and it keeps working
            assert s.load_all().log[-1] == (3, "more")
        else:
            total = cut_at - 1
            break
    assert total == 10                                              # 5 events per replaced file, 2 files


def test_skipping_the_sync_before_the_rename_loses_data_and_the_sweep_notices(monkeypatch):
    """The negative control: the same sweep against a storage that renames before syncing."""
    def careless(self, name, data):
        tmp = name + ".tmp"
        self.disk.remove(tmp)
        self.disk.write(tmp, data)
        self.disk.rename(tmp, name)                                  # no sync!
    monkeypatch.setattr(RaftStorage, "_replace", careless)
    bad = 0
    for seed in range(40):
        s = populated(rng=random.Random(seed))
        snapshot_and_compact(s)
        s.disk.crash()
        try:
            r = s.load_all()
            bad += r.base != 2
        except RecoveryError:
            bad += 1
    assert bad > 20
