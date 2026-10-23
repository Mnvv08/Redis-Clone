import os

import pytest

import realdisk
from raft import Entry
from realdisk import RealDisk
from storage import JOURNAL, SNAPSHOT, RaftStorage, frame


def E(term, command=None):
    return Entry(term, command)


# ---------- the disk itself ----------

def test_written_bytes_can_be_read_back(tmp_path):
    d = RealDisk(str(tmp_path))
    assert d.read("f") == b""
    d.write("f", b"hello")
    d.write("f", b" world")
    d.sync("f")
    assert d.read("f") == b"hello world"
    assert RealDisk(str(tmp_path)).read("f") == b"hello world"            # a new process sees it too


def test_sync_really_calls_fsync_and_a_new_file_also_syncs_its_directory(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(RealDisk, "_flush", staticmethod(lambda fd: calls.append(fd)))
    d = RealDisk(str(tmp_path))
    d.write("f", b"x")
    assert calls == []                                                     # write alone is not a sync
    d.sync("f")
    assert len(calls) == 2 and d.stats["dir_syncs"] == 1                   # the file, and the directory entry that names it
    d.write("f", b"y")
    d.sync("f")
    assert len(calls) == 3 and d.stats["dir_syncs"] == 1                   # an existing file needs no directory sync


def test_on_macos_the_stronger_full_fsync_is_used(tmp_path, monkeypatch):
    seen = []

    class FakeFcntl:
        F_FULLFSYNC = 51

        @staticmethod
        def fcntl(fd, op):
            seen.append(op)
    monkeypatch.setattr(realdisk, "fcntl", FakeFcntl)
    monkeypatch.setattr(realdisk.sys, "platform", "darwin")
    d = RealDisk(str(tmp_path))
    d.write("f", b"x")
    d.sync("f")
    assert seen == [51, 51]


def test_rename_replaces_the_destination_and_later_writes_go_to_the_new_file(tmp_path):
    d = RealDisk(str(tmp_path))
    d.write("a", b"old")
    d.sync("a")
    d.write("b", b"new")
    d.sync("b")
    d.rename("b", "a")
    assert d.read("a") == b"new" and d.read("b") == b"" and not os.path.exists(tmp_path / "b")
    d.write("a", b"+more")                                                 # the old descriptor must not be reused
    d.sync("a")
    assert d.read("a") == b"new+more"
    assert d.stats["renames"] == 1 and d.stats["dir_syncs"] >= 3


def test_remove_and_truncate(tmp_path):
    d = RealDisk(str(tmp_path))
    d.remove("missing")                                                    # not an error
    d.truncate("missing", 3)
    d.write("f", b"abcdef")
    d.sync("f")
    d.truncate("f", 2)
    assert d.read("f") == b"ab"
    d.write("f", b"Z")                                                     # appending continues from the cut
    d.sync("f")
    assert d.read("f") == b"abZ"
    d.remove("f")
    assert d.read("f") == b"" and not os.path.exists(tmp_path / "f")


def test_truncate_is_synced_too(tmp_path, monkeypatch):
    d = RealDisk(str(tmp_path))
    d.write("f", b"abcdef")
    d.sync("f")
    calls = []
    monkeypatch.setattr(RealDisk, "_flush", staticmethod(lambda fd: calls.append(fd)))
    d.truncate("f", 2)
    assert len(calls) == 1                                                 # a cut that is not flushed can come back after a crash


def test_crash_only_releases_descriptors(tmp_path):
    d = RealDisk(str(tmp_path))
    d.write("f", b"x")
    d.sync("f")
    d.crash()
    assert d._fds == {} and d.read("f") == b"x"
    d.write("f", b"y")                                                     # and it keeps working
    d.sync("f")
    assert d.read("f") == b"xy"


# ---------- RaftStorage on a real disk ----------

def reopen(path):
    return RaftStorage(RealDisk(str(path)))


def test_hard_state_and_log_survive_a_restart(tmp_path):
    s = reopen(tmp_path)
    s.save_hard_state(3, 2)
    s.save_log(1, [E(1, "a"), E(2, {"k": [1, None]})])
    s.save_log(2, [E(3, "B")])
    assert reopen(tmp_path).load() == (3, 2, [(1, "a"), (3, "B")])


def test_a_torn_journal_tail_is_cut_off_like_after_a_real_crash(tmp_path):
    s = reopen(tmp_path)
    s.save_hard_state(1, 1)
    s.save_log(1, [E(1, "a")])
    good = len(s.disk.read(JOURNAL))
    s.disk.write(JOURNAL, frame({"l": [2, [[1, "half written"]]]})[:-4])   # the power failed in the middle of a record
    s.disk.sync(JOURNAL)
    again = reopen(tmp_path)
    assert again.load() == (1, 1, [(1, "a")])
    assert os.path.getsize(tmp_path / JOURNAL) == good                     # the damaged tail is really gone from the file
    again.save_log(2, [E(1, "b")])
    assert reopen(tmp_path).load()[2] == [(1, "a"), (1, "b")]


def test_a_snapshot_and_compaction_survive_a_restart_and_leave_no_temp_files(tmp_path):
    s = reopen(tmp_path)
    s.save_hard_state(2, 1)
    s.save_log(1, [E(1, "a"), E(1, "b"), E(2, "c")])
    s.save_snapshot(2, 1, {"x": [1, 2]})
    s.compact(2, 1, 2, 1, [E(2, "c")])
    r = reopen(tmp_path).load_all()
    assert (r.term, r.voted_for, r.base, r.base_term, r.state, r.log) == (2, 1, 2, 1, {"x": [1, 2]}, [(2, "c")])
    assert sorted(os.listdir(tmp_path)) == [JOURNAL, SNAPSHOT]


def test_leftover_temp_files_are_deleted_on_load(tmp_path):
    (tmp_path / (SNAPSHOT + ".tmp")).write_bytes(b"half a snapshot")
    (tmp_path / (JOURNAL + ".tmp")).write_bytes(b"half a journal")
    reopen(tmp_path).load_all()
    assert os.listdir(tmp_path) == []
