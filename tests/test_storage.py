import json
import random
import struct
import zlib

import pytest

from raft import Entry
from storage import HEADER, JOURNAL, PowerCut, RaftStorage, SimDisk


def E(term, command=None):
    return Entry(term, command)


def record(obj):
    payload = json.dumps(obj, separators=(",", ":")).encode()
    return HEADER.pack(len(payload), zlib.crc32(payload)) + payload


# ---------- the simulated disk ----------

def test_written_bytes_are_not_durable_until_synced():
    d = SimDisk(tear=False)
    d.write("f", b"hello")
    assert d.read("f") == b""
    d.sync("f")
    assert d.read("f") == b"hello"
    d.write("f", b" world")
    d.crash()
    assert d.read("f") == b"hello"


def test_a_crash_may_keep_a_random_prefix_of_unsynced_bytes_but_never_a_gap():
    seen = set()
    for seed in range(60):
        d = SimDisk(random.Random(seed))
        d.write("f", b"0123456789")
        d.crash()
        kept = d.read("f")
        assert b"0123456789".startswith(kept)
        seen.add(len(kept))
    assert 0 in seen and 10 in seen and len(seen) > 5


def test_files_are_independent():
    d = SimDisk(tear=False)
    d.write("a", b"1")
    d.write("b", b"2")
    d.sync("a")
    d.crash()
    assert d.read("a") == b"1" and d.read("b") == b""


def test_truncate_cuts_the_durable_bytes():
    d = SimDisk()
    d.write("f", b"abcdef")
    d.sync("f")
    d.truncate("f", 2)
    assert d.read("f") == b"ab"


def test_fault_hook_sees_every_step_and_can_cut_the_power():
    events = []
    d = SimDisk(hook=lambda disk, event: events.append(event))
    d.write("f", b"x")
    d.sync("f")
    assert events == ["write", "before_sync", "after_sync"]

    def cut_at(name):
        def hook(disk, event):
            if event == name:
                raise PowerCut(disk, event)
        return hook
    d = SimDisk(hook=cut_at("write"), tear=False)
    with pytest.raises(PowerCut):
        d.write("f", b"x")
    assert d.pending == {}
    d = SimDisk(hook=cut_at("before_sync"), tear=False)
    d.write("f", b"x")
    with pytest.raises(PowerCut):
        d.sync("f")
    d.crash()
    assert d.read("f") == b""
    d = SimDisk(hook=cut_at("after_sync"), tear=False)
    d.write("f", b"x")
    with pytest.raises(PowerCut):
        d.sync("f")
    d.crash()
    assert d.read("f") == b"x"                               # it was already safe on disk


def test_a_disk_that_ignores_sync_loses_everything():
    d = SimDisk(sync=False, tear=False)
    d.write("f", b"x")
    d.sync("f")
    d.crash()
    assert d.read("f") == b""


# ---------- the journal ----------

def test_a_fresh_disk_loads_as_an_empty_node():
    assert RaftStorage(SimDisk()).load() == (0, None, [])


def test_hard_state_and_log_round_trip():
    s = RaftStorage(SimDisk())
    s.save_hard_state(3, 2)
    s.save_log(1, [E(1, "a"), E(2, "b")])
    s.save_hard_state(4, None)
    s.save_log(3, [E(4, None)])
    assert RaftStorage(s.disk).load() == (4, None, [(1, "a"), (2, "b"), (4, None)])


def test_a_log_write_replaces_everything_from_its_index():
    s = RaftStorage(SimDisk())
    s.save_log(1, [E(1, "a"), E(1, "b"), E(1, "c")])
    s.save_log(2, [E(2, "X")])
    assert s.load() == (0, None, [(1, "a"), (2, "X")])
    s.save_log(1, [E(3, "fresh")])
    assert s.load()[2] == [(3, "fresh")]


@pytest.mark.parametrize("command", [None, True, False, 0, -5, 2 ** 70, 2.5, "", "héllo ✓ 日本", [1, [2, None]], {"a": {"b": None}, "k": [1, "x"]}])
def test_json_commands_come_back_equal(command):
    s = RaftStorage(SimDisk())
    s.save_log(1, [E(1, command)])
    (_, _, [(term, back)]) = s.load()
    assert back == command and type(back) is type(command)


def build_journal():
    """A journal and the state after each of its records."""
    records = [{"h": [1, 2]}, {"l": [1, [[1, "a"], [1, "b"]]]}, {"h": [2, None]}, {"l": [2, [[2, "c"], [2, "d"]]]},
               {"h": [3, 1]}, {"l": [3, [[3, "e"]]]}]
    data, states, boundaries = b"", [(0, None, [])], [0]
    term, vote, log = 0, None, []
    for r in records:
        data += record(r)
        if "h" in r:
            term, vote = r["h"]
        else:
            i, entries = r["l"]
            log[i - 1:] = [(t, c) for t, c in entries]
        states.append((term, vote, list(log)))
        boundaries.append(len(data))
    return data, states, boundaries


def test_every_possible_torn_tail_recovers_to_the_last_complete_record():
    data, states, boundaries = build_journal()
    for cut in range(len(data) + 1):
        disk = SimDisk()
        disk.durable[JOURNAL] = data[:cut]
        s = RaftStorage(disk)
        complete = max(i for i, b in enumerate(boundaries) if b <= cut)
        assert s.load() == states[complete], cut
        assert len(disk.read(JOURNAL)) == boundaries[complete]           # the damaged tail was cut off
        s.save_hard_state(9, 1)                                            # and the journal is usable again
        assert RaftStorage(disk).load()[:2] == (9, 1)


def test_a_flipped_bit_stops_recovery_at_that_record():
    data, states, boundaries = build_journal()
    for record_index in range(len(boundaries) - 1):
        for offset in (0, 5, 9, boundaries[record_index + 1] - boundaries[record_index] - 1):
            bad = bytearray(data)
            bad[boundaries[record_index] + offset] ^= 0x01
            disk = SimDisk()
            disk.durable[JOURNAL] = bytes(bad)
            assert RaftStorage(disk).load() == states[record_index], (record_index, offset)


def test_garbage_after_the_journal_is_ignored_and_removed():
    data, states, boundaries = build_journal()
    for junk in (b"\x00" * 3, b"\xff" * 40, b"garbage bytes that look like nothing", struct.pack("<II", 5, 0) + b"abcde"):
        disk = SimDisk()
        disk.durable[JOURNAL] = data + junk
        assert RaftStorage(disk).load() == states[-1]
        assert disk.read(JOURNAL) == data


@pytest.mark.parametrize("bad", [
    {"l": [5, [[1, "x"]]]},          # a gap: there is no entry 1..4
    {"l": [0, [[1, "x"]]]},          # indexes start at 1
    {"l": ["1", [[1, "x"]]]},
    {"h": [0, 3]},                   # the term would go backwards
    {"h": [1]}, {"x": 1}, {"l": [1]}, {"l": [1, [[1]]]}, [1, 2], 7, "text",
])
def test_structurally_invalid_records_end_recovery(bad):
    disk = SimDisk()
    disk.durable[JOURNAL] = record({"h": [1, 2]}) + record(bad) + record({"h": [9, 9]})
    assert RaftStorage(disk).load()[:2] == (1, 2)
    assert len(disk.read(JOURNAL)) == len(record({"h": [1, 2]}))


def test_a_record_that_is_not_json_ends_recovery_even_with_a_valid_checksum():
    payload = b"\xff\xfe not json"
    disk = SimDisk()
    disk.durable[JOURNAL] = record({"h": [1, 2]}) + HEADER.pack(len(payload), zlib.crc32(payload)) + payload
    assert RaftStorage(disk).load()[:2] == (1, 2)


def test_a_power_cut_at_any_disk_event_leaves_the_state_before_or_after_the_operation():
    """Random operations, a power cut at a random disk event: recovery gives a state that really existed."""
    for seed in range(400):
        rng = random.Random(seed)
        cut_after = rng.randrange(1, 60)
        count = [0]

        def hook(disk, event):
            count[0] += 1
            if count[0] == cut_after:
                raise PowerCut(disk, event)
        disk = SimDisk(random.Random(seed), hook=hook)
        s = RaftStorage(disk)
        term, vote, log = 0, None, []
        before = (term, vote, list(log))
        after = before
        try:
            for _ in range(40):
                before = (term, vote, list(log))
                if rng.random() < 0.4:
                    term += rng.randrange(0, 3)
                    vote = rng.choice([None, 1, 2, 3])
                    after = (term, vote, list(log))
                    s.save_hard_state(term, vote)
                else:
                    i = rng.randrange(1, len(log) + 2)
                    entries = [(term, rng.choice(["a", "b", None, 7])) for _ in range(rng.randrange(1, 4))]
                    new_log = log[:i - 1] + entries
                    after = (term, vote, list(new_log))
                    s.save_log(i, [E(t, c) for t, c in entries])
                    log = new_log
                after = (term, vote, list(log))
        except PowerCut:
            pass
        else:
            continue
        disk.crash()
        recovered = RaftStorage(disk).load()
        assert recovered in (before, after), (seed, recovered, before, after)
        RaftStorage(disk).save_hard_state(recovered[0] + 1, None)           # and it keeps working afterwards
        assert RaftStorage(disk).load()[0] == recovered[0] + 1
