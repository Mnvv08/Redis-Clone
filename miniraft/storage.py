"""Simulated disk and Raft's durable state.

Raft is only correct if three things survive a crash: the current term, who this node voted for in
that term, and the log. A node must make them durable BEFORE it tells anybody anything that depends
on them (a vote, an acknowledgement of new entries). This file provides:

  * SimDisk     a disk with the one property that matters: bytes you have written are lost in a
                power cut unless you have synced them. A cut between a write and its sync may even
                keep a random prefix of the unsynced bytes (a "torn" write).
  * RaftStorage an append-only journal of checksummed records on a SimDisk. Recovery replays it,
                stops at the first record that is incomplete or fails its checksum, and cuts the
                damaged tail off so new records start cleanly.

Journal record = 4-byte length, 4-byte CRC32 of the payload, JSON payload:
    {"h": [term, voted_for]}               hard state (term and vote change together, atomically)
    {"l": [from_index, [[term, cmd]...]]}  "delete log entries from_index on, then append these"
Commands must therefore be JSON values (None, bool, int, float, str, list, dict with str keys).

Step 7 adds snapshots. A snapshot is the state machine's state after applying log entries 1..N. Once it
is safely on disk those entries are not needed any more, so the journal is REWRITTEN to start after
them. Two more files and two more record kinds:

    raft.snapshot                          one record {"i": N, "t": term of entry N, "s": state}
    {"b": [N, term]}                       first record of a compacted journal: "the log starts after N"

Both files are replaced the only safe way: write a temporary file, sync it, then rename it over the old
one (rename is atomic: you see the old file or the new one, never a mix). A power cut between the two
replacements leaves a new snapshot next to an old journal; recovery reconciles them (load_all).
"""
import json
import random
import struct
import zlib
from dataclasses import dataclass

HEADER = struct.Struct("<II")
JOURNAL = "raft.journal"
SNAPSHOT = "raft.snapshot"


class RecoveryError(Exception):
    """The files on disk cannot be turned back into a consistent node (data was lost)."""


class PowerCut(BaseException):
    """Raised by a fault hook to simulate the power failing at exactly this disk event."""

    def __init__(self, disk, event):
        super().__init__(event)
        self.disk, self.event = disk, event


class SimDisk:
    """Files that are durable only once synced. Fault hooks see the events write, before_sync, after_sync,
    before_rename and after_rename. rename() is atomic and takes the file's unsynced bytes with it, so
    renaming a file that was never synced still loses (or tears) its content in a power cut."""

    def __init__(self, rng=None, owner=None, hook=None, sync=True, tear=True):
        self.rng = rng or random.Random(0)
        self.owner, self.hook, self.honest_sync, self.tear = owner, hook, sync, tear
        self.durable = {}
        self.pending = {}
        self.stats = {"writes": 0, "syncs": 0, "crashes": 0, "torn_bytes": 0, "renames": 0}

    def _event(self, name):
        if self.hook is not None:
            self.hook(self, name)

    def write(self, name, data):
        self._event("write")
        self.pending[name] = self.pending.get(name, b"") + bytes(data)
        self.stats["writes"] += 1

    def sync(self, name):
        self._event("before_sync")
        if self.honest_sync:                              # sync=False models forgetting to fsync
            self.durable[name] = self.durable.get(name, b"") + self.pending.pop(name, b"")
        self.stats["syncs"] += 1
        self._event("after_sync")

    def read(self, name):
        """What a freshly started process would find on disk."""
        return self.durable.get(name, b"")

    def rename(self, src, dst):
        """Atomically replace dst with src."""
        self._event("before_rename")
        self.durable[dst] = self.durable.pop(src, b"")
        if src in self.pending:
            self.pending[dst] = self.pending.pop(src)
        else:
            self.pending.pop(dst, None)
        self.stats["renames"] += 1
        self._event("after_rename")

    def remove(self, name):
        self.durable.pop(name, None)
        self.pending.pop(name, None)

    def truncate(self, name, size):
        self.durable[name] = self.durable.get(name, b"")[:size]

    def crash(self):
        """Power cut: unsynced bytes are gone, except that the disk may have kept a prefix of them."""
        self.stats["crashes"] += 1
        for name, data in self.pending.items():
            keep = self.rng.randint(0, len(data)) if self.tear else 0
            self.stats["torn_bytes"] += keep
            if keep:
                self.durable[name] = self.durable.get(name, b"") + data[:keep]
        self.pending = {}


def frame(record):
    payload = json.dumps(record, separators=(",", ":")).encode()
    return HEADER.pack(len(payload), zlib.crc32(payload)) + payload


@dataclass
class Recovered:
    term: int
    voted_for: object
    log: list              # [(term, command)] for the entries AFTER the snapshot
    base: int              # index of the last entry the snapshot covers (0 = no snapshot)
    base_term: int
    state: object          # the snapshot's state machine state, or None


class RaftStorage:
    def __init__(self, disk):
        self.disk = disk

    # ----- writing: every call returns only after the record is synced

    def _append(self, record):
        self.disk.write(JOURNAL, frame(record))
        self.disk.sync(JOURNAL)

    def _replace(self, name, data):
        """The only safe way to change a file as a whole: temp file, sync, atomic rename."""
        tmp = name + ".tmp"
        self.disk.remove(tmp)
        self.disk.write(tmp, data)
        self.disk.sync(tmp)
        self.disk.rename(tmp, name)

    def save_snapshot(self, index, term, state):
        self._replace(SNAPSHOT, frame({"i": index, "t": term, "s": state}))

    def compact(self, base_index, base_term, term, voted_for, entries):
        """Rewrite the journal so it starts after base_index and holds only what is still needed."""
        data = frame({"b": [base_index, base_term]}) + frame({"h": [term, voted_for]})
        if entries:
            data += frame({"l": [base_index + 1, [[e.term, e.command] for e in entries]]})
        self._replace(JOURNAL, data)

    def save_hard_state(self, term, voted_for):
        self._append({"h": [term, voted_for]})

    def save_log(self, from_index, entries):
        """Make the log hold `entries` from from_index on (anything after that point is dropped)."""
        self._append({"l": [from_index, [[e.term, e.command] for e in entries]]})

    # ----- reading

    def load(self):
        """(term, voted_for, [(term, command), ...]): the journal's view without the snapshot details."""
        r = self.load_all()
        return r.term, r.voted_for, r.log

    def _read_snapshot(self):
        data = self.disk.read(SNAPSHOT)
        if len(data) < HEADER.size:
            return None, bool(data)
        length, crc = HEADER.unpack_from(data, 0)
        payload = data[HEADER.size:HEADER.size + length]
        if len(payload) != length or zlib.crc32(payload) != crc or len(data) != HEADER.size + length:
            return None, True
        try:
            record = json.loads(payload)
            index, term, state = record["i"], record["t"], record["s"]
        except (ValueError, KeyError, TypeError):
            return None, True
        if not all(isinstance(x, int) and not isinstance(x, bool) for x in (index, term)) or index < 1:
            return None, True
        return (index, term, state), False

    def load_all(self):
        """Rebuild everything from disk. Cuts off a damaged journal tail, deletes leftover temp files,
        and reconciles the snapshot with the journal (they can disagree after a power cut between the
        two replacements). Raises RecoveryError if the files cannot be made consistent."""
        for name in (SNAPSHOT, JOURNAL):
            self.disk.remove(name + ".tmp")
        snap, snap_damaged = self._read_snapshot()
        data = self.disk.read(JOURNAL)
        term, voted_for, log = 0, None, []
        jbase, jbase_term = 0, 0
        pos = 0
        while pos + HEADER.size <= len(data):
            length, crc = HEADER.unpack_from(data, pos)
            end = pos + HEADER.size + length
            payload = data[pos + HEADER.size:end]
            if end > len(data) or zlib.crc32(payload) != crc:
                break
            try:
                record = json.loads(payload)
                if "b" in record:
                    new_base, new_base_term = record["b"]
                    if pos != 0 or not isinstance(new_base, int) or not isinstance(new_base_term, int) or new_base < 1:
                        break
                    jbase, jbase_term = new_base, new_base_term
                elif "h" in record:
                    new_term, new_vote = record["h"]
                    if not isinstance(new_term, int) or new_term < term:
                        break
                    term, voted_for = new_term, new_vote
                else:
                    from_index, entries = record["l"]
                    if not isinstance(from_index, int) or not jbase + 1 <= from_index <= jbase + len(log) + 1:
                        break
                    log[from_index - jbase - 1:] = [(t, c) for t, c in entries]
            except (ValueError, KeyError, TypeError):
                break
            pos = end
        if pos < len(data):
            self.disk.truncate(JOURNAL, pos)
        if snap is None:
            if jbase > 0:
                raise RecoveryError(f"the journal starts after entry {jbase} but there is no readable snapshot")
            return Recovered(term, voted_for, log, 0, 0, None)       # a damaged snapshot with a full journal is not needed
        index, snap_term, state = snap
        if index < jbase:
            raise RecoveryError(f"the journal starts after entry {jbase} but the snapshot only covers {index}")
        if index == jbase:
            if snap_term != jbase_term:
                raise RecoveryError("the snapshot and the journal disagree about the last compacted entry")
        elif jbase < index <= jbase + len(log) and log[index - jbase - 1][0] == snap_term:
            log = log[index - jbase:]                                # the journal agrees: keep what follows
        else:
            log = []                                                 # the journal's log is not an extension of the snapshot
        return Recovered(term, voted_for, log, index, snap_term, state)
