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

The journal only grows; step 7 (snapshots) is where it gets compacted.
"""
import json
import random
import struct
import zlib

HEADER = struct.Struct("<II")
JOURNAL = "raft.journal"


class PowerCut(BaseException):
    """Raised by a fault hook to simulate the power failing at exactly this disk event."""

    def __init__(self, disk, event):
        super().__init__(event)
        self.disk, self.event = disk, event


class SimDisk:
    """Files that are durable only once synced. Fault hooks see the events write, before_sync, after_sync."""

    def __init__(self, rng=None, owner=None, hook=None, sync=True, tear=True):
        self.rng = rng or random.Random(0)
        self.owner, self.hook, self.honest_sync, self.tear = owner, hook, sync, tear
        self.durable = {}
        self.pending = {}
        self.stats = {"writes": 0, "syncs": 0, "crashes": 0, "torn_bytes": 0}

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


class RaftStorage:
    def __init__(self, disk):
        self.disk = disk

    # ----- writing: every call returns only after the record is synced

    def _append(self, record):
        payload = json.dumps(record, separators=(",", ":")).encode()
        self.disk.write(JOURNAL, HEADER.pack(len(payload), zlib.crc32(payload)) + payload)
        self.disk.sync(JOURNAL)

    def save_hard_state(self, term, voted_for):
        self._append({"h": [term, voted_for]})

    def save_log(self, from_index, entries):
        """Make the log hold `entries` from from_index on (anything after that point is dropped)."""
        self._append({"l": [from_index, [[e.term, e.command] for e in entries]]})

    # ----- reading

    def load(self):
        """Returns (term, voted_for, [(term, command), ...]) and cuts off any damaged tail."""
        data = self.disk.read(JOURNAL)
        term, voted_for, log = 0, None, []
        pos = 0
        while pos + HEADER.size <= len(data):
            length, crc = HEADER.unpack_from(data, pos)
            end = pos + HEADER.size + length
            payload = data[pos + HEADER.size:end]
            if end > len(data) or zlib.crc32(payload) != crc:
                break
            try:
                record = json.loads(payload)
                if "h" in record:
                    new_term, new_vote = record["h"]
                    if not isinstance(new_term, int) or new_term < term:
                        break
                    term, voted_for = new_term, new_vote
                else:
                    from_index, entries = record["l"]
                    if not isinstance(from_index, int) or not 1 <= from_index <= len(log) + 1:
                        break
                    log[from_index - 1:] = [(t, c) for t, c in entries]
            except (ValueError, KeyError, TypeError):
                break
            pos = end
        if pos < len(data):
            self.disk.truncate(JOURNAL, pos)
        return term, voted_for, log
