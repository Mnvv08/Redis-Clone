# miniraft

The Raft consensus algorithm, built from scratch in pure Python (standard library only), together with
the tools to check that it is actually correct: a deterministic simulator, safety monitors, crash and
power-cut injection, and a linearizability checker.

    python demo.py          # a 1-second tour of everything below
    python -m pytest        # 324 tests, about 40 seconds (pytest is the only dependency, dev only)

## What is in it

| File | What it does |
|---|---|
| `sim.py` | Deterministic simulator: virtual time, one seeded RNG, a network that drops, duplicates, delays and partitions, nodes that crash. A run is a pure function of its seed, so any failure replays exactly. |
| `raft.py` | The protocol: leader election, log replication, the Figure 8 commit rule, a no-op on election, snapshots and InstallSnapshot. Includes `Cluster`, which runs safety monitors on every step. |
| `storage.py` | Durable state: a simulated disk (data is lost unless synced, writes can tear), a CRC-framed append-only journal, atomic file replacement, snapshot and journal recovery. |
| `kv.py` | A replicated key-value store on top of Raft with client sessions, so a retried request is executed exactly once. |
| `lin.py` | A linearizability checker (Wing and Gong search with memoisation) that explains why a history is not linearizable. |

## What is checked

The monitors run inside every test and raise as soon as a property breaks:
Election Safety (one leader per term), Leader Completeness, State Machine Safety, Log Matching, and
a snapshot monitor (a snapshot must equal the replay of the committed log up to its index).

- **Chaos tests**: random crashes, restarts, partitions, drops and duplicates over many seeds.
- **Power-cut sweeps**: the power is cut at *every* disk event of a scenario (with and without torn
  writes) and the cluster must recover, including during a snapshot or an InstallSnapshot.
- **Negative controls**: a disk that ignores fsync, a snapshot without its session table, a rename
  without a sync, a stale read: each is planted on purpose and must be caught.
- **Mutation testing** (by hand): about 68 deliberate bugs in the snapshot code; all but two were
  caught, and those two do not change behaviour.
- **Linearizability**: histories from chaotic runs are fed to the checker, and checked against a
  brute-force oracle on small cases.

## Design notes

- Persist before you send: a node writes its term, vote and log to disk before replying.
- Snapshot file first, journal compaction second; a crash in between leaves a snapshot ahead of the
  journal and recovery reconciles them (or refuses to start if data was lost).
- Duplicates are removed when a command is *applied*, not when it is proposed, so it works whichever
  leader gets the retry. The session table is part of the snapshot, otherwise exactly-once breaks.
- Reads go through the log, which is simple and linearizable.

## Limits (deliberately out of scope)

Single-chunk InstallSnapshot (no streaming); snapshots only at `last_applied` with no trailing log
retained; client sessions never expire; no read-index or leases; no PreVote; no membership changes;
leader discovery is simulated; no real network or real disk (the simulated ones are the point:
they can be broken on purpose). Verified on Python 3.13 (Linux) and 3.14 (macOS).

## Reading order

`sim.py` then `raft.py` (election, replication), `storage.py`, `kv.py`, `lin.py`, and the tests in
`tests/` roughly in that same order. The `*_demo.py` files are small, runnable stories.
