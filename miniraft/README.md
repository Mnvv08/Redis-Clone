# miniraft

The Raft consensus algorithm, built from scratch in pure Python (standard library only), together with
the tools to check that it is actually correct: a deterministic simulator, safety monitors, crash and
power-cut injection, and a linearizability checker. The same Raft code also runs for real: separate
processes, TCP sockets, files on disk, and you can kill the leader with SIGKILL.

    python demo.py          # a 5-second tour of everything below
    python -m pytest        # 393 tests, about 45 seconds (pytest is the only dependency, dev only)

## Run a real cluster

Open three terminals in this folder and start one node in each:

    python server.py --id 1 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data data/n1
    python server.py --id 2 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data data/n2
    python server.py --id 3 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data data/n3

Then, in a fourth terminal:

    python client.py put city Jaipur
    python client.py get city
    python client.py status

Press Ctrl+C in the leader's terminal (or kill -9 it), run `python client.py status` again to see the new
leader, keep writing, then start the dead node again: it rebuilds itself from its files and catches up.

## What is in it

| File | What it does |
|---|---|
| `sim.py` | Deterministic simulator: virtual time, one seeded RNG, a network that drops, duplicates, delays and partitions, nodes that crash. A run is a pure function of its seed, so any failure replays exactly. |
| `raft.py` | The protocol: leader election, log replication, the Figure 8 commit rule, a no-op on election, snapshots and InstallSnapshot. Includes `Cluster`, which runs safety monitors on every step. |
| `storage.py` | Durable state: a CRC-framed append-only journal, atomic file replacement, snapshot and journal recovery, on top of a disk object. `SimDisk` loses unsynced data and tears writes on a power cut. |
| `kv.py` | A replicated key-value store on top of Raft with client sessions, so a retried request is executed exactly once. |
| `lin.py` | A linearizability checker (Wing and Gong search with memoisation) that explains why a history is not linearizable. |
| `realdisk.py` | `RealDisk`: the same disk interface on real files, with fsync (F_FULLFSYNC on macOS), directory syncs and atomic rename. |
| `wire.py`, `realnet.py` | Raft messages as JSON lines (validated on receipt) and a TCP network plus real clock with the simulator's interface, so `raft.py` runs unchanged. |
| `server.py`, `client.py` | A node process (Raft + KV store + one TCP port) and a client that finds the leader and retries safely. |
| `localcluster.py`, `real_demo.py` | Start, SIGKILL and restart real processes; a demo that does exactly that. |

## What is checked

The monitors run inside every simulated test and raise as soon as a property breaks:
Election Safety (one leader per term), Leader Completeness, State Machine Safety, Log Matching, and
a snapshot monitor (a snapshot must equal the replay of the committed log up to its index).

- **Chaos tests**: random crashes, restarts, partitions, drops and duplicates over many seeds.
- **Power-cut sweeps**: the power is cut at *every* disk event of a scenario (with and without torn
  writes) and the cluster must recover, including during a snapshot or an InstallSnapshot.
- **Real processes**: a client writes continuously while the leader is killed with SIGKILL; afterwards
  every acknowledged write must be present exactly once and in order. Also a full-cluster kill and
  restart, and a killed node recovering from its own disk and catching up by snapshot over TCP.
- **Negative controls**: a disk that ignores fsync, a snapshot without its session table, a rename
  without a sync, a stale read: each is planted on purpose and must be caught.
- **Mutation testing** (by hand): about 68 deliberate bugs in the snapshot code and 23 in the real
  disk, wire, network, server and client code. All were caught except 4 that change no behaviour.
- **Linearizability**: histories from chaotic runs are fed to the checker, and checked against a
  brute-force oracle on small cases.

## Design notes

- Persist before you send: a node writes its term, vote and log to disk before replying.
- Snapshot file first, journal compaction second; a crash in between leaves a snapshot ahead of the
  journal and recovery reconciles them (or refuses to start if data was lost).
- Duplicates are removed when a command is *applied*, not when it is proposed, so it works whichever
  leader gets the retry. The session table is part of the snapshot, otherwise exactly-once breaks.
- Reads go through the log, which is simple and linearizable.
- The network layer drops messages while a peer is down and keeps no queues: Raft is designed for lost
  messages, so retransmission, ordering and acknowledgement stay in the protocol where they belong.
- A node is one thread and one asyncio loop, so node code needs no locks.

## Limits (deliberately out of scope)

Single-chunk InstallSnapshot (no streaming, one message must hold the whole snapshot); snapshots only
at `last_applied` with no trailing log retained; client sessions never expire (every `client.py` call
is a new client, so the session table keeps growing); no read-index or leases; no PreVote (a node that
rejoins can force a new election); no membership changes. Real-network limits: no authentication or
encryption (trusted networks only), a slow fsync blocks the whole node, and the real-process tests run
in real time (their timeouts are generous, but they are not deterministic like the simulated ones).
Verified on Python 3.13 (Linux); the simulated suite also passed on 3.14 (macOS).

## Reading order

`sim.py` then `raft.py` (election, replication), `storage.py`, `kv.py`, `lin.py`, then `realdisk.py`,
`wire.py`, `realnet.py`, `server.py`, `client.py`, and the tests in `tests/` roughly in that same order.
The `*_demo.py` files are small, runnable stories.
