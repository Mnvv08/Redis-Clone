"""Log compaction and InstallSnapshot, side by side. Run: python snapshot_demo.py"""
from kv import KVCluster
from storage import JOURNAL


def run(threshold):
    c = KVCluster(3, seed=7, snapshot_threshold=threshold).start()
    c.wait_for(lambda: c.stable_leader() is not None, 3000)
    leader = c.stable_leader()
    sleeper = next(n for n in c.nodes.values() if n is not leader)
    alice = c.client("alice")
    alice.call("put", k="balance", v="0")
    sleeper.crash()                                    # this node misses everything below
    for i in range(300):
        alice.call("append", k="ledger", v="x")
    sent = {"InstallSnapshot": 0, "AppendEntries": 0}
    original = c.net.send

    def counting(src, dst, msg):
        if type(msg).__name__ in sent and dst == sleeper.id:
            sent[type(msg).__name__] += 1
        return original(src, dst, msg)
    c.net.send = counting
    sleeper.restart()
    c.wait_for(c.converged, 20000)
    same = c.stores[sleeper.id].data == c.stores[leader.id].data
    print(f"  leader: {len(leader.log):>4} log entries in memory, journal {len(leader.storage.disk.read(JOURNAL)):>6} bytes, "
          f"{leader.snapshots_taken} snapshots taken")
    print(f"  the node that slept through 301 commands was sent {sent['AppendEntries']} AppendEntries and "
          f"{sent['InstallSnapshot']} InstallSnapshot, installed {sleeper.snapshots_installed}")
    print(f"  it now holds the same data as the leader: {same} (ledger length {len(c.stores[sleeper.id].data['ledger'])})")
    return c


print("without snapshots:")
run(None)
print("with a snapshot every 25 applied entries:")
c = run(25)

print("\nand exactly-once still holds across it all:")
leader = c.stable_leader()
before = c.stores[leader.id].executed
leader.propose({"c": "alice", "s": 1, "op": "put", "k": "balance", "v": "9999"})       # a very old retry
c.run_for(300)
print(f"  an ancient retry (alice #1, put balance=9999) was ignored: balance is still "
      f"{c.stores[leader.id].data['balance']!r}, executed count {before} -> {c.stores[leader.id].executed}")
