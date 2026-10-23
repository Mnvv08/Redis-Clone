"""Three real processes, real TCP, real files: kill the leader and watch the cluster carry on.

    python real_demo.py          (about 2 seconds; the servers' logs are printed at the end)
"""
import tempfile
import time

from localcluster import LocalCluster


def show(cluster, title):
    print(f"\n{title}")
    for node_id in cluster.addrs:
        s = cluster.status(node_id)
        if s is None:
            print(f"  node {node_id}: DOWN")
        else:
            print(f"  node {node_id}: {s['state']:<9} term {s['term']}  commit {s['commit_index']:<4} "
                  f"log {s['base']}+{s['log_entries']}  snapshots taken {s['snapshots_taken']}, installed {s['snapshots_installed']}")


directory = tempfile.mkdtemp(prefix="miniraft-")
cluster = LocalCluster(directory, snapshot_every=10)
cluster.start_all()
try:
    leader = cluster.wait_for_leader()
    client = cluster.client()
    print(f"3 server processes started (data in {directory}); node {leader} won the election")
    for i in range(30):
        client.append("ledger", f"<{i}>")
    show(cluster, "after 30 writes:")

    print(f"\nkilling the leader (node {leader}) with SIGKILL ...")
    cluster.kill(leader)
    started = time.monotonic()
    new_leader = cluster.wait_for_leader(among=list(cluster.procs))
    print(f"node {new_leader} was elected {time.monotonic() - started:.2f}s later; the client carries on:")
    for i in range(30, 70):
        client.append("ledger", f"<{i}>")
    show(cluster, "after 40 more writes while node %d is dead:" % leader)

    print(f"\nrestarting node {leader}: it recovers from its own files, then catches up from the others ...")
    cluster.start(leader)
    target = cluster.status(new_leader)["commit_index"]
    cluster.wait_until(lambda: (cluster.status(leader) or {}).get("commit_index", 0) >= target)
    show(cluster, "after the restart:")

    ledger = client.get("ledger")
    expected = "".join(f"<{i}>" for i in range(70))
    print(f"\nledger holds every one of the 70 writes exactly once, in order: {ledger == expected}")
    print(f"the client had to retry {client.retries} time(s) because of the crash, and nothing ran twice")
finally:
    cluster.stop_all()

print(f"\n--- node {leader}'s log (it was the first leader, then died and came back) ---")
print(cluster.log_text(leader).rstrip())
