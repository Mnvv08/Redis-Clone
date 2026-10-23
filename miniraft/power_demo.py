"""Pull the plug on a whole Raft cluster, with and without fsync. Run: python power_demo.py"""
from raft import Cluster, SafetyViolation


def outage(sync):
    c = Cluster(3, seed=4, sync=sync, tear=False).start()
    c.wait_for(lambda: c.stable_leader() is not None, 3000)
    for i in range(5):
        c.propose(f"order-{i}")
        c.run_for(20)
    c.wait_for(c.converged, 3000)
    print(f"  before the outage: {sum(len(n.log) for n in c.nodes.values())} log entries across 3 nodes, "
          f"{len(c.committed)} committed")
    for n in c.nodes.values():
        n.crash()
    for n in c.nodes.values():
        n.restart()
    print(f"  after power returns: logs hold {[len(n.log) for n in c.nodes.values()]} entries")
    try:
        c.wait_for(lambda: c.stable_leader() is not None, 3000)
        for i in range(5):
            c.propose(f"new-{i}")
            c.run_for(20)
        c.run_for(500)
        print("  cluster carried on safely; committed:", [cmd for _, cmd in c.nodes[1].applied if cmd][:6], "...")
    except SafetyViolation as e:
        print("  SAFETY VIOLATION caught by the monitors:", type(e).__name__)


print("with fsync (the real thing):")
outage(True)
print("with a disk that ignores fsync:")
outage(False)
