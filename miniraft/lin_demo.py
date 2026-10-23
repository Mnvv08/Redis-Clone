"""Is the cluster really one consistent copy? Check recorded histories. Run: python lin_demo.py"""
import random

from kv import KVCluster, Workload
from lin import check


def honest_run():
    c = KVCluster(5, seed=21, drop=0.1, duplicate=0.1, latency=(1, 30)).start()
    c.wait_for(lambda: c.stable_leader() is not None, 3000)
    rng = random.Random(21)
    loads = [Workload(c, c.client(f"client{i}", retry=100), random.Random(i), 12, keys=("a", "b")) for i in range(5)]
    crashes = 0
    while not all(w.finished for w in loads):
        c.run_for(rng.uniform(10, 60))
        leader = c.stable_leader()
        if leader is not None and rng.random() < 0.2:
            leader.crash()
            crashes += 1
            c.run_for(rng.uniform(100, 400))
            leader.restart()
    return c, crashes


c, crashes = honest_run()
result = check(c.ops)
overlaps = sum(1 for x in c.ops for y in c.ops if x is not y and x["invoke"] < y["return"] and y["invoke"] < x["return"]) // 2
print(f"1. {len(c.ops)} operations from 5 clients, {crashes} leader crashes, {overlaps} pairs of overlapping operations")
print(f"   verdict: {'LINEARIZABLE' if result.ok else 'VIOLATION'} ({result.states} search states)\n")

print("2. Now a buggy server: reads are answered from a follower's memory instead of the leader.")
c = KVCluster(3, seed=1).start()
c.wait_for(lambda: c.stable_leader() is not None, 3000)
follower = next(n for n in c.nodes.values() if n is not c.stable_leader())
c.client("writer").call("put", k="balance", v="100")
c.run_for(0.001)
c.ops.append({"client": "reader", "seq": 1, "command": {"c": "reader", "s": 1, "op": "get", "k": "balance"},
              "invoke": c.sim.now, "return": c.sim.now, "result": {"ok": c.stores[follower.id].data.get("balance")}, "done": True})
print(check(c.ops).explain())
