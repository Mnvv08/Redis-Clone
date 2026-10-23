"""Three tellers append to one ledger while leaders keep crashing. A retried deposit must never be
recorded twice. Run: python kv_demo.py"""
import random

from kv import KVCluster

DEPOSITS = 15
c = KVCluster(5, seed=11, drop=0.25, duplicate=0.15, latency=(1, 30)).start()
c.wait_for(lambda: c.stable_leader() is not None, 3000)
rng = random.Random(11)


def teller(name):
    client = c.client(name, retry=60)

    def next_deposit(record=None):
        if client.seq < DEPOSITS:
            c.sim.schedule(rng.uniform(1, 15), lambda: client.submit("append", k="ledger", v=f"{name}#{client.seq + 1};"))
    client.on_done = next_deposit
    next_deposit()
    return client


tellers = [teller(f"teller{i}") for i in range(3)]
crashes = 0
while not all(t.seq == DEPOSITS and t.pending is None for t in tellers):
    c.run_for(rng.uniform(5, 40))
    leader = c.stable_leader()
    if leader is not None and rng.random() < 0.3:
        leader.crash()                                   # often right after it accepted a deposit
        crashes += 1
        c.run_for(rng.uniform(100, 400))
        leader.restart()
for n in c.nodes.values():
    n.restart()
c.wait_for(c.converged, 30000)

log = c.nodes[1].log
store = c.stores[1]
entries = [x for x in store.data["ledger"].split(";") if x]
print(f"leader crashes: {crashes}, client retries: {sum(t.retries for t in tellers)}")
print(f"commands in the replicated log: {sum(1 for e in log if e.command is not None)} for {len(c.ops)} deposits")
print(f"executed: {store.executed}, retries answered from the session table: {store.replays}, stale: {store.stale}")
print(f"all 5 replicas identical: {len({repr(v) for v in c.store_states().values()}) == 1}")
print(f"ledger entries: {len(entries)}, distinct: {len(set(entries))}  (a double-count would make these differ)")
