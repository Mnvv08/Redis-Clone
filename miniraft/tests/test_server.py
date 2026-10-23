import asyncio
import json

from client import Client, ask
from localcluster import free_ports
from server import Server


def quiet(text):
    pass


class Group:
    """Real Servers (real TCP, real files) inside one process, so a test can reach into them."""

    def __init__(self, tmp_path, n=3, snapshot_every=1000):
        self.tmp, self.snapshot_every = tmp_path, snapshot_every
        self.addrs = {i + 1: ("127.0.0.1", p) for i, p in enumerate(free_ports(n))}
        self.servers = {}

    async def start(self, node_id):
        server = Server(node_id, self.addrs, str(self.tmp / f"data{node_id}"), snapshot_every=self.snapshot_every,
                        election_timeout=(100.0, 200.0), heartbeat=25.0, request_timeout=2.0, log=quiet)
        await server.start()
        self.servers[node_id] = server
        return server

    async def start_all(self):
        for node_id in self.addrs:
            await self.start(node_id)

    async def stop(self, node_id):
        await self.servers.pop(node_id).stop()

    async def stop_all(self):
        for node_id in list(self.servers):
            await self.stop(node_id)

    def leaders(self):
        return [s for s in self.servers.values() if s.node.state == "leader"]

    async def wait_for(self, condition, timeout=10.0):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            if condition():
                return True
            await asyncio.sleep(0.02)
        return False

    async def leader(self, timeout=10.0):
        assert await self.wait_for(lambda: len(self.leaders()) == 1 and all(
            s.node.leader_id == self.leaders()[0].id for s in self.servers.values()), timeout), "no stable leader"
        return self.leaders()[0]

    def converged(self):
        servers = list(self.servers.values())
        return len({(s.node.commit_index, json.dumps(s.store.data, sort_keys=True)) for s in servers}) == 1

    def client(self, **options):
        return Client(self.addrs, **options)


async def in_thread(fn, *args, **kwargs):
    return await asyncio.get_running_loop().run_in_executor(None, lambda: fn(*args, **kwargs))


def run_group(tmp_path, body, **options):
    async def go():
        g = Group(tmp_path, **options)
        try:
            await body(g)
        finally:
            await g.stop_all()                                              # even when an assertion failed
    asyncio.run(go())


def test_a_cluster_of_one_serves_requests(tmp_path):
    async def body(g):
        await g.start_all()
        c = g.client()
        assert await in_thread(c.put, "x", "1") == {"ok": None}
        assert await in_thread(c.get, "x") == "1"
    run_group(tmp_path, body, n=1)


def test_writes_are_replicated_and_a_follower_redirects_to_the_leader(tmp_path):
    async def body(g):
        await g.start_all()
        leader = await g.leader()
        follower = next(i for i in g.addrs if i != leader.id)
        reply = await in_thread(ask, g.addrs[follower], {"c": "a", "s": 1, "op": "put", "k": "x", "v": "1"})
        assert reply == {"error": "not leader", "leader": leader.id}
        c = g.client()
        ids = sorted(g.addrs)
        c.target = ids[(ids.index(leader.id) + 1) % 3]                      # a follower whose successor in the rotation is the other follower
        assert (await in_thread(c.put, "x", "1")) == {"ok": None}
        assert c.retries == 1 and c.target == leader.id                     # the redirect hint was followed, not guessed
        for i in range(10):
            await in_thread(c.append, "log", str(i))
        assert await g.wait_for(g.converged)
        assert all(s.store.data == {"x": "1", "log": "0123456789"} for s in g.servers.values())
    run_group(tmp_path, body)


def test_a_retried_request_runs_once_and_an_old_one_is_stale(tmp_path):
    async def body(g):
        await g.start_all()
        leader = await g.leader()
        req = {"c": "alice", "s": 1, "op": "append", "k": "x", "v": "A"}
        replies = [await in_thread(ask, g.addrs[leader.id], req) for _ in range(3)]
        assert replies == [{"ok": "A"}] * 3                                  # the same answer every time, one execution
        await in_thread(ask, g.addrs[leader.id], {"c": "alice", "s": 2, "op": "append", "k": "x", "v": "B"})
        stale = await in_thread(ask, g.addrs[leader.id], req)
        assert stale == {"error": "stale request"}
        assert await g.wait_for(g.converged)
        assert all(s.store.data == {"x": "AB"} and s.store.executed == 2 for s in g.servers.values())
    run_group(tmp_path, body)


def test_bad_requests_get_errors_and_do_not_disturb_the_node(tmp_path):
    async def body(g):
        await g.start_all()
        leader = await g.leader()
        for bad in [[1], "x", {"op": "put"}, {"c": True, "s": 1, "op": "get", "k": "x"}, {"c": "a", "s": "1", "op": "get"},
                    {"c": "a", "s": 1, "op": 5}, {"c": ["a"], "s": 1, "op": "get"}]:
            assert (await in_thread(ask, g.addrs[leader.id], bad))["error"] in ("malformed command", "bad request")
        assert (await in_thread(ask, g.addrs[leader.id], {"c": "a", "s": 1, "op": "frobnicate", "k": "x"}))["error"].startswith("unknown operation")
        assert await in_thread(g.client().put, "ok", "still works") == {"ok": None}
        status = await in_thread(ask, g.addrs[leader.id], {"op": "status"})
        assert status["id"] == leader.id and status["state"] == "leader"
    run_group(tmp_path, body)


def test_the_cluster_survives_losing_its_leader_and_the_leader_comes_back_from_disk(tmp_path):
    async def body(g):
        await g.start_all()
        old = await g.leader()
        c = g.client()
        for i in range(8):
            await in_thread(c.append, "log", str(i))
        await g.stop(old.id)
        new = await g.leader()
        assert new.id != old.id
        for i in range(8, 12):
            await in_thread(c.append, "log", str(i % 10))
        back = await g.start(old.id)                                         # a brand new object: only its files survive
        assert back.node.current_term >= 1 and back.node.last_log_index >= 9
        assert await g.wait_for(g.converged)
        assert all(s.store.data == {"log": "012345678901"} for s in g.servers.values())
    run_group(tmp_path, body)


def test_a_whole_cluster_restart_loses_nothing(tmp_path):
    async def body(g):
        await g.start_all()
        c = g.client()
        for i in range(20):
            await in_thread(c.append, "log", str(i % 10))
        assert await g.wait_for(g.converged)
        await g.stop_all()
        await g.start_all()
        await g.leader()
        assert await in_thread(c.get, "log") == "01234567890123456789"
        await in_thread(c.append, "log", "!")
        assert await g.wait_for(g.converged)
        assert all(s.store.data == {"log": "01234567890123456789!"} for s in g.servers.values())
    run_group(tmp_path, body, snapshot_every=6)


def test_a_node_that_missed_a_lot_is_caught_up_by_a_snapshot_over_tcp(tmp_path):
    async def body(g):
        await g.start_all()
        leader = await g.leader()
        sleeper = next(i for i in g.addrs if i != leader.id)
        await g.stop(sleeper)
        c = g.client()
        for i in range(40):
            await in_thread(c.append, "log", str(i % 10))
        back = await g.start(sleeper)
        assert await g.wait_for(g.converged, 15)
        assert back.node.snapshots_installed >= 1 and back.node.base >= 30
        assert back.store.data == leader.store.data and back.store.sessions == leader.store.sessions
    run_group(tmp_path, body, snapshot_every=5)


def test_a_partitioned_leader_cannot_commit_and_steps_aside_when_healed(tmp_path):
    async def body(g):
        await g.start_all()
        old = await g.leader()
        c = g.client()
        await in_thread(c.put, "k", "before")
        others = [s for s in g.servers.values() if s is not old]
        old.net.blocked = {s.id for s in others}                              # cut the leader off, both ways
        for s in others:
            s.net.blocked = {old.id}
        assert await g.wait_for(lambda: any(s.node.state == "leader" for s in others), 10)
        new = next(s for s in others if s.node.state == "leader")
        assert new.node.current_term > old.node.current_term
        isolated = await in_thread(ask, g.addrs[old.id], {"c": "z", "s": 1, "op": "put", "k": "k", "v": "LOST"})
        assert isolated == {"error": "timeout"}                               # it could not reach a majority: never acknowledged
        c2 = g.client()
        c2.target = new.id
        assert await in_thread(c2.put, "k", "after") == {"ok": None}
        old.net.blocked = set()
        for s in others:
            s.net.blocked = set()
        assert await g.wait_for(lambda: g.converged() and old.node.state == "follower", 15)
        assert all(s.store.data == {"k": "after"} for s in g.servers.values())       # the unacknowledged write is gone everywhere
    run_group(tmp_path, body)
