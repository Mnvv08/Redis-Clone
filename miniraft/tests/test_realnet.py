import asyncio
import json

from raft import AppendEntries, Entry, InstallSnapshot, VoteReply
from localcluster import free_ports
from realnet import RealClock, RealNetwork
from wire import encode


class StubNode:
    def __init__(self, node_id):
        self.id, self.up, self.got = node_id, True, []

    def on_message(self, src, msg):
        self.got.append((src, msg))


async def until(condition, timeout=5.0):
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if condition():
            return True
        await asyncio.sleep(0.01)
    return False


async def pair(on_client=None):
    p1, p2 = free_ports(2)
    addrs = {1: ("127.0.0.1", p1), 2: ("127.0.0.1", p2)}
    nets, nodes = {}, {}
    for i in (1, 2):
        nets[i] = RealNetwork(addrs, i, on_client=on_client, log=lambda text: None)
        nodes[i] = StubNode(i)
        nets[i].register(nodes[i])
        await nets[i].start()
    return addrs, nets, nodes


def connected(net, peer):
    return peer in net._out


async def raw(addr, *lines):
    reader, writer = await asyncio.open_connection(*addr)
    for line in lines:
        writer.write(line if isinstance(line, bytes) else json.dumps(line).encode() + b"\n")
    await writer.drain()
    return reader, writer


def test_the_clock_has_the_simulators_interface():
    async def go():
        clock = RealClock(asyncio.get_running_loop(), seed=1)
        fired = []
        clock.schedule(20, lambda: fired.append("a"))
        clock.schedule(20, lambda: fired.append("cancelled")).cancel()
        before = clock.now
        await asyncio.sleep(0.08)
        assert fired == ["a"] and clock.now - before >= 60
        assert clock.rng.uniform(1, 2) == RealClock(asyncio.get_running_loop(), seed=1).rng.uniform(1, 2)
    asyncio.run(go())


def test_messages_travel_between_two_networks():
    async def go():
        addrs, nets, nodes = await pair()
        assert await until(lambda: connected(nets[1], 2) and connected(nets[2], 1))
        msgs = [VoteReply(3, True), AppendEntries(2, 1, 0, 0, (Entry(2, "a"), Entry(2, None)), 0)]
        for m in msgs:
            nets[1].send(1, 2, m)
        assert await until(lambda: len(nodes[2].got) == 2)
        assert nodes[2].got == [(1, m) for m in msgs]
        assert nets[1].stats["sent"] == 2 and nets[2].stats["delivered"] == 2
        for n in nets.values():
            await n.close()
    asyncio.run(go())


def test_a_big_snapshot_goes_through_in_one_message():
    async def go():
        addrs, nets, nodes = await pair()
        assert await until(lambda: connected(nets[1], 2))
        big = InstallSnapshot(3, 1, 500, 3, {"data": {f"k{i}": "x" * 100 for i in range(20000)}})   # about 2 MB
        nets[1].send(1, 2, big)
        assert await until(lambda: nodes[2].got, 10)
        assert nodes[2].got[0][1] == big
        for n in nets.values():
            await n.close()
    asyncio.run(go())


def test_messages_to_a_node_that_is_down_are_dropped_and_it_is_found_again_when_it_starts():
    async def go():
        p1, p2 = free_ports(2)
        addrs = {1: ("127.0.0.1", p1), 2: ("127.0.0.1", p2)}
        a = RealNetwork(addrs, 1, log=lambda t: None)
        a.register(StubNode(1))
        await a.start()
        a.send(1, 2, VoteReply(1, True))
        assert a.stats["dropped"] == 1 and a.stats["sent"] == 0              # nobody there: no error, no queue
        b, node_b = RealNetwork(addrs, 2, log=lambda t: None), StubNode(2)
        b.register(node_b)
        await b.start()
        assert await until(lambda: connected(a, 2))
        a.send(1, 2, VoteReply(2, True))
        assert await until(lambda: node_b.got == [(1, VoteReply(2, True))])
        await a.close()
        await b.close()
    asyncio.run(go())


def test_a_peer_that_restarts_is_reconnected_to():
    async def go():
        addrs, nets, nodes = await pair()
        assert await until(lambda: connected(nets[1], 2))
        await nets[2].close()
        assert await until(lambda: not connected(nets[1], 2))
        again, node = RealNetwork(addrs, 2, log=lambda t: None), StubNode(2)
        again.register(node)
        await again.start()
        assert await until(lambda: connected(nets[1], 2))
        nets[1].send(1, 2, VoteReply(9, False))
        assert await until(lambda: node.got == [(1, VoteReply(9, False))])
        await nets[1].close()
        await again.close()
    asyncio.run(go())


def test_garbage_unknown_senders_and_bad_messages_are_ignored_and_the_connection_survives():
    async def go():
        addrs, nets, nodes = await pair()
        good = {"from": 1, "m": encode(VoteReply(5, True))}
        reader, writer = await raw(addrs[2], b"this is not json\n", b"[1, 2, 3]\n", {"from": 99, "m": encode(VoteReply(1, True))},
                                   {"from": 2, "m": encode(VoteReply(1, True))},               # claims to be the receiver itself
                                   {"from": True, "m": encode(VoteReply(1, True))},
                                   {"from": [1], "m": encode(VoteReply(1, True))},
                                   {"from": 1, "m": {"t": "VoteReply", "f": {"term": "x", "granted": True}}},
                                   {"from": 1, "m": 5}, good)
        assert await until(lambda: nodes[2].got == [(1, VoteReply(5, True))])
        assert nets[2].stats["bad"] >= 5
        writer.close()
        for n in nets.values():
            await n.close()
    asyncio.run(go())


def test_a_crashed_node_hears_nothing_and_a_blocked_peer_is_ignored_both_ways():
    async def go():
        addrs, nets, nodes = await pair()
        assert await until(lambda: connected(nets[1], 2) and connected(nets[2], 1))
        nodes[2].up = False
        nets[1].send(1, 2, VoteReply(1, True))
        await asyncio.sleep(0.1)
        assert nodes[2].got == []
        nodes[2].up = True
        nets[2].blocked.add(1)                                                    # 2 ignores 1
        nets[1].send(1, 2, VoteReply(2, True))
        nets[1].blocked.add(2)                                                    # and 1 no longer sends to 2 at all
        before = nets[1].stats["sent"]
        nets[1].send(1, 2, VoteReply(3, True))
        await asyncio.sleep(0.1)
        assert nodes[2].got == [] and nets[1].stats["sent"] == before
        for n in nets.values():
            await n.close()
    asyncio.run(go())


def test_lines_without_an_m_go_to_the_client_handler():
    async def go():
        seen = []

        async def on_client(request, writer):
            seen.append(request)
            writer.write(b'{"ok":1}\n')
            await writer.drain()
        addrs, nets, nodes = await pair(on_client)
        reader, writer = await raw(addrs[1], {"op": "ping"})
        assert json.loads(await asyncio.wait_for(reader.readline(), 3)) == {"ok": 1} and seen == [{"op": "ping"}]
        writer.write(b'{"op":"again"}\n')
        await writer.drain()
        assert json.loads(await asyncio.wait_for(reader.readline(), 3)) == {"ok": 1}      # the same connection serves many requests
        writer.close()
        for n in nets.values():
            await n.close()
    asyncio.run(go())


def test_a_bug_in_the_client_handler_does_not_kill_the_connection_loop():
    async def go():
        async def on_client(request, writer):
            if request.get("boom"):
                raise RuntimeError("bug")
            writer.write(b'{"ok":2}\n')
            await writer.drain()
        addrs, nets, nodes = await pair(on_client)
        reader, writer = await raw(addrs[1], {"boom": True}, {"fine": True})
        assert json.loads(await asyncio.wait_for(reader.readline(), 3)) == {"ok": 2}
        writer.close()
        for n in nets.values():
            await n.close()
    asyncio.run(go())


def test_close_releases_the_port():
    async def go():
        addrs, nets, nodes = await pair()
        await nets[1].close()
        again = RealNetwork(addrs, 1, log=lambda t: None)
        again.register(StubNode(1))
        await again.start()                                                       # would raise if the port were still taken
        await again.close()
        await nets[2].close()
    asyncio.run(go())
