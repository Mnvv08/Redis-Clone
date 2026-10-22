import random

import pytest

from sim import Network, Node, SimulationError, Simulator


class Recorder(Node):
    def __init__(self, node_id, sim, net):
        super().__init__(node_id, sim, net)
        self.got = []                      # (time, src, msg)
        self.events = []

    def on_message(self, src, msg):
        assert self.up and self.net.can_reach(src, self.id)      # the network must never cheat
        self.got.append((self.sim.now, src, msg))

    def on_crash(self):
        self.events.append(("crash", self.sim.now))

    def on_restart(self):
        self.events.append(("restart", self.sim.now))


def cluster(n=3, seed=0, **net_options):
    sim = Simulator(seed)
    net = Network(sim, **net_options)
    return sim, net, [Recorder(i, sim, net) for i in range(1, n + 1)]


# ---------- the clock and the event queue ----------

def test_events_run_in_time_order_and_ties_in_scheduling_order():
    sim = Simulator()
    order = []
    sim.schedule(5, lambda: order.append("c"))
    sim.schedule(1, lambda: order.append("a"))
    sim.schedule(5, lambda: order.append("d"))
    sim.schedule(1, lambda: order.append("b"))
    sim.run()
    assert order == ["a", "b", "c", "d"] and sim.now == 5


def test_clock_moves_to_each_event_and_never_backwards():
    sim = Simulator()
    seen = []
    for delay in (3, 1, 2, 2, 7):
        sim.schedule(delay, lambda: seen.append(sim.now))
    sim.run()
    assert seen == sorted(seen) == [1, 2, 2, 3, 7]


def test_run_until_stops_before_later_events_and_sets_the_clock():
    sim = Simulator()
    hits = []
    sim.schedule(5, lambda: hits.append(5))
    sim.schedule(15, lambda: hits.append(15))
    sim.run(until=10)
    assert hits == [5] and sim.now == 10
    sim.run(until=20)
    assert hits == [5, 15] and sim.now == 20


def test_event_exactly_at_until_runs():
    sim = Simulator()
    hits = []
    sim.schedule(10, lambda: hits.append(1))
    sim.run(until=10)
    assert hits == [1]


def test_callbacks_can_schedule_more_events():
    sim = Simulator()
    ticks = []

    def tick():
        ticks.append(sim.now)
        if len(ticks) < 5:
            sim.schedule(10, tick)
    sim.schedule(0, tick)
    sim.run()
    assert ticks == [0, 10, 20, 30, 40]


def test_cancelled_timers_never_run_and_do_not_count_as_pending():
    sim = Simulator()
    hits = []
    t = sim.schedule(5, lambda: hits.append("x"))
    sim.schedule(6, lambda: hits.append("y"))
    assert sim.pending() == 2
    t.cancel()
    assert sim.pending() == 1
    sim.run()
    assert hits == ["y"]


def test_scheduling_in_the_past_is_an_error():
    with pytest.raises(SimulationError, match="past"):
        Simulator().schedule(-1, lambda: None)


def test_runaway_loops_are_caught():
    sim = Simulator()

    def forever():
        sim.schedule(1, forever)
    sim.schedule(0, forever)
    with pytest.raises(SimulationError, match="re-arming"):
        sim.run(max_events=1000)


def test_step_runs_one_event_at_a_time():
    sim = Simulator()
    hits = []
    sim.schedule(1, lambda: hits.append(1))
    sim.schedule(2, lambda: hits.append(2))
    assert sim.step() and hits == [1] and sim.now == 1
    assert sim.step() and not sim.step() and hits == [1, 2]


# ---------- the network ----------

def test_messages_arrive_after_a_delay_within_the_latency_bounds():
    sim, net, (a, b, c) = cluster(latency=(2, 4))
    for i in range(200):
        net.send(1, 2, i)
    sim.run()
    assert len(b.got) == 200
    assert all(2 <= t <= 4 for t, _, _ in b.got)
    assert all(src == 1 for _, src, _ in b.got)


def test_variable_latency_reorders_messages():
    sim, net, (a, b, c) = cluster(latency=(1, 10), seed=3)
    for i in range(50):
        net.send(1, 2, i)
    sim.run()
    order = [m for _, _, m in b.got]
    assert sorted(order) == list(range(50)) and order != list(range(50))


def test_fixed_latency_keeps_order():
    sim, net, (a, b, c) = cluster(latency=(3, 3))
    for i in range(20):
        net.send(1, 2, i)
    sim.run()
    assert [m for _, _, m in b.got] == list(range(20))


def test_loss_rate_is_respected_roughly():
    sim, net, (a, b, c) = cluster(drop=0.3, seed=1)
    for i in range(4000):
        net.send(1, 2, i)
    sim.run()
    assert 0.25 < 1 - len(b.got) / 4000 < 0.35
    assert net.stats["dropped"] + net.stats["delivered"] == 4000


def test_duplication_delivers_some_messages_twice():
    sim, net, (a, b, c) = cluster(duplicate=0.5, seed=2)
    for i in range(1000):
        net.send(1, 2, i)
    sim.run()
    assert 1400 < len(b.got) < 1600
    assert net.stats["duplicated"] == len(b.got) - 1000


def test_messages_are_copied_so_nodes_cannot_share_state():
    sim, net, (a, b, c) = cluster()
    msg = {"entries": [1, 2, 3]}
    net.send(1, 2, msg)
    msg["entries"].append(99)                  # the sender changes it after sending
    sim.run()
    assert b.got[0][2] == {"entries": [1, 2, 3]}
    b.got[0][2]["entries"].clear()             # the receiver changes it too
    assert msg["entries"] == [1, 2, 3, 99]


def test_sending_to_an_unknown_node_is_an_error():
    sim, net, _ = cluster()
    with pytest.raises(SimulationError, match="no such node"):
        net.send(1, 42, "x")


def test_duplicate_node_ids_are_rejected():
    sim, net, _ = cluster()
    with pytest.raises(SimulationError, match="already exists"):
        Recorder(1, sim, net)


# ---------- partitions ----------

def test_partition_blocks_traffic_between_groups_but_not_inside_them():
    sim, net, (a, b, c, d, e) = cluster(5)
    net.partition({1, 2}, {3, 4, 5})
    for src in range(1, 6):
        for dst in range(1, 6):
            if src != dst:
                net.send(src, dst, (src, dst))
    sim.run()
    for node in (a, b):
        assert {s for _, s, _ in node.got} == ({1, 2} - {node.id})
    for node in (c, d, e):
        assert {s for _, s, _ in node.got} == ({3, 4, 5} - {node.id})


def test_heal_restores_traffic():
    sim, net, (a, b, c) = cluster()
    net.partition({1}, {2, 3})
    net.send(1, 2, "lost")
    net.heal()
    net.send(1, 2, "ok")
    sim.run()
    assert [m for _, _, m in b.got] == ["ok"]


def test_a_partition_that_starts_while_a_message_is_in_flight_swallows_it():
    sim, net, (a, b, c) = cluster(latency=(10, 10))
    net.send(1, 2, "in flight")
    sim.run(until=5)
    net.partition({1}, {2, 3})
    sim.run()
    assert b.got == [] and net.stats["blocked"] == 1


def test_nodes_left_out_of_a_partition_are_isolated():
    sim, net, (a, b, c) = cluster()
    net.partition({1, 2})
    net.send(3, 1, "x")
    net.send(1, 3, "y")
    net.send(1, 2, "z")
    sim.run()
    assert [m for _, _, m in b.got] == ["z"] and a.got == [] and c.got == []


def test_one_way_blocks_are_directional():
    sim, net, (a, b, c) = cluster()
    net.block(1, 2)
    net.send(1, 2, "to b")
    net.send(2, 1, "to a")
    sim.run()
    assert b.got == [] and [m for _, _, m in a.got] == ["to a"]
    net.unblock(1, 2)
    net.send(1, 2, "again")
    sim.run()
    assert [m for _, _, m in b.got] == ["again"]
    assert net.can_reach(1, 2)


def test_a_new_partition_replaces_the_old_one():
    sim, net, (a, b, c) = cluster()
    net.partition({1}, {2, 3})
    net.partition({1, 2}, {3})
    net.send(1, 2, "now allowed")
    sim.run()
    assert len(b.got) == 1


# ---------- crashes ----------

def test_crashed_nodes_receive_nothing_and_send_nothing():
    sim, net, (a, b, c) = cluster()
    b.crash()
    net.send(1, 2, "to the dead")
    b.send(1, "from the dead")
    sim.run()
    assert b.got == [] and a.got == []
    assert not b.up and b.events == [("crash", 0.0)]


def test_a_message_in_flight_when_the_node_crashes_is_lost():
    sim, net, (a, b, c) = cluster(latency=(10, 10))
    net.send(1, 2, "x")
    sim.run(until=5)
    b.crash()
    sim.run()
    b.restart()
    sim.run()
    assert b.got == []


def test_restart_brings_a_node_back_and_calls_the_hook():
    sim, net, (a, b, c) = cluster()
    b.crash()
    sim.run(until=50)
    b.restart()
    net.send(1, 2, "welcome back")
    sim.run()
    assert b.up and [m for _, _, m in b.got] == ["welcome back"]
    assert b.events == [("crash", 0.0), ("restart", 50)]


def test_crash_and_restart_are_idempotent():
    sim, net, (a, b, c) = cluster()
    b.restart()
    b.crash()
    b.crash()
    b.restart()
    b.restart()
    assert b.events == [("crash", 0.0), ("restart", 0.0)]


def test_timers_fire_cancel_and_replace():
    sim, net, (a, b, c) = cluster()
    hits = []
    a.set_timer("t", 10, lambda: hits.append("first"))
    a.set_timer("t", 20, lambda: hits.append("second"))      # replaces the first
    a.set_timer("u", 5, lambda: hits.append("u"))
    assert a.has_timer("t")
    a.cancel_timer("u")
    sim.run()
    assert hits == ["second"] and not a.has_timer("t") and sim.now == 20


def test_a_timer_can_rearm_itself():
    sim, net, (a, b, c) = cluster()
    beats = []

    def beat():
        beats.append(sim.now)
        a.set_timer("hb", 10, beat)
    a.set_timer("hb", 10, beat)
    sim.run(until=45)
    assert beats == [10, 20, 30, 40]


def test_crash_cancels_every_timer_and_restart_does_not_revive_them():
    sim, net, (a, b, c) = cluster()
    hits = []
    a.set_timer("x", 10, lambda: hits.append("x"))
    a.set_timer("y", 20, lambda: hits.append("y"))
    sim.run(until=5)
    a.crash()
    sim.run(until=30)
    a.restart()
    sim.run(until=100)
    assert hits == [] and not a.has_timer("x")


# ---------- determinism ----------

def run_scenario(seed):
    sim, net, nodes = cluster(4, seed=seed, latency=(1, 9), drop=0.2, duplicate=0.1, trace=True)
    rng = random.Random(99)                 # the scenario itself is fixed; only the simulator seed varies
    for step in range(300):
        sim.schedule(step * 0.7, lambda s=rng.randrange(1, 5), d=rng.randrange(1, 5), i=step: net.send(s, d, i) if s != d else None)
    sim.schedule(60, lambda: net.partition({1, 2}, {3, 4}))
    sim.schedule(110, net.heal)
    sim.schedule(130, nodes[2].crash)
    sim.schedule(170, nodes[2].restart)
    sim.run()
    return net.trace, dict(net.stats)


def test_same_seed_gives_an_identical_run():
    first = run_scenario(5)
    assert run_scenario(5) == first and len(first[0]) > 300


def test_different_seeds_give_different_runs():
    assert run_scenario(5) != run_scenario(6)


# ---------- invariants under random chaos ----------

def test_random_chaos_keeps_the_basic_guarantees():
    """Whatever the failures, a node only receives messages somebody sent, never from across a live
    partition, never while down, and time never goes backwards."""
    for seed in range(15):
        sim, net, nodes = cluster(5, seed=seed, latency=(0.5, 20), drop=0.1, duplicate=0.1, trace=True)
        rng = random.Random(seed)
        sent = set()
        for step in range(400):
            t = step * 1.5

            def act(rng_state=rng.random(), a=rng.randrange(1, 6), b=rng.randrange(1, 6), i=step):
                if rng_state < 0.6:
                    if a != b:
                        sent.add((a, b, i))
                        net.send(a, b, (a, b, i))
                elif rng_state < 0.7:
                    net.partition(*rng.sample([{1, 2, 3}, {4, 5}, {1, 4}, {2, 3, 5}], 2))
                elif rng_state < 0.8:
                    net.heal()
                elif rng_state < 0.9:
                    nodes[a - 1].crash()
                else:
                    nodes[a - 1].restart()
            sim.schedule(t, act)
        sim.run()
        times = [e[0] for e in net.trace]
        assert times == sorted(times)
        for node in nodes:
            for _, src, msg in node.got:
                assert msg[:2] == (src, node.id) and msg in sent
        delivered = sum(len(n.got) for n in nodes)
        assert delivered == net.stats["delivered"]
        assert sum(1 for e in net.trace if e[1] == "deliver") == delivered


def test_a_timer_set_before_a_crash_does_not_fire_after_a_quick_restart():
    sim, net, (a, b, c) = cluster()
    hits = []
    a.set_timer("x", 10, lambda: hits.append("x"))
    sim.run(until=5)
    a.crash()
    sim.run(until=6)
    a.restart()                                # back up before the old timer would have fired
    sim.run(until=50)
    assert hits == []
