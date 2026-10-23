"""Real server processes, killed with SIGKILL. Slower than the rest (a few seconds each) because it is real time."""
import threading
import time

import pytest

from localcluster import LocalCluster


@pytest.fixture
def cluster(tmp_path):
    c = LocalCluster(str(tmp_path), snapshot_every=10)
    c.start_all()
    yield c
    c.stop_all()


def test_a_leader_is_elected_and_serves_requests(cluster):
    leader = cluster.wait_for_leader()
    client = cluster.client()
    assert client.put("x", "1") == {"ok": None} and client.get("x") == "1"
    assert cluster.wait_until(lambda: all((cluster.status(i) or {}).get("keys") == 1 for i in cluster.addrs))


def test_killing_the_leader_with_sigkill_loses_no_acknowledged_write_and_runs_nothing_twice(cluster):
    leader = cluster.wait_for_leader()
    client = cluster.client()
    acked, failure = [], []

    def writer():
        try:
            for i in range(60):
                reply = client.append("log", f"<{i}>")
                assert "ok" in reply, reply
                acked.append(i)
        except Exception as e:                                               # pragma: no cover
            failure.append(e)
    thread = threading.Thread(target=writer)
    thread.start()
    assert cluster.wait_until(lambda: len(acked) >= 15)
    retries_before = client.retries
    assert cluster.kill(leader) == -9                                        # in the middle of the stream, and really by SIGKILL
    thread.join(60)
    assert not thread.is_alive() and not failure, failure
    assert acked == list(range(60))
    value = cluster.client().get("log")
    assert value == "".join(f"<{i}>" for i in range(60))                    # every write once, in order, none duplicated
    assert client.retries > retries_before                                  # the kill really did force retries


def test_a_killed_node_restarts_from_its_own_disk_and_catches_up(cluster):
    leader = cluster.wait_for_leader()
    client = cluster.client()
    follower = next(i for i in cluster.addrs if i != leader)
    for i in range(25):
        client.append("log", str(i % 10))
    cluster.kill(follower)
    for i in range(40):
        client.append("log", "x")
    cluster.start(follower)
    assert cluster.wait_until(lambda: (cluster.status(follower) or {}).get("commit_index", 0) >= 66)
    status = cluster.status(follower)
    assert status["snapshots_installed"] >= 1 or status["base"] >= 20       # it used a snapshot, its own or the leader's
    assert status["keys"] == 1
    cluster.wait_for_leader()                                                # (without PreVote a rejoining node may force a new election; any leader will do)


def test_killing_every_node_and_starting_them_again_keeps_the_data(cluster):
    cluster.wait_for_leader()
    client = cluster.client()
    for i in range(23):
        client.put(f"k{i}", str(i))
    for node_id in list(cluster.procs):
        cluster.kill(node_id)
    cluster.start_all()
    cluster.wait_for_leader()
    fresh = cluster.client()
    assert [fresh.get(f"k{i}") for i in range(23)] == [str(i) for i in range(23)]
    assert fresh.put("after", "restart") == {"ok": None}
