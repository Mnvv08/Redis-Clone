"""Start, kill and restart real server processes on this machine. Used by real_demo.py and the tests."""
import os
import signal
import socket
import subprocess
import sys
import time

from client import Client, ask

HERE = os.path.dirname(os.path.abspath(__file__))


def free_ports(n):
    sockets = [socket.socket() for _ in range(n)]
    try:
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


class LocalCluster:
    def __init__(self, directory, n=3, snapshot_every=50, election_ms="150,300"):
        self.dir, self.snapshot_every, self.election_ms = directory, snapshot_every, election_ms
        self.addrs = {i + 1: ("127.0.0.1", port) for i, port in enumerate(free_ports(n))}
        self.spec = ",".join(f"{i}={h}:{p}" for i, (h, p) in self.addrs.items())
        self.procs = {}
        os.makedirs(directory, exist_ok=True)

    def start(self, node_id):
        log = open(os.path.join(self.dir, f"node{node_id}.log"), "ab")
        self.procs[node_id] = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "server.py"), "--id", str(node_id), "--cluster", self.spec,
             "--data", os.path.join(self.dir, f"data{node_id}"), "--snapshot-every", str(self.snapshot_every),
             "--election-ms", self.election_ms], stdout=log, stderr=log, cwd=HERE)

    def start_all(self):
        for node_id in self.addrs:
            self.start(node_id)

    def kill(self, node_id):
        """SIGKILL: the process gets no chance to clean up, exactly like a power cut as far as it can tell."""
        proc = self.procs.pop(node_id)
        proc.send_signal(signal.SIGKILL)
        return proc.wait()                                # -9 if it really died from the signal

    def stop_all(self):
        for proc in self.procs.values():
            proc.send_signal(signal.SIGTERM)
        for proc in self.procs.values():
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.procs.clear()

    def status(self, node_id):
        try:
            return ask(self.addrs[node_id], {"op": "status"}, 1.0)
        except (OSError, ValueError):
            return None

    def wait_for_leader(self, timeout=15.0, among=None):
        """The id of the node that every reachable node agrees is leader; waits until there is one."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = {i: self.status(i) for i in (among or self.procs)}
            leaders = [i for i, s in states.items() if s and s["state"] == "leader"]
            if len(leaders) == 1 and all(s and s["leader"] == leaders[0] for s in states.values()):
                return leaders[0]
            time.sleep(0.1)
        raise TimeoutError("no stable leader: " + str(states))

    def wait_until(self, condition, timeout=15.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return True
            time.sleep(0.1)
        return False

    def client(self, **options):
        return Client(self.addrs, **options)

    def log_text(self, node_id):
        with open(os.path.join(self.dir, f"node{node_id}.log"), errors="replace") as f:
            return f.read()
