"""A client for a real miniraft cluster.

    python client.py put x 1          python client.py get x          python client.py status
    python client.py --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 append log hello

The cluster address defaults to $MINIRAFT_CLUSTER, then to three nodes on 127.0.0.1:7001-7003.

Client.call() finds the leader, and if a request times out or the leader dies it resends the SAME
request number to whoever is leader now. The servers' session table makes the retry run at most once.
Every command line invocation is a new client with a fresh random id, so it numbers its request 1.
"""
import argparse
import json
import os
import socket
import sys
import time
import uuid

from wire import parse_cluster

DEFAULT_CLUSTER = "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"


class ClusterUnavailable(Exception):
    """No node answered with a result before the deadline."""


def ask(address, request, timeout=8.0):
    """One request over a fresh connection; returns the decoded reply (OSError / ValueError on trouble)."""
    with socket.create_connection(address, timeout=timeout) as s:
        s.sendall(json.dumps(request, separators=(",", ":")).encode() + b"\n")
        line = s.makefile("rb").readline()
    if not line:
        raise ConnectionError("the node closed the connection")
    return json.loads(line)


class Client:
    def __init__(self, addrs, client_id=None, timeout=20.0, io_timeout=8.0):
        self.addrs, self.timeout, self.io_timeout = dict(addrs), timeout, io_timeout
        self.id = client_id or f"cli-{uuid.uuid4().hex[:12]}"
        self.seq = 0
        self.ids = sorted(self.addrs)
        self.target = self.ids[0]
        self.retries = 0

    def _rotate(self):
        self.target = self.ids[(self.ids.index(self.target) + 1) % len(self.ids)]

    def call(self, op, **fields):
        """Run one command; returns {"ok": ...} or {"error": ...} (an error here is the store's own, e.g. a bad append)."""
        self.seq += 1
        command = {"c": self.id, "s": self.seq, "op": op, **fields}
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                reply = ask(self.addrs[self.target], command, self.io_timeout)
            except (OSError, ValueError):
                reply = {"error": "unreachable"}
            error = reply.get("error")
            if error not in ("unreachable", "not leader", "timeout"):
                return reply
            self.retries += 1
            hint = reply.get("leader")
            if error == "not leader" and hint in self.addrs and hint != self.target:
                self.target = hint
            else:
                self._rotate()
                time.sleep(0.1)
        raise ClusterUnavailable(f"{self.id}#{self.seq} {op}: no leader answered in {self.timeout:.0f}s")

    def put(self, key, value):
        return self.call("put", k=key, v=value)

    def get(self, key):
        return self.call("get", k=key).get("ok")

    def append(self, key, value):
        return self.call("append", k=key, v=value)

    def status(self, node_id):
        return ask(self.addrs[node_id], {"op": "status"}, 2.0)


def main(argv=None):
    p = argparse.ArgumentParser(description="Talk to a miniraft cluster.")
    p.add_argument("--cluster", default=os.environ.get("MINIRAFT_CLUSTER", DEFAULT_CLUSTER))
    p.add_argument("op", choices=["put", "get", "delete", "append", "cas", "status"])
    p.add_argument("args", nargs="*", help="put KEY VALUE | get KEY | delete KEY | append KEY VALUE | cas KEY OLD NEW")
    a = p.parse_args(argv)
    client = Client(parse_cluster(a.cluster))
    shape = {"put": ("k", "v"), "get": ("k",), "delete": ("k",), "append": ("k", "v"), "cas": ("k", "old", "new"), "status": ()}[a.op]
    if len(a.args) != len(shape):
        p.error(f"{a.op} takes {len(shape)} argument(s)")
    try:
        if a.op == "status":
            for node_id in client.ids:
                try:
                    s = client.status(node_id)
                    print(f"node {node_id}: {s['state']:<9} term {s['term']:<3} leader {s['leader']}  commit {s['commit_index']:<5} "
                          f"log {s['base']}+{s['log_entries']}  keys {s['keys']}  snapshots {s['snapshots_taken']}/{s['snapshots_installed']}")
                except (OSError, ValueError):
                    print(f"node {node_id}: not reachable")
            return 0
        reply = client.call(a.op, **dict(zip(shape, a.args)))
    except ClusterUnavailable as e:
        print(e, file=sys.stderr)
        return 1
    print(json.dumps(reply))
    return 0 if "error" not in reply else 2


if __name__ == "__main__":
    sys.exit(main())
