"""Raft consensus, built step by step on top of the deterministic simulator in sim.py.

Step 2 (this file so far): LEADER ELECTION.

Every node is always a follower, a candidate or a leader. Time is divided into numbered terms. A
follower that hears nothing from a leader for a randomly chosen timeout becomes a candidate, bumps
the term and asks everybody for a vote. A candidate that gathers votes from a majority becomes the
leader of that term and sends heartbeats to stop new elections. The rules that make this safe:

  * a node votes for at most one candidate per term, and remembers it (even across a crash);
  * a node only votes for a candidate whose log is at least as up to date as its own;
  * anyone who sees a higher term immediately adopts it and goes back to being a follower.

Together these give ELECTION SAFETY: at most one leader per term, however messages are lost,
duplicated, delayed or reordered, and however nodes crash or the network splits.

Persistent state (current_term, voted_for, log) is simply kept across crash/restart here; step 4
replaces that shortcut with explicit storage. Log replication arrives in step 3: for now heartbeats
carry no entries and followers accept them without checking their log.
"""
from dataclasses import dataclass

from sim import Network, Node, SimulationError, Simulator

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


class ElectionSafetyViolation(SimulationError):
    """Two different nodes were leader in the same term. This must never happen."""


@dataclass(frozen=True)
class Entry:
    term: int
    command: object


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class VoteReply:
    term: int
    granted: bool


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: tuple
    leader_commit: int


@dataclass(frozen=True)
class AppendReply:
    term: int
    success: bool


class RaftNode(Node):
    def __init__(self, node_id, sim, net, all_ids, election_timeout=(150.0, 300.0), heartbeat=50.0,
                 on_leader=None):
        super().__init__(node_id, sim, net)
        self.peers = [p for p in all_ids if p != node_id]
        self.quorum = len(all_ids) // 2 + 1
        self.election_timeout, self.heartbeat = election_timeout, heartbeat
        self.on_leader = on_leader
        # persistent state (survives a crash)
        self.current_term = 0
        self.voted_for = None
        self.log = []                        # entries; the first one has index 1
        # volatile state (rebuilt after a restart)
        self.state = FOLLOWER
        self.leader_id = None
        self.votes = set()
        # bookkeeping for tests: every vote this node ever cast, as (term, candidate)
        self.vote_log = []

    # ----- small helpers

    @property
    def last_log_index(self):
        return len(self.log)

    @property
    def last_log_term(self):
        return self.log[-1].term if self.log else 0

    def start(self):
        self._reset_election_timer()

    def _reset_election_timer(self):
        self.set_timer("election", self.sim.rng.uniform(*self.election_timeout), self._on_election_timeout)

    def _cast_vote(self, candidate):
        self.voted_for = candidate
        self.vote_log.append((self.current_term, candidate))

    def _step_down(self, term):
        """Adopt a (not older) term and become a follower."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.state = FOLLOWER
        self.votes = set()
        self.cancel_timer("heartbeat")
        if not self.has_timer("election"):               # a deposed leader has no election timer yet
            self._reset_election_timer()

    # ----- crash and restart

    def on_restart(self):
        self.state = FOLLOWER
        self.leader_id = None
        self.votes = set()
        self._reset_election_timer()

    # ----- elections

    def _on_election_timeout(self):
        self.current_term += 1
        self.state = CANDIDATE
        self.leader_id = None
        self._cast_vote(self.id)
        self.votes = {self.id}
        self._reset_election_timer()                     # if this election fails, try again later
        if len(self.votes) >= self.quorum:               # a cluster of one needs nobody else
            self._become_leader()
            return
        for peer in self.peers:
            self.send(peer, RequestVote(self.current_term, self.id, self.last_log_index, self.last_log_term))

    def _become_leader(self):
        self.state = LEADER
        self.leader_id = self.id
        self.cancel_timer("election")
        if self.on_leader is not None:
            self.on_leader(self)
        self._heartbeat()

    def _heartbeat(self):
        if self.state != LEADER:
            return
        for peer in self.peers:
            self.send(peer, AppendEntries(self.current_term, self.id, self.last_log_index, self.last_log_term, (), 0))
        self.set_timer("heartbeat", self.heartbeat, self._heartbeat)

    # ----- messages

    def on_message(self, src, msg):
        if msg.term > self.current_term:                 # rule for everybody: a newer term wins
            self._step_down(msg.term)
        if isinstance(msg, RequestVote):
            self._on_request_vote(src, msg)
        elif isinstance(msg, VoteReply):
            self._on_vote_reply(src, msg)
        elif isinstance(msg, AppendEntries):
            self._on_append_entries(src, msg)
        elif isinstance(msg, AppendReply):
            pass                                         # step 3

    def _on_request_vote(self, src, msg):
        up_to_date = (msg.last_log_term, msg.last_log_index) >= (self.last_log_term, self.last_log_index)
        grant = (msg.term == self.current_term and self.voted_for in (None, msg.candidate_id) and up_to_date)
        if grant:
            self._cast_vote(msg.candidate_id)
            self._reset_election_timer()                 # we just backed somebody: give them time to win
        self.send(src, VoteReply(self.current_term, grant))

    def _on_vote_reply(self, src, msg):
        if self.state != CANDIDATE or msg.term != self.current_term or not msg.granted:
            return                                       # stale, from an old election, or a refusal
        self.votes.add(src)                              # a set: a duplicated reply counts once
        if len(self.votes) >= self.quorum:
            self._become_leader()

    def _on_append_entries(self, src, msg):
        if msg.term < self.current_term:
            self.send(src, AppendReply(self.current_term, False))
            return
        if self.state == LEADER:
            raise ElectionSafetyViolation(f"node {self.id} and node {msg.leader_id} lead term {msg.term}")
        if self.state == CANDIDATE:                      # somebody else won this term
            self._step_down(msg.term)
        self.leader_id = msg.leader_id
        self._reset_election_timer()
        self.send(src, AppendReply(self.current_term, True))


class Cluster:
    """A simulated cluster plus the checks that every test wants."""

    def __init__(self, n=3, seed=0, latency=(1.0, 10.0), drop=0.0, duplicate=0.0,
                 election_timeout=(150.0, 300.0), heartbeat=50.0, trace=False):
        self.sim = Simulator(seed)
        self.net = Network(self.sim, latency=latency, drop=drop, duplicate=duplicate, trace=trace)
        ids = list(range(1, n + 1))
        self.nodes = {i: RaftNode(i, self.sim, self.net, ids, election_timeout, heartbeat, self._record_leader)
                      for i in ids}
        self.leader_of_term = {}                         # term -> the only node allowed to lead it
        self.history = []                                # (time, term, leader) for every election won

    def _record_leader(self, node):
        first = self.leader_of_term.setdefault(node.current_term, node.id)
        if first != node.id:
            raise ElectionSafetyViolation(f"term {node.current_term} has two leaders: {first} and {node.id}")
        self.history.append((self.sim.now, node.current_term, node.id))

    def start(self):
        for node in self.nodes.values():
            node.start()
        return self

    def run_for(self, duration):
        self.sim.run(until=self.sim.now + duration)

    def wait_for(self, predicate, timeout):
        """Advance time until predicate() is true; False if `timeout` elapses first."""
        deadline = self.sim.now + timeout
        while self.sim.now <= deadline:
            if predicate():
                return True
            if not self.sim.step():
                break
        return predicate()

    def leaders(self):
        return [n for n in self.nodes.values() if n.up and n.state == LEADER]

    def up_nodes(self):
        return [n for n in self.nodes.values() if n.up]

    def stable_leader(self):
        """The one leader of the highest term among running nodes, if there is exactly one such leader."""
        top = max((n.current_term for n in self.up_nodes()), default=0)
        found = [n for n in self.leaders() if n.current_term == top]
        return found[0] if len(found) == 1 else None
