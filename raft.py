"""Raft consensus, built step by step on top of the deterministic simulator in sim.py.

Step 2: LEADER ELECTION.  Step 3: LOG REPLICATION.  (Persistence arrives in step 4.)

Every node is always a follower, a candidate or a leader. Time is divided into numbered terms. A
follower that hears nothing from a leader for a randomly chosen timeout becomes a candidate, bumps
the term and asks everybody for a vote. A candidate that gathers votes from a majority becomes the
leader of that term and sends heartbeats to stop new elections. The rules that make this safe:

  * a node votes for at most one candidate per term, and remembers it (even across a crash);
  * a node only votes for a candidate whose log is at least as up to date as its own;
  * anyone who sees a higher term immediately adopts it and goes back to being a follower.

Together these give ELECTION SAFETY: at most one leader per term, however messages are lost,
duplicated, delayed or reordered, and however nodes crash or the network splits.

Log replication: the leader appends commands to its log and sends them to followers in
AppendEntries messages. Each message names the entry just before the new ones (prev_log_index and
prev_log_term); a follower refuses unless it has exactly that entry, and the leader then backs up
until the logs agree. An entry is COMMITTED once the leader has stored it on a majority AND it is
from the leader's own term (older entries become committed along with it, never by counting
copies). Committed entries are applied, in order, to the state machine. The properties this buys,
all checked by the tests while the simulator misbehaves:

  * Log Matching: if two logs have an entry with the same index and term, the logs are identical
    up to that index.
  * Leader Completeness: a committed entry is in the log of every later leader.
  * State Machine Safety: no two nodes ever apply different commands at the same index.

A new leader appends one empty "no-op" entry to its log so that entries from earlier terms can be
committed straight away (Raft thesis, section 6.4).

Persistent state (current_term, voted_for, log) is simply kept across crash/restart here; step 4
replaces that shortcut with explicit storage. The commit index and the applied state machine are
volatile: a restarted node learns the commit index again from its leader and re-applies its log.
"""
from dataclasses import dataclass

from sim import Network, Node, SimulationError, Simulator

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


class SafetyViolation(SimulationError):
    """A Raft safety property was broken. These must never happen."""


class ElectionSafetyViolation(SafetyViolation):
    """Two different nodes were leader in the same term."""


class StateMachineSafetyViolation(SafetyViolation):
    """Two nodes applied different commands at the same log index."""


class LeaderCompletenessViolation(SafetyViolation):
    """A leader is missing an entry that was already committed."""


class LogMatchingViolation(SafetyViolation):
    """Two logs agree on an entry's index and term but not on what came before it."""


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
    match_index: int = 0       # on success: the last index now known to match the leader's log
    conflict_index: int = 0    # on failure: where the leader should resume (a hint to back up quickly)


class RaftNode(Node):
    def __init__(self, node_id, sim, net, all_ids, election_timeout=(150.0, 300.0), heartbeat=50.0,
                 on_leader=None, on_apply=None, max_entries=50):
        super().__init__(node_id, sim, net)
        self.peers = [p for p in all_ids if p != node_id]
        self.quorum = len(all_ids) // 2 + 1
        self.election_timeout, self.heartbeat = election_timeout, heartbeat
        self.on_leader, self.on_apply, self.max_entries = on_leader, on_apply, max_entries
        # persistent state (survives a crash)
        self.current_term = 0
        self.voted_for = None
        self.log = []                        # entries; the first one has index 1
        # volatile state (rebuilt after a restart)
        self.state = FOLLOWER
        self.leader_id = None
        self.votes = set()
        self.commit_index = 0                # highest index known to be committed
        self.last_applied = 0                # highest index applied to the state machine
        self.applied = []                    # the state machine: (index, command) in order
        self.next_index = {}                 # leader only: next entry to send to each peer
        self.match_index = {}                # leader only: highest entry known stored on each peer
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
        self.commit_index = self.last_applied = 0          # volatile: relearned from the leader
        self.applied = []
        self.next_index, self.match_index = {}, {}
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
        self.next_index = {p: self.last_log_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        if self.on_leader is not None:
            self.on_leader(self)
        self.log.append(Entry(self.current_term, None))     # the no-op that lets old entries commit
        self._advance_commit()
        self._heartbeat()

    def _heartbeat(self):
        if self.state != LEADER:
            return
        for peer in self.peers:
            self._replicate(peer)
        self.set_timer("heartbeat", self.heartbeat, self._heartbeat)

    # ----- log replication: the leader's side

    def propose(self, command):
        """Append a command to the log. Only a leader can; returns its log index, or None."""
        if not self.up or self.state != LEADER:
            return None
        self.log.append(Entry(self.current_term, command))
        index = len(self.log)
        self._advance_commit()
        for peer in self.peers:
            self._replicate(peer)
        return index

    def _replicate(self, peer):
        nxt = self.next_index[peer]
        prev = nxt - 1
        prev_term = self.log[prev - 1].term if prev > 0 else 0
        entries = tuple(self.log[nxt - 1:nxt - 1 + self.max_entries])
        self.send(peer, AppendEntries(self.current_term, self.id, prev, prev_term, entries, self.commit_index))

    def _advance_commit(self):
        """Commit the highest entry of the current term that a majority stores (older ones come with it)."""
        for n in range(self.last_log_index, self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                break                                         # terms never decrease along a log
            if 1 + sum(1 for m in self.match_index.values() if m >= n) >= self.quorum:
                self.commit_index = n
                self._apply()
                return

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            self.applied.append((self.last_applied, entry.command))
            if self.on_apply is not None:
                self.on_apply(self, self.last_applied, entry)

    def _on_append_reply(self, src, msg):
        if self.state != LEADER or msg.term != self.current_term:
            return                                            # stale: from an old term or an old leadership
        if msg.success:
            self.match_index[src] = max(self.match_index[src], msg.match_index)
            self.next_index[src] = max(self.next_index[src], self.match_index[src] + 1)
            self._advance_commit()
            if self.next_index[src] <= self.last_log_index:   # more to send: do not wait for the heartbeat
                self._replicate(src)
        else:                                                 # back up, but never below what is known to match
            self.next_index[src] = max(self.match_index[src] + 1, min(self.next_index[src], msg.conflict_index))
            self._replicate(src)

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
            self._on_append_reply(src, msg)

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
        prev = msg.prev_log_index
        if prev > len(self.log):                          # we are missing entries before the new ones
            self.send(src, AppendReply(self.current_term, False, 0, len(self.log) + 1))
            return
        if prev > 0 and self.log[prev - 1].term != msg.prev_log_term:
            first = prev                                  # skip back over the whole conflicting term
            while first > 1 and self.log[first - 2].term == self.log[prev - 1].term:
                first -= 1
            self.send(src, AppendReply(self.current_term, False, 0, first))
            return
        index = prev
        for entry in msg.entries:
            index += 1
            if index <= len(self.log):
                if self.log[index - 1].term != entry.term:  # a conflict: drop it and everything after it
                    del self.log[index - 1:]
                    self.log.append(entry)
            else:
                self.log.append(entry)
        last_new = prev + len(msg.entries)
        if msg.leader_commit > self.commit_index:         # never past what this message proved we match
            self.commit_index = max(self.commit_index, min(msg.leader_commit, last_new))
            self._apply()
        self.send(src, AppendReply(self.current_term, True, last_new, 0))


class Cluster:
    """A simulated cluster plus the checks that every test wants."""

    def __init__(self, n=3, seed=0, latency=(1.0, 10.0), drop=0.0, duplicate=0.0,
                 election_timeout=(150.0, 300.0), heartbeat=50.0, max_entries=50, trace=False):
        self.sim = Simulator(seed)
        self.net = Network(self.sim, latency=latency, drop=drop, duplicate=duplicate, trace=trace)
        ids = list(range(1, n + 1))
        self.nodes = {i: RaftNode(i, self.sim, self.net, ids, election_timeout, heartbeat, self._record_leader,
                                  self._record_apply, max_entries)
                      for i in ids}
        self.leader_of_term = {}                         # term -> the only node allowed to lead it
        self.history = []                                # (time, term, leader) for every election won
        self.committed = {}                              # index -> the entry some node has applied there

    def _record_leader(self, node):
        first = self.leader_of_term.setdefault(node.current_term, node.id)
        if first != node.id:
            raise ElectionSafetyViolation(f"term {node.current_term} has two leaders: {first} and {node.id}")
        for index, entry in self.committed.items():      # Leader Completeness
            if len(node.log) < index or node.log[index - 1] != entry:
                raise LeaderCompletenessViolation(
                    f"node {node.id} became leader of term {node.current_term} without committed entry {index}: {entry}")
        self.history.append((self.sim.now, node.current_term, node.id))

    def _record_apply(self, node, index, entry):
        first = self.committed.setdefault(index, entry)  # State Machine Safety
        if first != entry:
            raise StateMachineSafetyViolation(f"index {index}: {first} was applied before, node {node.id} applied {entry}")

    def check_logs(self):
        """Log Matching: where two logs hold the same term at an index, they are identical up to it."""
        nodes = list(self.nodes.values())
        for a in range(len(nodes)):
            for b in range(a + 1, len(nodes)):
                la, lb = nodes[a].log, nodes[b].log
                same = [i for i in range(min(len(la), len(lb))) if la[i].term == lb[i].term]
                if same and la[:same[-1] + 1] != lb[:same[-1] + 1]:
                    raise LogMatchingViolation(f"nodes {nodes[a].id} and {nodes[b].id} disagree below index {same[-1] + 1}")

    def propose(self, command):
        """Hand a command to the current leader. Returns (leader, index), or None if there is no leader."""
        leader = self.stable_leader()
        if leader is None:
            return None
        return leader, leader.propose(command)

    def converged(self):
        """Every running node holds the same log and has committed all of it."""
        up = self.up_nodes()
        return bool(up) and all(n.log == up[0].log and n.commit_index == len(n.log) for n in up)

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
