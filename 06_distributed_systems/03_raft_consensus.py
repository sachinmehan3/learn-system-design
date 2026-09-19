"""
LESSON 06.03 -- CONSENSUS AND LEADER ELECTION WITH RAFT
========================================================

THE CONSENSUS PROBLEM
---------------------
Get several nodes to AGREE on a value (or a sequence of values -- a log),
even if some nodes crash and messages are delayed or lost. Once decided, a
value never changes.

Why you care: consensus is the foundation of
  * LEADER ELECTION (exactly one leader, even across failures),
  * DISTRIBUTED LOCKS and coordination (ZooKeeper, etcd, Consul),
  * REPLICATED STATE MACHINES: if every replica applies the same log of
    commands in the same order, they all end in the same state. That's how
    etcd (Kubernetes' brain), CockroachDB, TiDB, Spanner (Paxos), Kafka's
    KRaft, and Consul keep strongly consistent replicas.

In an interview you rarely implement consensus -- you say "use etcd/ZooKeeper"
-- but you must understand WHAT it gives you and its COST:
  * Needs a MAJORITY (quorum) of nodes: 2f+1 nodes tolerate f failures.
    3 nodes tolerate 1; 5 tolerate 2. (Even numbers add cost, not tolerance.)
  * Every write needs a round trip to a majority -> higher latency; the
    leader is a throughput bottleneck. Use it for METADATA/coordination, not
    for every user request at huge scale.
  * It's a CP system: a minority partition cannot make progress.

(FLP impossibility: in a fully asynchronous network, no deterministic
algorithm guarantees consensus if even one node may crash. Raft/Paxos stay
SAFE always and are LIVE in practice by using timeouts / randomness.)

RAFT IN A NUTSHELL (Ongaro & Ousterhout, 2014 -- designed to be understandable)
-------------------------------------------------------------------------------
Each node is a FOLLOWER, CANDIDATE, or LEADER. Time is divided into TERMS
(a monotonically increasing number; at most one leader per term).

1) LEADER ELECTION
   * Followers expect periodic heartbeats from the leader. If a follower's
     ELECTION TIMEOUT (RANDOMIZED, e.g. 150-300 ms) expires, it becomes a
     candidate: term += 1, votes for itself, asks everyone for votes.
   * A node grants at most ONE vote per term, and only to a candidate whose
     log is at least as up-to-date as its own (so a new leader always has
     every committed entry).
   * Majority of votes -> LEADER. Randomized timeouts make split votes rare.
   * Anyone who sees a HIGHER term immediately steps down to follower. This
     is what fences an old leader that comes back from a partition.

2) LOG REPLICATION
   * Clients send commands to the leader; it appends them to its log and
     sends AppendEntries to followers (these double as heartbeats).
   * AppendEntries includes the index and term of the entry PRECEDING the
     new ones; a follower rejects if its log doesn't match there, and the
     leader backs up and retries. This forces follower logs to match the
     leader's exactly (conflicting uncommitted entries are overwritten).
   * An entry is COMMITTED once stored on a MAJORITY; then it's applied to
     the state machine and the client gets a response. Committed = durable
     forever, even if the leader dies.

The simulation below runs a 5-node Raft cluster in discrete time with message
delays, elects a leader, replicates commands, crashes the leader (with an
unreplicated entry on it), elects a new leader, and brings the old leader
back to show it being brought in line.
"""

import random

N_NODES = 5
MAJORITY = N_NODES // 2 + 1
HEARTBEAT_EVERY = 3


class Cluster:
    """Network + clock. Messages take 1-2 ticks; messages to/from dead nodes are dropped."""

    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.now = 0
        self.inflight = []           # (deliver_at, dst, msg)
        self.nodes = [RaftNode(i, self) for i in range(N_NODES)]
        self.events = []

    def send(self, src, dst, msg):
        if self.nodes[src].alive:
            msg["src"] = src
            self.inflight.append((self.now + self.rng.randint(1, 2), dst, msg))

    def log(self, text):
        print(f"  t={self.now:3d}  {text}")

    def step(self):
        self.now += 1
        due = [m for m in self.inflight if m[0] <= self.now]
        self.inflight = [m for m in self.inflight if m[0] > self.now]
        for _, dst, msg in due:
            if self.nodes[dst].alive:
                self.nodes[dst].on_message(msg)
        for n in self.nodes:
            n.tick()

    def run(self, ticks):
        for _ in range(ticks):
            self.step()

    def leader(self):
        leaders = [n for n in self.nodes if n.alive and n.state == "leader"]
        return max(leaders, key=lambda n: n.term) if leaders else None


class RaftNode:
    def __init__(self, node_id, cluster):
        self.id = node_id
        self.cluster = cluster
        self.alive = True
        # Persistent state (would be on disk):
        self.term = 0
        self.voted_for = None
        self.log = []                # list of (term, command); log index i is stored at log[i-1]
        # Volatile state:
        self.state = "follower"
        self.commit_index = 0        # highest log index known to be committed
        self.votes = set()
        self.next_index = {}
        self.match_index = {}
        self.next_heartbeat = 0
        self.reset_election_timer()

    # --- helpers ------------------------------------------------------------
    def reset_election_timer(self):
        # RANDOMIZED timeout: the key trick that prevents endless split votes.
        self.election_deadline = self.cluster.now + self.cluster.rng.randint(10, 20)

    def last_log_term(self):
        return self.log[-1][0] if self.log else 0

    def peers(self):
        return [i for i in range(N_NODES) if i != self.id]

    def become_follower(self, term):
        if self.state == "leader":
            self.cluster.log(f"n{self.id} sees higher term {term} -> steps down to follower")
        self.state = "follower"
        self.term = term
        self.voted_for = None

    # --- timers ---------------------------------------------------------------
    def tick(self):
        if not self.alive:
            return
        now = self.cluster.now
        if self.state == "leader":
            if now >= self.next_heartbeat:
                self.broadcast_append_entries()
        elif now >= self.election_deadline:
            self.start_election()

    def start_election(self):
        self.state = "candidate"
        self.term += 1
        self.voted_for = self.id
        self.votes = {self.id}
        self.reset_election_timer()
        self.cluster.log(f"n{self.id} election timeout -> CANDIDATE for term {self.term}")
        for p in self.peers():
            self.cluster.send(self.id, p, {"type": "RequestVote", "term": self.term,
                                           "last_log_index": len(self.log),
                                           "last_log_term": self.last_log_term()})

    def become_leader(self):
        self.state = "leader"
        self.next_index = {p: len(self.log) for p in self.peers()}   # 0-based: next entry to send
        self.match_index = {p: 0 for p in self.peers()}
        self.cluster.log(f"n{self.id} wins {len(self.votes)} votes -> LEADER for term {self.term}")
        self.broadcast_append_entries()

    def broadcast_append_entries(self):
        self.next_heartbeat = self.cluster.now + HEARTBEAT_EVERY
        for p in self.peers():
            prev_index = self.next_index[p]
            prev_term = self.log[prev_index - 1][0] if prev_index > 0 else 0
            self.cluster.send(self.id, p, {
                "type": "AppendEntries", "term": self.term,
                "prev_index": prev_index, "prev_term": prev_term,
                "entries": self.log[prev_index:],        # empty list = pure heartbeat
                "leader_commit": self.commit_index})

    # --- client API -----------------------------------------------------------
    def client_submit(self, command):
        assert self.state == "leader"
        self.log.append((self.term, command))
        self.cluster.log(f"client -> leader n{self.id}: append {command!r} at index {len(self.log)}")

    # --- message handling -----------------------------------------------------
    def on_message(self, msg):
        if msg["term"] > self.term:
            self.become_follower(msg["term"])
        handler = getattr(self, "on_" + msg["type"])
        handler(msg)

    def on_RequestVote(self, msg):
        # Grant only if: same term, haven't voted for someone else, and the
        # candidate's log is at least as up-to-date as ours (term first, then length).
        up_to_date = (msg["last_log_term"], msg["last_log_index"]) >= (self.last_log_term(), len(self.log))
        grant = (msg["term"] == self.term and self.voted_for in (None, msg["src"]) and up_to_date)
        if grant:
            self.voted_for = msg["src"]
            self.reset_election_timer()
        self.cluster.send(self.id, msg["src"], {"type": "VoteReply", "term": self.term, "granted": grant})

    def on_VoteReply(self, msg):
        if self.state == "candidate" and msg["term"] == self.term and msg["granted"]:
            self.votes.add(msg["src"])
            if len(self.votes) >= MAJORITY:
                self.become_leader()

    def on_AppendEntries(self, msg):
        if msg["term"] < self.term:          # stale leader: reject, telling it our newer term
            self.cluster.send(self.id, msg["src"], {"type": "AppendReply", "term": self.term,
                                                    "success": False, "match": 0})
            return
        self.state = "follower"              # a valid leader exists for this term
        self.reset_election_timer()

        prev = msg["prev_index"]
        # Consistency check: do we have the entry the leader thinks precedes the new ones?
        if prev > len(self.log) or (prev > 0 and self.log[prev - 1][0] != msg["prev_term"]):
            self.cluster.send(self.id, msg["src"], {"type": "AppendReply", "term": self.term,
                                                    "success": False, "match": 0})
            return
        idx = prev
        for entry in msg["entries"]:
            if idx < len(self.log) and self.log[idx][0] != entry[0]:
                dropped = self.log[idx:]
                del self.log[idx:]            # conflict: drop our divergent (uncommitted) suffix
                self.cluster.log(f"n{self.id} discards conflicting uncommitted entries {dropped}")
            if idx >= len(self.log):
                self.log.append(entry)
            idx += 1
        match = prev + len(msg["entries"])
        self.commit_index = max(self.commit_index, min(msg["leader_commit"], match))
        self.cluster.send(self.id, msg["src"], {"type": "AppendReply", "term": self.term,
                                                "success": True, "match": match})

    def on_AppendReply(self, msg):
        if self.state != "leader" or msg["term"] != self.term:
            return
        p = msg["src"]
        if msg["success"]:
            self.match_index[p] = max(self.match_index[p], msg["match"])
            self.next_index[p] = self.match_index[p]
            self.advance_commit()
        else:
            self.next_index[p] = max(0, self.next_index[p] - 1)   # back up and retry

    def advance_commit(self):
        # Commit the highest index replicated on a majority -- but only entries
        # from the CURRENT term (a Raft safety rule; older ones commit with them).
        for n in range(len(self.log), self.commit_index, -1):
            replicas = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if replicas >= MAJORITY and self.log[n - 1][0] == self.term:
                newly = self.log[self.commit_index:n]
                self.commit_index = n
                self.cluster.log(f"leader n{self.id}: COMMITTED {[c for _, c in newly]} "
                                 f"(stored on {replicas}/{N_NODES} nodes)")
                break


def show_logs(cluster):
    for n in cluster.nodes:
        status = "DEAD" if not n.alive else n.state
        cmds = [c for _, c in n.log]
        print(f"      n{n.id} [{status:9s} term={n.term}] commit={n.commit_index} log={cmds}")


if __name__ == "__main__":
    c = Cluster(seed=7)

    print("=" * 80)
    print("PHASE 1: Leader election from a cold start")
    print("=" * 80)
    c.run(25)
    leader = c.leader()

    print()
    print("=" * 80)
    print("PHASE 2: Log replication")
    print("=" * 80)
    for cmd in ("SET x=1", "SET y=2", "SET x=3"):
        leader.client_submit(cmd)
    c.run(8)
    show_logs(c)

    print()
    print("=" * 80)
    print("PHASE 3: Leader appends an entry, then CRASHES before replicating it")
    print("=" * 80)
    leader.client_submit("SET z=99 (never replicated)")
    leader.alive = False
    c.log(f"n{leader.id} CRASHES")
    c.run(30)
    new_leader = c.leader()
    new_leader.client_submit("SET y=7")
    c.run(8)
    show_logs(c)

    print()
    print("=" * 80)
    print("PHASE 4: The old leader restarts (as a follower, keeping its persisted log)")
    print("=" * 80)
    old = leader
    old.alive = True
    old.state = "follower"
    old.reset_election_timer()
    c.log(f"n{old.id} RESTARTS with stale term {old.term}")
    c.run(12)
    show_logs(c)
    committed = {tuple(n.log[:n.commit_index]) for n in c.nodes}
    print(f"\n  All nodes have identical committed logs: {len(committed) == 1}")
    print("  The uncommitted 'SET z=99' was discarded -- it was never acknowledged to the client,")
    print("  so no promise was broken. Committed entries were never lost.")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Leader election / locks / cluster membership: use a consensus service --
   etcd or ZooKeeper -- rather than rolling our own."
* Majority quorums: 3 nodes tolerate 1 failure, 5 tolerate 2. Minority side of
  a partition can't elect a leader or commit -> no split brain (CP).
* Terms/epochs fence stale leaders (the same idea as fencing tokens for locks).
* Consensus costs a majority round trip per write -- fine for metadata and
  coordination, usually too expensive for every user action at huge scale.
""")
