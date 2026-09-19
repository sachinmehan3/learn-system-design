"""
LESSON 01.06 -- CONSISTENCY MODELS
==================================

When data is replicated, different replicas may briefly disagree. A
CONSISTENCY MODEL is a contract: "here's what a reader is guaranteed to see."
From strongest (easiest to reason about, slowest) to weakest (fastest):

1. LINEARIZABILITY (strong consistency)
   The system behaves as if there's ONE copy of the data and every operation
   happens atomically at some instant between its start and end. Once a write
   completes, ALL subsequent reads (by anyone) see it.
   Cost: coordination on every op (consensus / quorum / single leader).
   Needed for: locks, leader election, unique usernames, account balances.

2. SEQUENTIAL CONSISTENCY
   All clients see operations in the same order, and each client's own ops
   appear in program order -- but that order needn't match real time.

3. CAUSAL CONSISTENCY
   If operation A could have caused B (e.g., B is a reply to comment A), then
   everyone sees A before B. Concurrent (unrelated) ops may be seen in
   different orders by different clients. Implemented with vector clocks /
   dependency tracking (see module 06). A sweet spot: strong enough to avoid
   the most confusing anomalies, still available under partitions.

4. SESSION GUARANTEES (per-client, practical and very common in interviews):
   * READ-YOUR-WRITES: after I update my profile, *I* always see the update
     (others may not yet). Fix: route a user's reads to the leader for a
     short while after they write, or track the write's version/timestamp
     and only read from replicas that have caught up to it.
   * MONOTONIC READS: I never see time go backwards (see a new comment, refresh,
     and it's gone because I hit a laggier replica). Fix: pin a user to one
     replica (e.g. hash(user_id) -> replica).
   * MONOTONIC WRITES: my writes are applied in the order I made them.
   * CONSISTENT PREFIX READS: if writes happened in order A,B,C, nobody sees
     C without B. Matters with sharded data where partitions replicate independently.

5. EVENTUAL CONSISTENCY
   If writes stop, all replicas EVENTUALLY converge to the same value. No
   promise about what you read in the meantime. Very available, very fast.
   DNS, Cassandra (at low consistency levels), CDN caches, most caches.

WHERE DOES INCONSISTENCY COME FROM? REPLICATION LAG.
---------------------------------------------------
In leader-follower replication (module 05), the leader applies a write and
ships it to followers asynchronously. Until a follower applies it, reads from
that follower are stale. Lag is usually milliseconds, but can be seconds or
minutes under load or failure.

The simulation below creates a leader with lagging followers and demonstrates
read-your-writes and monotonic-reads violations -- and the standard fixes.
"""

import random


class Replica:
    def __init__(self, name, lag_ticks):
        self.name = name
        self.lag_ticks = lag_ticks   # how many ticks behind the leader it applies writes
        self.value = None
        self.applied_version = 0


class LeaderFollowerDB:
    """Leader takes writes; followers apply each write `lag_ticks` later."""

    def __init__(self, follower_lags):
        self.clock = 0
        self.leader = Replica("leader", 0)
        self.followers = [Replica(f"follower{i + 1}", lag) for i, lag in enumerate(follower_lags)]
        self.log = []  # (tick_written, version, value) -- the replication log

    def tick(self):
        """Advance time; each follower applies log entries old enough for its lag."""
        self.clock += 1
        for f in self.followers:
            for t, version, value in self.log:
                if self.clock - t >= f.lag_ticks and version > f.applied_version:
                    f.value, f.applied_version = value, version

    def write(self, value):
        version = len(self.log) + 1
        self.log.append((self.clock, version, value))
        self.leader.value, self.leader.applied_version = value, version
        return version

    def read_from(self, replica):
        return replica.value, replica.applied_version


def demo_read_your_writes(rng):
    print("\n--- READ-YOUR-WRITES ---")
    db = LeaderFollowerDB(follower_lags=[1, 5])
    db.write("bio: 'hello'")
    for _ in range(10):
        db.tick()

    my_version = db.write("bio: 'I love system design'")
    print(f"  User updates bio (version {my_version}), then immediately reloads the page.")

    # NAIVE: load balancer sends the read to a random follower.
    replica = db.followers[1]
    value, _ = db.read_from(replica)
    print(f"  naive read from {replica.name}: {value}   <- user thinks the update was LOST!")

    # FIX 1: read from leader if the user wrote recently.
    value, _ = db.read_from(db.leader)
    print(f"  fix #1 (read own data from leader):  {value}")

    # FIX 2: client remembers the version it wrote; only accept replicas that
    # have caught up to it, else fall back to the leader.
    db.tick()   # a moment passes: follower1 (lag 1) catches up, follower2 (lag 5) doesn't
    for r in db.followers:
        v, applied = db.read_from(r)
        ok = applied >= my_version
        print(f"  fix #2 check {r.name}: applied_version={applied} >= {my_version}? "
              f"{'use it -> ' + str(v) if ok else 'skip (too stale)'}")
    print("  (if no replica had caught up, we'd fall back to the leader)")


def demo_monotonic_reads(rng):
    print("\n--- MONOTONIC READS ---")
    db = LeaderFollowerDB(follower_lags=[1, 6])
    db.write("0 comments")
    for _ in range(10):
        db.tick()
    db.write("1 comment: 'nice post!'")
    db.tick()
    db.tick()   # follower1 has it now, follower2 does not

    print("  User refreshes twice; each request goes to a random replica:")
    for i, r in enumerate([db.followers[0], db.followers[1]], 1):
        print(f"    refresh {i} -> {r.name}: {db.read_from(r)[0]}")
    print("  -> The comment appeared, then VANISHED. Time went backwards.")

    user_id = 12345
    pinned = db.followers[hash(user_id) % len(db.followers)]
    print(f"  FIX: pin user to one replica via hash(user_id) -> always {pinned.name}:")
    for i in (1, 2, 3):
        print(f"    refresh {i} -> {pinned.name}: {db.read_from(pinned)[0]}")
        for _ in range(3):
            db.tick()
    print("  -> The pinned replica may be a bit stale, but it only ever moves FORWARD.")


def demo_eventual_consistency():
    print("\n--- EVENTUAL CONSISTENCY: replicas converge once writes stop ---")
    db = LeaderFollowerDB(follower_lags=[1, 3, 6])
    db.write("v1")
    for t in range(8):
        states = ", ".join(f"{r.name}={r.value}" for r in db.followers)
        print(f"  tick {t}: {states}")
        db.tick()


if __name__ == "__main__":
    rng = random.Random(3)
    demo_read_your_writes(rng)
    demo_monotonic_reads(rng)
    demo_eventual_consistency()

    print("""
INTERVIEW TALKING POINTS
------------------------
* Don't just say "eventually consistent" -- say WHICH anomalies matter for this
  feature and how you prevent them (read-your-writes for profile edits,
  monotonic reads for comment threads).
* Strong consistency where correctness demands it (money, inventory, unique
  constraints); eventual consistency elsewhere for latency and availability.
* Common fixes: read-from-leader after write, version tokens, sticky replicas.
""")
