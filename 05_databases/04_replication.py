"""
LESSON 05.04 -- DATABASE REPLICATION
=====================================

REPLICATION = keeping copies of the same data on multiple machines.
Why:
  * HIGH AVAILABILITY: if one node dies, another has the data.
  * READ SCALING: spread reads over many replicas.
  * LATENCY: put replicas near users (other regions).
  * DISASTER RECOVERY / backups / analytics replicas.
(Replication copies ALL data to each node. It does NOT help if the data is
too big for one machine or writes are too many for one machine -- that's
SHARDING, next lesson. Real systems use both.)

THREE ARCHITECTURES
-------------------
1) SINGLE-LEADER (primary/replica, master/slave) -- MySQL, Postgres, MongoDB
   * All WRITES go to the leader; it streams its change log to FOLLOWERS.
   * Reads can go to followers (read scaling) -- but they may be stale
     (replication lag -> see 01_fundamentals/06_consistency_models.py).
   * Simple, no write conflicts. Write throughput limited to one node.

2) MULTI-LEADER -- multiple nodes accept writes (e.g. one leader per datacenter)
   * Writes are fast locally in every region; survives a region outage.
   * CONFLICTS: two leaders accept different writes to the same key. Must be
     resolved: last-write-wins (LWW -- loses data!), merge functions, CRDTs,
     or application-level resolution. Also used by offline-first apps
     (each phone is a "leader") and collaborative editing.

3) LEADERLESS (Dynamo-style) -- Cassandra, DynamoDB, Riak
   * Client (or coordinator) writes to N replicas directly and waits for W
     acks; reads from R replicas and takes the newest. If R + W > N, read and
     write sets overlap, so a read sees the latest write (QUORUM -- see
     06_distributed_systems/01_quorum.py).
   * Repair: READ REPAIR (fix stale replicas seen during reads) and
     ANTI-ENTROPY (background Merkle-tree comparison).

SYNCHRONOUS vs ASYNCHRONOUS REPLICATION
---------------------------------------
* SYNCHRONOUS: leader waits for follower(s) to confirm before acknowledging
  the client. + No data loss on leader failure. - Slower; a slow/dead
  follower blocks writes.
* ASYNCHRONOUS: leader acknowledges immediately, ships changes later.
  + Fast; followers can't block writes. - If the leader dies, writes not yet
  replicated are LOST. (Most common default!)
* SEMI-SYNCHRONOUS: wait for ONE follower synchronously, the rest async.
  Guarantees at least 2 copies of every acknowledged write. A common
  production compromise.

FAILOVER (when the leader dies)
-------------------------------
  1. Detect failure (usually a heartbeat timeout -- too short = false alarms,
     too long = long outage).
  2. Choose a new leader (the most up-to-date follower; often via consensus:
     ZooKeeper/etcd/Raft).
  3. Reconfigure clients and other followers to use the new leader.
Dangers:
  * Lost writes (async replication).
  * SPLIT BRAIN: the old leader wasn't dead, just slow/partitioned, and both
    nodes accept writes. Prevented by FENCING (the old leader's writes are
    rejected using a monotonically increasing epoch/term number or
    "STONITH": shoot the other node in the head).

HOW CHANGES ARE SHIPPED
  * Statement-based (replay SQL -- breaks with NOW(), RAND()),
  * WAL shipping (physical bytes -- tied to storage version),
  * Logical/row-based log (MySQL binlog, Postgres logical replication) --
    also the basis for CHANGE DATA CAPTURE (CDC) into caches/search/Kafka.
"""

import random


class Replica:
    def __init__(self, name):
        self.name = name
        self.log = []                 # applied writes, in order
        self.alive = True

    def apply(self, entry):
        self.log.append(entry)


class ReplicatedDB:
    """
    Single-leader replication.
      mode="async": ack after leader writes; followers catch up later.
      mode="sync" : ack after ALL followers write.
      mode="semi" : ack after leader + ONE follower write.
    """

    def __init__(self, mode, n_followers=2, seed=0):
        self.mode = mode
        self.leader = Replica("leader")
        self.followers = [Replica(f"follower{i + 1}") for i in range(n_followers)]
        self.pending = {f.name: [] for f in self.followers}   # async backlog
        self.rng = random.Random(seed)
        self.latency_ms = 0.0
        self.epoch = 1               # leadership term, used for fencing

    def write(self, entry):
        self.leader.apply(entry)
        latency = 1.0                                       # local write
        follower_rtts = sorted(self.rng.uniform(1, 20) for _ in self.followers)
        if self.mode == "sync":
            latency += follower_rtts[-1]                    # wait for the SLOWEST follower
            for f in self.followers:
                f.apply(entry)
        elif self.mode == "semi":
            latency += follower_rtts[0]                     # wait for the FASTEST follower
            self.followers[0].apply(entry)
            for f in self.followers[1:]:
                self.pending[f.name].append(entry)
        else:
            for f in self.followers:
                self.pending[f.name].append(entry)
        self.latency_ms += latency

    def replicate_some(self):
        """Background replication: each follower catches up a little."""
        for f in self.followers:
            n = self.rng.randint(0, 3)
            batch, self.pending[f.name] = self.pending[f.name][:n], self.pending[f.name][n:]
            for e in batch:
                f.apply(e)

    def fail_leader_and_promote(self):
        """Leader crashes. Promote the most up-to-date follower."""
        self.leader.alive = False
        new_leader = max(self.followers, key=lambda f: len(f.log))
        lost = [e for e in self.leader.log if e not in new_leader.log]
        self.epoch += 1
        return new_leader, lost


def run(mode):
    db = ReplicatedDB(mode, seed=42)
    for i in range(1, 21):
        db.write(f"order#{i}")
        if i % 2 == 0:
            db.replicate_some()
    new_leader, lost = db.fail_leader_and_promote()
    print(f"  {mode:5s}: avg write latency {db.latency_ms / 20:5.1f} ms | leader dies -> promote "
          f"{new_leader.name} (has {len(new_leader.log)}/20) | LOST acknowledged writes: "
          f"{lost if lost else 'none'}")


def split_brain_demo():
    print("\n  Old leader (epoch 1) was only partitioned, not dead. New leader has epoch 2.")
    storage_epoch = 2   # storage/lock service has seen the new epoch
    for who, epoch in (("old leader", 1), ("new leader", 2)):
        ok = epoch >= storage_epoch
        print(f"    {who} writes with epoch={epoch}: {'ACCEPTED' if ok else 'REJECTED (fenced)'}")


if __name__ == "__main__":
    print("=" * 96)
    print("Leader failure under different replication modes (20 writes, then the leader crashes)")
    print("=" * 96)
    for mode in ("async", "semi", "sync"):
        run(mode)
    print("\n  -> async: fastest, but acknowledged writes vanish on failover.")
    print("     sync: no loss, but every write waits for the slowest follower.")
    print("     semi-sync: the usual compromise.")

    print()
    print("=" * 96)
    print("Split brain and fencing tokens")
    print("=" * 96)
    split_brain_demo()

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Primary with two read replicas across AZs; semi-synchronous replication so
   an acknowledged write exists on 2 nodes; automated failover via a
   consensus-backed coordinator; fencing with epoch numbers to prevent split brain."
* Reads from replicas are stale by the replication lag: route read-your-writes
  traffic to the primary.
* Multi-leader / leaderless for multi-region writes, at the cost of conflict
  resolution (LWW, CRDTs) -- mention the trade-off explicitly.
* Replication != sharding: replication scales reads and availability, sharding
  scales writes and data size.
""")
