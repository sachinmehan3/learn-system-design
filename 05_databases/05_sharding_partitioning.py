"""
LESSON 05.05 -- SHARDING (PARTITIONING)
========================================

WHY SHARD?
----------
Replication gives every node ALL the data. When the data no longer fits on one
machine, or WRITES exceed what one leader can handle, you split the data into
SHARDS (partitions), each holding a subset, on different machines. Each shard
is usually replicated too.

    VERTICAL PARTITIONING: split by TABLE/feature (users DB, orders DB, chat DB).
                           Often the first step (and what microservices do).
    HORIZONTAL PARTITIONING (sharding): split ROWS of one table across nodes.

Sharding is a big step: cross-shard joins and transactions become hard or
impossible, operations get more complex. Exhaust simpler options first:
bigger machine, read replicas, caching, archiving old data.

CHOOSING THE SHARD KEY -- THE MOST IMPORTANT DECISION
-----------------------------------------------------
Good shard key: HIGH CARDINALITY, EVEN distribution of data AND traffic, and
most queries include it (so each query hits ONE shard).
    e.g. user_id for user data; chat_id for messages; tenant_id for SaaS.
Bad: country (skewed: US >> Iceland), created_date (all new writes hit one
shard), boolean flags (2 values).

STRATEGIES
----------
1) RANGE-BASED: shard 1 = keys A-F, shard 2 = G-M ...
   + Range queries are efficient (all of "2024-01" on one shard).
   - HOT SPOTS: sequential keys (timestamps, auto-increment IDs) send ALL new
     writes to the last shard. (HBase, Bigtable, MongoDB ranged sharding.)
2) HASH-BASED: shard = hash(key) mod N (or consistent hashing / hash slots).
   + Even distribution.
   - Range queries must hit every shard (scatter-gather).
   - Naive mod N remaps almost everything on resize -> use consistent hashing
     or a fixed number of virtual partitions.
3) DIRECTORY-BASED (lookup service): a table maps key -> shard.
   + Maximum flexibility (move any tenant anywhere; isolate a huge customer).
   - The directory is an extra hop and a critical dependency (cache it!).
4) GEO-BASED: EU users on EU shards (latency, data residency laws -- GDPR).

PROBLEMS SHARDING INTRODUCES
----------------------------
* HOT SHARDS / CELEBRITY PROBLEM: one key (Justin Bieber's user_id) gets
  enormous traffic -> one shard melts. Mitigate: split hot keys (add a
  random suffix, e.g. key#0..key#9, and fan-in on read), dedicated shard,
  heavy caching.
* CROSS-SHARD QUERIES: "top 10 posts globally" must SCATTER to every shard
  and GATHER/merge results -> tail latency and load grow with shard count.
  Keep common queries single-shard; precompute global views asynchronously.
* SECONDARY INDEXES: "find user by email" when sharded by user_id:
    - LOCAL index per shard (query every shard), or
    - GLOBAL index sharded by email (one lookup, but writes update 2 shards,
      usually asynchronously).
* CROSS-SHARD TRANSACTIONS: need 2PC or sagas (module 06). Design so a
  transaction touches one shard (e.g. keep a user's data together).
* RESHARDING: moving data while serving traffic. Pre-split into many small
  logical partitions (e.g. 1024) mapped to fewer physical nodes; move whole
  partitions when adding nodes. (Instagram: thousands of logical shards on
  a few Postgres servers; Vitess, Citus, and CockroachDB automate this.)
* UNIQUE IDs across shards: no single auto-increment -> Snowflake IDs
  (06_distributed_systems/09_unique_id_generation.py).
"""

import bisect
import random
import zlib
from collections import Counter


def stable_hash(s):
    return zlib.crc32(str(s).encode())


class RangeSharding:
    def __init__(self, boundaries):
        # boundaries: upper bounds (exclusive) of each shard except the last.
        self.boundaries = boundaries

    def shard_for(self, key):
        return bisect.bisect_right(self.boundaries, key)


class HashSharding:
    def __init__(self, n):
        self.n = n

    def shard_for(self, key):
        return stable_hash(key) % self.n


class LogicalPartitionSharding:
    """
    Fixed number of logical partitions mapped to physical nodes via a table.
    Adding a node = reassigning some partitions (moving whole partitions),
    never rehashing keys.
    """

    def __init__(self, n_partitions, nodes):
        self.n_partitions = n_partitions
        self.assignment = {p: nodes[p % len(nodes)] for p in range(n_partitions)}

    def partition_for(self, key):
        return stable_hash(key) % self.n_partitions

    def shard_for(self, key):
        return self.assignment[self.partition_for(key)]

    def add_node(self, node):
        nodes = sorted(set(self.assignment.values())) + [node]
        target = self.n_partitions // len(nodes)
        moved = 0
        counts = Counter(self.assignment.values())
        for p in range(self.n_partitions):          # steal partitions from overloaded nodes
            owner = self.assignment[p]
            if moved < target and counts[owner] > target:
                counts[owner] -= 1
                self.assignment[p] = node
                moved += 1
        return moved


def distribution(sharder, keys, n):
    counts = Counter(sharder.shard_for(k) for k in keys)
    return " ".join(f"s{i}={counts.get(i, 0):5d}" for i in range(n))


if __name__ == "__main__":
    rng = random.Random(3)

    print("=" * 80)
    print("PART 1: Sequential keys (auto-increment IDs / timestamps) -- the newest 10,000 writes")
    print("=" * 80)
    new_ids = range(90_000, 100_000)
    rs = RangeSharding([25_000, 50_000, 75_000])
    hs = HashSharding(4)
    print(f"  range sharding: {distribution(rs, new_ids, 4)}   <- ALL writes hit the last shard (hot spot)")
    print(f"  hash sharding : {distribution(hs, new_ids, 4)}   <- evenly spread")

    print()
    print("=" * 80)
    print("PART 2: Range query 'ids 40,000..40,100' -- how many shards must we ask?")
    print("=" * 80)
    q = range(40_000, 40_101)
    print(f"  range sharding: {len({rs.shard_for(k) for k in q})} shard(s)")
    print(f"  hash sharding : {len({hs.shard_for(k) for k in q})} shard(s)  <- scatter-gather")

    print()
    print("=" * 80)
    print("PART 3: A celebrity key (hot shard) and key splitting")
    print("=" * 80)
    traffic = [f"user{rng.randint(1, 100_000)}" for _ in range(50_000)] + ["user_celebrity"] * 50_000
    print(f"  plain key   : {distribution(hs, traffic, 4)}")
    split = [k if k != "user_celebrity" else f"user_celebrity#{rng.randint(0, 7)}" for k in traffic]
    print(f"  split into 8: {distribution(hs, split, 4)}")
    print("  (writes/reads pick a random suffix; reading the full value merges all 8 sub-keys)")

    print()
    print("=" * 80)
    print("PART 4: Resharding -- 4 nodes -> 5 nodes, 100,000 keys")
    print("=" * 80)
    keys = [f"user{i}" for i in range(100_000)]
    before = {k: stable_hash(k) % 4 for k in keys}
    after = {k: stable_hash(k) % 5 for k in keys}
    print(f"  hash mod N          : {sum(before[k] != after[k] for k in keys) / len(keys):5.1%} of keys move")
    lp = LogicalPartitionSharding(1024, ["A", "B", "C", "D"])
    before = {k: lp.shard_for(k) for k in keys}
    moved_partitions = lp.add_node("E")
    after = {k: lp.shard_for(k) for k in keys}
    print(f"  1024 logical parts. : {sum(before[k] != after[k] for k in keys) / len(keys):5.1%} of keys move "
          f"({moved_partitions} whole partitions reassigned to E)")
    print(f"  load after          : {dict(sorted(Counter(after.values()).items()))}")

    print()
    print("=" * 80)
    print("PART 5: Scatter-gather query across shards ('top 3 most-liked posts globally')")
    print("=" * 80)
    shards = [[(rng.randint(0, 10_000), f"post{s}-{i}") for i in range(1000)] for s in range(4)]
    partial = [sorted(shard, reverse=True)[:3] for shard in shards]      # each shard: local top 3
    top = sorted((p for part in partial for p in part), reverse=True)[:3]  # coordinator merges
    print(f"  each shard returns its local top 3 -> coordinator merges 12 -> {top}")
    print("  cost: every query touches EVERY shard; latency = the slowest shard.")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Shard only when needed; say what forces it ("~2 PB of messages and 100k
  writes/s -> must shard").
* Pick the shard key from the dominant query: "shard messages by chat_id so
  loading a conversation hits one shard."
* Name the problems and fixes: hot keys (split/cache), cross-shard queries
  (scatter-gather or precomputed views), global secondary indexes, resharding
  via many logical partitions or consistent hashing.
""")
