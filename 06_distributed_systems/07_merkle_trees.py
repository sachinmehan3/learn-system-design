"""
LESSON 06.07 -- MERKLE TREES (ANTI-ENTROPY AND DATA VERIFICATION)
=================================================================

THE PROBLEM
-----------
Two replicas each hold 100 million keys. A few keys differ (a write was
missed during an outage). How do you find WHICH ones without shipping all
100 million keys across the network?

THE MERKLE TREE (hash tree)
---------------------------
  * Split the key range into buckets; hash each bucket's contents -> LEAVES.
  * Each parent node = hash(left child + right child), up to a single ROOT.

                       root = H(H12 + H34)
                      /                   \\
              H12 = H(H1+H2)         H34 = H(H3+H4)
               /        \\            /        \\
            H1          H2         H3          H4      <- hashes of key ranges

COMPARING TWO REPLICAS
  1. Exchange root hashes. Equal -> replicas are identical. DONE (1 message!).
  2. Different -> compare the two children; recurse ONLY into subtrees whose
     hashes differ.
  3. At the leaves, exchange just those buckets' keys.
  Cost is O(d * log n) hashes for d differences instead of O(n) keys.

WHERE IT'S USED
  * Anti-entropy repair in Dynamo, Cassandra ("nodetool repair"), Riak.
  * Git (commits/trees are Merkle DAGs -- that's why a commit hash covers
    the whole repository state), IPFS.
  * Blockchains (Bitcoin block headers hold the Merkle root of transactions;
    light clients verify a transaction with a log(n) "Merkle proof").
  * Certificate Transparency logs, ZFS/Btrfs checksums, Amazon S3/DynamoDB
    backups, rsync-like sync tools.
"""

import hashlib
import random


def H(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()[:10]


class MerkleTree:
    def __init__(self, store: dict, n_buckets=16):
        self.n_buckets = n_buckets
        buckets = [[] for _ in range(n_buckets)]
        for k in sorted(store):
            buckets[int(H(k), 16) % n_buckets].append(f"{k}={store[k]}")
        self.buckets = buckets
        # levels[0] = leaves, levels[-1] = [root]
        level = [H("|".join(b)) for b in buckets]
        self.levels = [level]
        while len(level) > 1:
            level = [H(level[i] + level[i + 1]) for i in range(0, len(level), 2)]
            self.levels.append(level)

    @property
    def root(self):
        return self.levels[-1][0]


def diff(a: MerkleTree, b: MerkleTree):
    """Walk down from the root, only into subtrees whose hashes differ."""
    comparisons = 0
    frontier = [0]                                   # node indexes at the current level
    for depth in range(len(a.levels) - 1, -1, -1):
        next_frontier = []
        for idx in frontier:
            comparisons += 1
            if a.levels[depth][idx] != b.levels[depth][idx]:
                if depth == 0:
                    next_frontier.append(idx)        # a differing leaf bucket
                else:
                    next_frontier += [2 * idx, 2 * idx + 1]
        frontier = next_frontier
    return frontier, comparisons


if __name__ == "__main__":
    rng = random.Random(4)
    replica_a = {f"key{i:06d}": rng.randint(0, 1000) for i in range(100_000)}
    replica_b = dict(replica_a)

    print("=" * 72)
    print("Two replicas with 100,000 keys each")
    print("=" * 72)
    ta, tb = MerkleTree(replica_a, 1024), MerkleTree(replica_b, 1024)
    print(f"  identical replicas: root A={ta.root} root B={tb.root} -> equal, nothing to sync")

    # Replica B missed 3 writes during an outage.
    for k in ("key000042", "key055555", "key099999"):
        replica_a[k] = -1
    ta = MerkleTree(replica_a, 1024)
    print(f"  after 3 missed writes: root A={ta.root} root B={tb.root} -> differ, descend...")
    leaves, comparisons = diff(ta, tb)
    keys_to_send = sum(len(ta.buckets[i]) for i in leaves)
    print(f"  found {len(leaves)} differing buckets after {comparisons} hash comparisons")
    print(f"  keys exchanged: {keys_to_send} (instead of all 100,000)")
    fixed = [kv for i in leaves for kv in ta.buckets[i] if kv not in tb.buckets[i]]
    print(f"  differing entries found: {fixed}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Replicas run periodic anti-entropy: exchange Merkle tree roots per key
   range, and only descend into ranges whose hashes differ, so repair traffic
   is proportional to the number of differences, not the data size."
* Same idea verifies data integrity (Git, blockchains, backups).
""")
