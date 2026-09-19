"""
LESSON 06.01 -- QUORUMS (N, R, W) AND LEADERLESS REPLICATION
=============================================================

THE SETUP (Dynamo, Cassandra, Riak)
-----------------------------------
Every key is stored on N replicas. There's no leader:
  * A WRITE is sent to all N replicas; it succeeds once W of them acknowledge.
  * A READ is sent to replicas; it succeeds once R of them respond, and the
    client takes the value with the NEWEST version.

THE QUORUM CONDITION:   R + W > N
If it holds, every read set overlaps every write set in at least one replica
-- so at least one replica in any read has the latest successful write.

    N=3, W=2, R=2  -> 2+2 > 3  (classic "QUORUM" reads and writes)
    N=3, W=3, R=1  -> fast reads, but a write fails if ANY replica is down
    N=3, W=1, R=3  -> fast writes, slow/fragile reads
    N=3, W=1, R=1  -> 1+1 <= 3: fast and available but reads can be STALE

Tolerating failures: writes survive N-W dead replicas, reads survive N-R.
With N=3, W=R=2 you tolerate 1 failed replica for both.

Tuning is per query in Cassandra (consistency levels ONE, QUORUM, ALL,
LOCAL_QUORUM for multi-DC). That's PACELC in action: you choose, per
operation, latency vs consistency.

REPAIRING STALE REPLICAS
------------------------
* READ REPAIR: when a read sees replicas with old versions, it writes the
  newest value back to them.
* HINTED HANDOFF: if a target replica is down, another node temporarily
  stores the write with a "hint" and forwards it when the replica returns.
  ("SLOPPY QUORUM": W acks from ANY healthy nodes -- more available, but R+W>N
  no longer guarantees overlap.)
* ANTI-ENTROPY: background comparison of replicas using MERKLE TREES
  (lesson 07) to find and fix differences efficiently.

CAVEATS: even with R+W>N there are edge cases (concurrent writes, a write
that fails on some replicas but succeeded on fewer than W, clock-based
last-write-wins). Quorums give "strong-ish" consistency, not linearizability
by default.
"""

import random


class Replica:
    def __init__(self, name):
        self.name = name
        self.store = {}           # key -> (version, value)
        self.up = True

    def write(self, key, version, value):
        if not self.up:
            return False
        if version > self.store.get(key, (0, None))[0]:
            self.store[key] = (version, value)
        return True

    def read(self, key):
        if not self.up:
            return None
        return self.store.get(key, (0, None))


class QuorumStore:
    def __init__(self, n, r, w, rng):
        self.replicas = [Replica(f"r{i}") for i in range(n)]
        self.n, self.r, self.w = n, r, w
        self.rng = rng
        self.version = 0
        self.read_repairs = 0

    def put(self, key, value, reach=None):
        """`reach` = how many replicas the write actually gets to (network flakiness)."""
        self.version += 1
        targets = self.replicas[:] if reach is None else self.rng.sample(self.replicas, reach)
        acks = sum(r.write(key, self.version, value) for r in targets)
        return acks >= self.w

    def get(self, key, read_repair=True):
        responders = [r for r in self.rng.sample(self.replicas, self.n) if r.up][: self.r]
        if len(responders) < self.r:
            return "ERROR: not enough replicas"
        answers = [(r, r.read(key)) for r in responders]
        newest = max(a for _, a in answers)
        if read_repair:
            for r, a in answers:
                if a < newest:
                    r.write(key, *newest)
                    self.read_repairs += 1
        return newest[1]


def stale_read_rate(n, r, w, trials=3000, seed=1):
    rng = random.Random(seed)
    stale = 0
    for i in range(trials):
        s = QuorumStore(n, r, w, rng)
        s.put("x", "old")
        s.put("x", "new", reach=w)            # the new write reached only W replicas
        if s.get("x", read_repair=False) != "new":
            stale += 1
    return stale / trials


if __name__ == "__main__":
    print("=" * 72)
    print("PART 1: Stale reads vs quorum settings (N=3; the write reached exactly W replicas)")
    print("=" * 72)
    for r, w in ((1, 1), (1, 2), (2, 2), (1, 3), (3, 1)):
        cond = "R+W>N" if r + w > 3 else "R+W<=N"
        print(f"  N=3 R={r} W={w} ({cond:6s}): stale reads = {stale_read_rate(3, r, w):5.1%}")
    print("  -> Whenever R+W > N, reads always overlap the latest write. Otherwise, stale reads.")

    print()
    print("=" * 72)
    print("PART 2: Availability -- one replica down (N=3)")
    print("=" * 72)
    for r, w in ((2, 2), (1, 3), (3, 1)):
        s = QuorumStore(3, r, w, random.Random(2))
        s.replicas[0].up = False
        wrote = s.put("k", "v")
        read = s.get("k")
        print(f"  R={r} W={w}: write {'OK' if wrote else 'FAILED'}, read -> {read}")

    print()
    print("=" * 72)
    print("PART 3: Read repair heals stale replicas")
    print("=" * 72)
    s = QuorumStore(3, 2, 2, random.Random(5))
    s.put("k", "v1")
    s.put("k", "v2", reach=2)
    print("  before reads:", {r.name: r.store['k'][1] for r in s.replicas})
    for _ in range(5):
        s.get("k")
    print("  after reads :", {r.name: r.store['k'][1] for r in s.replicas},
          f"({s.read_repairs} read repair(s))")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "N=3 replicas across 3 AZs, QUORUM writes and reads (W=R=2) so R+W>N;
   we tolerate one AZ failure with no stale reads."
* Lower R/W for latency and availability where staleness is OK (e.g. W=1,
  R=1 for a view counter).
* Repair mechanisms: read repair, hinted handoff, Merkle-tree anti-entropy.
""")
