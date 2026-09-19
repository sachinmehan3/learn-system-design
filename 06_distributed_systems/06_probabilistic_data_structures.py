"""
LESSON 06.06 -- PROBABILISTIC DATA STRUCTURES: BLOOM FILTER, COUNT-MIN SKETCH, HYPERLOGLOG
==========================================================================================

At scale, EXACT answers can be too expensive in memory. These structures
trade a small, controllable error for enormous memory savings. They show up
constantly in interviews ("How do you count unique visitors to 1B pages?",
"How does the crawler know if it's seen a URL?", "Top-K trending hashtags?").

1) BLOOM FILTER -- "Have I seen this item before?"
-------------------------------------------------
A bit array of m bits + k hash functions.
  add(x):   set the k bits at positions h1(x)..hk(x).
  query(x): if ANY of the k bits is 0 -> DEFINITELY NOT present.
            if all are 1              -> PROBABLY present (false positive possible).
  * NO false negatives. False positive rate p ~ (1 - e^(-kn/m))^k.
  * Optimal k = (m/n) ln 2. About 10 bits per item gives ~1% false positives
    -- regardless of item size! (1B URLs: ~1.2 GB instead of ~100 GB.)
  * Can't delete (use a Counting Bloom filter or Cuckoo filter for that).
  Uses: LSM-trees skipping SSTables (Cassandra, RocksDB, HBase), web crawlers
  (seen URL?), cache-penetration protection, Chrome's old malicious-URL
  check, Medium's "already recommended" articles, CDN "cache on second hit".

2) COUNT-MIN SKETCH -- "How many times has X occurred?" (frequency)
------------------------------------------------------------------
A d x w grid of counters, one hash function per row.
  add(x):      increment grid[i][h_i(x)] for each row i.
  estimate(x): min over rows of grid[i][h_i(x)].
  * Never UNDER-estimates; over-estimates due to collisions (bounded by
    e/w * total count, with probability 1 - e^-d).
  * Fixed memory no matter how many distinct items.
  Uses: heavy hitters / TOP-K (trending hashtags, hot keys detection, top
  searched queries), rate limiting by IP at huge scale, network monitoring.
  Top-K = count-min sketch + a small min-heap of the current top candidates.

3) HYPERLOGLOG -- "How many DISTINCT items?" (cardinality)
----------------------------------------------------------
Intuition: hash each item to a random bit string. Seeing a hash that starts
with k leading zeros is a 1-in-2^k event -- so the longest run of leading
zeros observed hints at how many distinct items you've seen. To reduce
variance, split items into m buckets (by the first bits of the hash), track
the max leading zeros per bucket, and combine with a harmonic mean.
  * ~1.04/sqrt(m) standard error. Redis's HLL uses 12 KB to count up to 2^64
    distinct items with ~0.81% error.
  * MERGEABLE: HLL(day1) U HLL(day2) = take per-bucket max. Great for
    distributed counting (count per server, merge centrally).
  Uses: unique visitors (DAU/MAU), distinct search queries, unique IPs
  (Redis PFADD/PFCOUNT, BigQuery APPROX_COUNT_DISTINCT, Presto).
"""

import hashlib
import heapq
import math
import random
from collections import Counter


def _hash(item, seed=0):
    return int.from_bytes(hashlib.blake2b(f"{seed}:{item}".encode(), digest_size=8).digest(), "big")


class BloomFilter:
    def __init__(self, expected_items, fp_rate):
        # Standard sizing formulas:
        self.m = math.ceil(-expected_items * math.log(fp_rate) / (math.log(2) ** 2))
        self.k = max(1, round(self.m / expected_items * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)

    def _positions(self, item):
        # Double hashing trick: k positions from two hash values.
        h1, h2 = _hash(item, 1), _hash(item, 2)
        return [(h1 + i * h2) % self.m for i in range(self.k)]

    def add(self, item):
        for p in self._positions(item):
            self.bits[p // 8] |= 1 << (p % 8)

    def __contains__(self, item):
        return all(self.bits[p // 8] & (1 << (p % 8)) for p in self._positions(item))


class CountMinSketch:
    def __init__(self, width, depth):
        self.w, self.d = width, depth
        self.grid = [[0] * width for _ in range(depth)]

    def add(self, item, count=1):
        for row in range(self.d):
            self.grid[row][_hash(item, row) % self.w] += count

    def estimate(self, item):
        return min(self.grid[row][_hash(item, row) % self.w] for row in range(self.d))


class HyperLogLog:
    def __init__(self, b=12):
        self.b = b                      # number of bits used to pick a bucket
        self.m = 1 << b                 # number of buckets (registers)
        self.registers = [0] * self.m
        self.alpha = 0.7213 / (1 + 1.079 / self.m)

    def add(self, item):
        x = _hash(item)                              # 64-bit hash
        bucket = x >> (64 - self.b)                  # first b bits choose the register
        rest = (x << self.b) & ((1 << 64) - 1)       # remaining bits
        rank = 1
        while rank <= 64 - self.b and not (rest & (1 << 63)):
            rank += 1                                # position of first 1-bit = leading zeros + 1
            rest <<= 1
        self.registers[bucket] = max(self.registers[bucket], rank)

    def count(self):
        estimate = self.alpha * self.m * self.m / sum(2.0 ** -r for r in self.registers)
        zeros = self.registers.count(0)
        if estimate <= 2.5 * self.m and zeros:       # small-range correction
            estimate = self.m * math.log(self.m / zeros)
        return int(estimate)

    def merge(self, other):
        self.registers = [max(a, b) for a, b in zip(self.registers, other.registers)]


if __name__ == "__main__":
    rng = random.Random(0)

    print("=" * 76)
    print("BLOOM FILTER: 100,000 crawled URLs, target 1% false positives")
    print("=" * 76)
    bf = BloomFilter(100_000, 0.01)
    seen = [f"https://site{i}.com/page" for i in range(100_000)]
    for url in seen:
        bf.add(url)
    fn = sum(1 for u in seen[:10_000] if u not in bf)
    fp = sum(1 for i in range(100_000) if f"https://other{i}.org/" in bf)
    exact_bytes = sum(len(u) for u in seen)
    print(f"  size: {bf.m:,} bits = {bf.m / 8 / 1024:.0f} KB with k={bf.k} hashes "
          f"(the raw URLs are {exact_bytes / 1024:.0f} KB)")
    print(f"  false negatives: {fn}   (never happens)")
    print(f"  false positives: {fp / 100_000:.2%} of never-seen URLs (target 1%)")

    print()
    print("=" * 76)
    print("COUNT-MIN SKETCH + HEAP: top-5 trending hashtags from 200,000 posts")
    print("=" * 76)
    tags = [f"#tag{i}" for i in range(5000)]
    weights = [1 / (r ** 1.2) for r in range(1, 5001)]
    stream = rng.choices(tags, weights=weights, k=200_000)
    cms = CountMinSketch(width=2000, depth=5)
    top_k, heap, in_heap = 5, [], set()
    for tag in stream:
        cms.add(tag)
        est = cms.estimate(tag)
        if tag in in_heap:
            heap = [(est if t == tag else c, t) for c, t in heap]
            heapq.heapify(heap)
        elif len(heap) < top_k:
            heapq.heappush(heap, (est, tag)); in_heap.add(tag)
        elif est > heap[0][0]:
            _, out = heapq.heapreplace(heap, (est, tag)); in_heap.discard(out); in_heap.add(tag)
    exact = Counter(stream)
    print(f"  sketch memory: {2000 * 5:,} counters (vs one counter per distinct tag)")
    for est, tag in sorted(heap, reverse=True):
        print(f"    {tag:9s} estimated={est:6d}  exact={exact[tag]:6d}")

    print()
    print("=" * 76)
    print("HYPERLOGLOG: counting unique visitors")
    print("=" * 76)
    for n in (1_000, 100_000, 1_000_000):
        hll = HyperLogLog(b=12)
        for i in range(n):
            hll.add(f"user-{i}")
            if i % 3 == 0:
                hll.add(f"user-{i}")              # repeat visits don't change the count
        est = hll.count()
        print(f"  true={n:9,d}  estimate={est:9,d}  error={abs(est - n) / n:5.2%}  "
              f"memory={hll.m} 6-bit registers (~{hll.m * 6 / 8 / 1024:.0f} KB)")
    monday, tuesday = HyperLogLog(), HyperLogLog()
    for i in range(50_000):
        monday.add(f"u{i}")
    for i in range(30_000, 90_000):
        tuesday.add(f"u{i}")
    monday.merge(tuesday)
    print(f"  merge(Mon, Tue) unique users: estimate={monday.count():,} (true 90,000)")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Has the crawler seen this URL?" -> Bloom filter (~10 bits/URL, 1% FP;
   a false positive just means we skip a page, acceptable).
* "Trending topics / top-K / hot keys" -> count-min sketch + min-heap, per
   time window, merged across servers.
* "Unique visitors" -> HyperLogLog per page per day (12 KB each), mergeable
   for weekly/monthly uniques.
* Always state the error trade-off and why it's acceptable for the product.
""")
