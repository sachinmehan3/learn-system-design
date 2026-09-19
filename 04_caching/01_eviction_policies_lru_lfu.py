"""
LESSON 04.01 -- CACHING BASICS AND EVICTION POLICIES (LRU, LFU, FIFO)
======================================================================

WHY CACHE?
----------
A cache stores the results of expensive operations (DB queries, API calls,
rendered pages) in fast storage (RAM) so repeat requests are cheap.
    RAM read: ~100 ns.   DB query over network: ~1-10 ms.   That's 10,000x+.
Caching works because real access patterns are SKEWED: a small fraction of
items gets most of the traffic (Zipf / power-law distribution; the "80/20
rule"). Cache that hot fraction and most requests never touch the database.

KEY METRIC: HIT RATE = hits / (hits + misses)
    Effective latency = hit_rate * cache_latency + (1 - hit_rate) * db_latency
    Going from 90% -> 99% hit rate cuts DB load by 10x!

WHERE CACHES LIVE (from the user inward)
----------------------------------------
    Browser cache -> CDN edge -> load balancer / reverse proxy cache ->
    application in-process cache -> distributed cache (Redis/Memcached) ->
    database's own buffer pool -> OS page cache -> disk controller cache
Every layer: "don't do work you've already done".

IN-PROCESS vs DISTRIBUTED CACHE
  * In-process (a dict in your app): fastest (no network), but each server
    has its own copy (duplicated memory, inconsistent between servers), lost on restart.
  * Distributed (Redis, Memcached): shared by all app servers, survives app
    restarts, scales separately; costs a network hop (~0.5 ms).
  * Redis vs Memcached: Memcached = simple multithreaded key->blob cache.
    Redis = rich data structures (lists, sorted sets, hashes, streams),
    persistence, replication, pub/sub, Lua scripts. Redis is the usual answer.

EVICTION: WHEN THE CACHE IS FULL, WHAT DO WE THROW OUT?
-------------------------------------------------------
* LRU (Least Recently Used): evict the item untouched for the longest.
  Great default: recency predicts reuse. Weakness: one big scan (e.g. a batch
  job reading every item once) flushes out the genuinely hot items.
* LFU (Least Frequently Used): evict the item with the fewest accesses.
  Protects long-term popular items; weakness: items that WERE popular
  (yesterday's news) linger ("cache pollution"), so real LFUs decay counts.
* FIFO: evict the oldest inserted. Simple; ignores popularity.
* Random: surprisingly OK and very cheap.
* Modern: W-TinyLFU (Caffeine), ARC, Redis "allkeys-lru"/"allkeys-lfu"
  (approximated via sampling). They combine recency + frequency.

INTERVIEW CLASSIC: IMPLEMENT AN O(1) LRU CACHE
-----------------------------------------------
Hash map (key -> node) for O(1) lookup + doubly linked list ordered by
recency for O(1) move-to-front and evict-from-back. Implemented below from
scratch (in real Python you'd use collections.OrderedDict).
"""

import random
from collections import OrderedDict, defaultdict


class _Node:
    __slots__ = ("key", "value", "prev", "next")

    def __init__(self, key=None, value=None):
        self.key, self.value = key, value
        self.prev = self.next = None


class LRUCache:
    """
    O(1) get/put. Layout:
        head <-> [most recent] <-> ... <-> [least recent] <-> tail
    head and tail are sentinel nodes so we never have to check for None.
    """

    def __init__(self, capacity):
        self.capacity = capacity
        self.map = {}
        self.head, self.tail = _Node(), _Node()
        self.head.next, self.tail.prev = self.tail, self.head
        self.hits = self.misses = 0

    def _unlink(self, node):
        node.prev.next, node.next.prev = node.next, node.prev

    def _push_front(self, node):
        node.prev, node.next = self.head, self.head.next
        self.head.next.prev = node
        self.head.next = node

    def get(self, key):
        node = self.map.get(key)
        if node is None:
            self.misses += 1
            return None
        self.hits += 1
        self._unlink(node)          # accessed -> becomes most recently used
        self._push_front(node)
        return node.value

    def put(self, key, value):
        if key in self.map:
            node = self.map[key]
            node.value = value
            self._unlink(node)
            self._push_front(node)
            return
        if len(self.map) >= self.capacity:
            lru = self.tail.prev    # least recently used sits just before tail
            self._unlink(lru)
            del self.map[lru.key]
        node = _Node(key, value)
        self.map[key] = node
        self._push_front(node)

    def keys_mru_to_lru(self):
        out, n = [], self.head.next
        while n is not self.tail:
            out.append(n.key)
            n = n.next
        return out


class LFUCache:
    """
    O(1) LFU: keep a bucket (OrderedDict) of keys per access frequency and
    track the minimum frequency. Ties are broken by LRU within a bucket.
    """

    def __init__(self, capacity):
        self.capacity = capacity
        self.values = {}
        self.freq = {}
        self.buckets = defaultdict(OrderedDict)  # freq -> keys (LRU order)
        self.min_freq = 0
        self.hits = self.misses = 0

    def _touch(self, key):
        f = self.freq[key]
        del self.buckets[f][key]
        if not self.buckets[f] and self.min_freq == f:
            self.min_freq += 1
        self.freq[key] = f + 1
        self.buckets[f + 1][key] = None

    def get(self, key):
        if key not in self.values:
            self.misses += 1
            return None
        self.hits += 1
        self._touch(key)
        return self.values[key]

    def put(self, key, value):
        if key in self.values:
            self.values[key] = value
            self._touch(key)
            return
        if len(self.values) >= self.capacity:
            evict, _ = self.buckets[self.min_freq].popitem(last=False)
            del self.values[evict], self.freq[evict]
        self.values[key] = value
        self.freq[key] = 1
        self.buckets[1][key] = None
        self.min_freq = 1


class FIFOCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.data = OrderedDict()
        self.hits = self.misses = 0

    def get(self, key):
        if key in self.data:
            self.hits += 1
            return self.data[key]
        self.misses += 1
        return None

    def put(self, key, value):
        if key not in self.data and len(self.data) >= self.capacity:
            self.data.popitem(last=False)
        self.data[key] = value


def zipf_workload(rng, n_items, n_requests, s=1.1):
    """Popularity ~ 1/rank^s: a few items are very hot, a long tail is cold."""
    weights = [1 / (rank ** s) for rank in range(1, n_items + 1)]
    return rng.choices(range(n_items), weights=weights, k=n_requests)


def run(cache, workload):
    for key in workload:
        if cache.get(key) is None:
            cache.put(key, f"value-of-{key}")   # miss -> load from "DB" and cache it
    return cache.hits / (cache.hits + cache.misses)


if __name__ == "__main__":
    rng = random.Random(9)

    print("=" * 72)
    print("PART 1: LRU mechanics (capacity 3)")
    print("=" * 72)
    c = LRUCache(3)
    for op in ["put a", "put b", "put c", "get a", "put d", "get b", "get c"]:
        verb, key = op.split()
        result = c.put(key, key.upper()) if verb == "put" else c.get(key)
        extra = f" -> {result}" if verb == "get" else ""
        print(f"  {op:6s}{extra:8s} order (MRU..LRU) = {c.keys_mru_to_lru()}")
    print("  -> 'put d' evicted b (least recently used), because 'get a' refreshed a.")

    print()
    print("=" * 72)
    print("PART 2: Hit rate vs cache size on a Zipf (skewed) workload")
    print("         10,000 distinct items, 200,000 requests")
    print("=" * 72)
    workload = zipf_workload(rng, 10_000, 200_000)
    print(f"  {'cache size':>12} {'% of items':>11} {'FIFO':>7} {'LRU':>7} {'LFU':>7}")
    for size in (50, 200, 1000, 3000):
        rates = [run(cls(size), workload) for cls in (FIFOCache, LRUCache, LFUCache)]
        print(f"  {size:12d} {size / 10_000:11.1%} " + " ".join(f"{r:7.1%}" for r in rates))
    print("  -> Caching just 2% of items serves the majority of requests (skew!).")

    print()
    print("=" * 72)
    print("PART 3: A one-off full scan pollutes LRU but not LFU")
    print("=" * 72)
    hot = zipf_workload(rng, 1_000, 20_000)
    scan = list(range(100_000, 102_000))          # batch job reads 2,000 cold items once
    after = zipf_workload(rng, 1_000, 5_000)
    for cls in (LRUCache, LFUCache):
        cache = cls(500)
        run(cache, hot)
        run(cache, scan)
        cache.hits = cache.misses = 0
        print(f"  {cls.__name__}: hit rate right after the scan = {run(cache, after):.1%}")
    print("  -> The scan evicted every hot item from LRU, so it had to re-warm (extra misses);")
    print("     LFU kept the hot items because the scanned items were only seen once.")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Reads are 100:1, and popularity is skewed, so a Redis cache with LRU eviction
   holding ~20% of the working set should absorb most reads."
* Know how to implement LRU in O(1): hash map + doubly linked list.
* Mention hit rate as the key metric, and that effective latency and DB load
  are dominated by the MISS rate.
""")
