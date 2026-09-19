"""
LESSON 04.03 -- CACHE FAILURE MODES: STAMPEDE, PENETRATION, AVALANCHE, HOT KEYS
================================================================================

Caches protect the database. These are the four ways that protection fails,
each with standard fixes. Interviewers love asking "what happens when...?"

1) CACHE STAMPEDE (a.k.a. THUNDERING HERD, DOG-PILE)
   A very popular key expires. 10,000 concurrent requests all miss at the
   same instant and ALL hit the database to recompute the same value.
   Fixes:
     * REQUEST COALESCING / "SINGLE FLIGHT" / MUTEX: the first miss takes a
       lock and recomputes; the others wait for (or briefly serve the stale)
       result. Only ONE DB query. (Go's singleflight, Nginx proxy_cache_lock)
     * PROBABILISTIC EARLY EXPIRATION: each reader, as expiry approaches,
       recomputes early with a small, increasing probability -> one request
       refreshes it before it actually expires.
     * STALE-WHILE-REVALIDATE: serve the stale value and refresh in the background.

2) CACHE PENETRATION
   Requests for keys that DON'T EXIST (e.g. attacker requests user IDs that
   aren't real). Every one misses the cache (nothing to cache!) and hits the DB.
   Fixes:
     * NEGATIVE CACHING: cache "not found" with a short TTL.
     * BLOOM FILTER in front: a compact set of all existing keys; if it says
       "definitely not present", reject without touching the DB. (Module 06.)
     * Input validation / rate limiting.

3) CACHE AVALANCHE
   Many keys expire at the SAME time (e.g. all loaded at startup with the
   same 1-hour TTL), or the cache cluster restarts cold -> massive DB spike.
   Fixes:
     * TTL JITTER: ttl = base + random(0, base * 0.1) spreads expirations.
     * Cache WARMING before sending traffic to a new/cold cache.
     * Replicated / highly available cache cluster; circuit breakers and
       rate limits protecting the DB (module 08).

4) HOT KEYS
   One key gets a huge share of traffic (a celebrity's profile, a viral
   tweet, a flash-sale product). With consistent hashing it all lands on ONE
   cache node, which saturates (CPU or network) even though the others are idle.
   Fixes:
     * LOCAL (in-process) CACHE on each app server for the hottest keys, with
       a short TTL -> requests never reach the shared cache node.
     * KEY REPLICATION / SPLITTING: store copies as "key#0".."key#9" on
       different nodes; readers pick one at random. Writes update all copies.
     * Detect hot keys dynamically (sample traffic, count-min sketch).
"""

import random
import threading
import time


class CountingDB:
    def __init__(self, latency=0.05):
        self.latency = latency
        self.queries = 0
        self.lock = threading.Lock()
        self.existing = {f"user:{i}" for i in range(1000)}

    def load(self, key):
        with self.lock:
            self.queries += 1
        time.sleep(self.latency)          # expensive query
        return f"profile-of-{key}" if key in self.existing else None


# ---------------------------------------------------------------------------
# 1) STAMPEDE
# ---------------------------------------------------------------------------
class NaiveCache:
    def __init__(self, db):
        self.db, self.data = db, {}

    def get(self, key):
        if key in self.data:
            return self.data[key]
        value = self.db.load(key)             # every concurrent miss does this!
        self.data[key] = value
        return value


class SingleFlightCache(NaiveCache):
    """Concurrent misses for the same key share ONE in-flight load."""

    def __init__(self, db):
        super().__init__(db)
        self.inflight = {}                    # key -> Event signalled when the load finishes
        self.lock = threading.Lock()

    def get(self, key):
        if key in self.data:
            return self.data[key]
        with self.lock:
            if key in self.data:
                return self.data[key]
            event = self.inflight.get(key)
            leader = event is None
            if leader:
                event = self.inflight[key] = threading.Event()
        if leader:
            self.data[key] = self.db.load(key)
            with self.lock:
                del self.inflight[key]
            event.set()
        else:
            event.wait()                      # followers wait for the leader's result
        return self.data[key]


def stampede(cache_cls, n_clients=100):
    db = CountingDB()
    cache = cache_cls(db)
    start = threading.Barrier(n_clients)

    def client():
        start.wait()                          # everyone arrives at the same moment
        cache.get("user:1")                   # the hot key that just expired

    threads = [threading.Thread(target=client) for _ in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return db.queries


# ---------------------------------------------------------------------------
# 2) PENETRATION
# ---------------------------------------------------------------------------
def penetration(negative_caching):
    db = CountingDB(latency=0)
    cache = {}
    for i in range(10_000):
        key = f"user:{random.randint(10_000, 10_050)}"   # attacker: IDs that don't exist
        if key in cache:
            continue
        value = db.load(key)
        if value is not None or negative_caching:
            cache[key] = value                  # cache the "None" too (with a short TTL in reality)
    return db.queries


# ---------------------------------------------------------------------------
# 3) AVALANCHE
# ---------------------------------------------------------------------------
def avalanche(jitter):
    rng = random.Random(1)
    base_ttl = 3600
    expiries = [base_ttl + (rng.uniform(0, 0.2 * base_ttl) if jitter else 0) for _ in range(100_000)]
    per_minute = {}
    for e in expiries:
        per_minute[int(e // 60)] = per_minute.get(int(e // 60), 0) + 1
    return max(per_minute.values())


# ---------------------------------------------------------------------------
# 4) HOT KEYS
# ---------------------------------------------------------------------------
def hot_key_load(copies, n_nodes=10, requests=100_000):
    """Returns max share of traffic on any single cache node."""
    rng = random.Random(2)
    load = [0] * n_nodes
    for _ in range(requests):
        if rng.random() < 0.5:                     # 50% of all traffic: the celebrity key
            replica = rng.randrange(copies)        # read a random copy "celeb#i"
            node = (3 + replica) % n_nodes   # copies "celeb#0..#k" placed on different nodes
        else:
            node = rng.randrange(n_nodes)          # everything else spreads evenly
        load[node] += 1
    return max(load) / requests


if __name__ == "__main__":
    print("=" * 72)
    print("1) STAMPEDE: 100 concurrent requests for a key that just expired")
    print("=" * 72)
    print(f"  naive cache        : {stampede(NaiveCache):3d} DB queries")
    print(f"  single-flight cache: {stampede(SingleFlightCache):3d} DB query")

    print()
    print("=" * 72)
    print("2) PENETRATION: 10,000 requests for 51 non-existent user IDs")
    print("=" * 72)
    random.seed(3)
    print(f"  no negative caching: {penetration(False):5d} DB queries (every request!)")
    random.seed(3)
    print(f"  negative caching   : {penetration(True):5d} DB queries")

    print()
    print("=" * 72)
    print("3) AVALANCHE: 100,000 keys loaded at startup with TTL = 1 hour")
    print("=" * 72)
    print(f"  no jitter : worst minute sees {avalanche(False):6d} simultaneous expirations -> DB spike")
    print(f"  20% jitter: worst minute sees {avalanche(True):6d} expirations")

    print()
    print("=" * 72)
    print("4) HOT KEY: one key gets 50% of traffic across 10 cache nodes")
    print("=" * 72)
    for copies in (1, 5, 10):
        print(f"  {copies:2d} cop{'y' if copies == 1 else 'ies'} of the hot key: busiest node handles "
              f"{hot_key_load(copies):.0%} of all traffic (ideal 10%)")

    print("""
INTERVIEW TALKING POINTS
------------------------
* When you add a cache, proactively mention: single-flight/locking for
  stampedes, negative caching or a Bloom filter for penetration, TTL jitter
  for avalanches, and local caching / key replication for hot keys.
* The DB must survive a cold cache: warm caches before cutover, and put
  rate limits / circuit breakers in front of the DB.
""")
