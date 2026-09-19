"""
LESSON 04.02 -- CACHING STRATEGIES (READ AND WRITE PATTERNS) AND INVALIDATION
==============================================================================

"There are only two hard things in Computer Science: cache invalidation and
 naming things." -- Phil Karlton

A cache is a COPY of data. The moment the source changes, the copy may be
wrong. Strategies differ in who loads the cache and when writes reach it.

READ PATTERNS
-------------
1) CACHE-ASIDE (lazy loading) -- the most common pattern
       read:  value = cache.get(k)
              if miss: value = db.read(k); cache.set(k, value, ttl)
       write: db.write(k, v); cache.delete(k)      <- DELETE, don't update!
   + Only requested data gets cached; cache failure = slower, not broken.
   - First request for each key is a miss; stale data possible between the DB
     write and the delete (or if the delete fails) -> always set a TTL as a
     safety net.
   Why delete instead of update on write? Two concurrent writers can update
   the cache in the opposite order from the DB, leaving the cache permanently
   wrong. Deleting means the next reader reloads the truth.

2) READ-THROUGH
   Same as cache-aside, but the CACHE LIBRARY loads from the DB on a miss
   (app only talks to the cache). Cleaner app code.

WRITE PATTERNS
--------------
3) WRITE-THROUGH: write to cache AND DB synchronously (cache layer does both).
   + Cache is always fresh; reads right after writes hit.
   - Every write pays both latencies; cache fills with data never read.

4) WRITE-BACK (write-behind): write to the cache only; flush to the DB
   asynchronously (batched) later.
   + Very fast writes; batching absorbs bursts (e.g. counters, analytics).
   - DATA LOSS RISK if the cache node dies before flushing. Use only with a
     durable/replicated cache or for data you can afford to lose.

5) WRITE-AROUND: write to the DB only; cache is filled on the next read.
   + Avoids polluting the cache with write-once data (logs, uploads).
   - Read-after-write is a miss.

TTL (TIME TO LIVE)
------------------
Every cache entry should expire. Short TTL = fresher data, more DB load.
Long TTL = stale data risk. Pick per data type: user profile 5 min, product
catalogue 1 h, exchange rates 10 s, static assets 1 year (with versioned URLs).

INVALIDATION APPROACHES
-----------------------
* TTL only (simplest; bounded staleness).
* Explicit delete on write (cache-aside).
* Event-driven: DB change-data-capture (CDC, e.g. Debezium reading the
  binlog) publishes changes; a consumer invalidates caches. Robust because it
  doesn't depend on every code path remembering to invalidate.
* Versioned keys: "user:42:v7" -- bump the version, old entries just age out.

The simulation below compares the strategies on latency, DB load, staleness,
and what happens if the cache crashes.
"""

import random


class SlowDB:
    LATENCY_MS = 10

    def __init__(self):
        self.data = {}
        self.reads = self.writes = 0

    def read(self, k):
        self.reads += 1
        return self.data.get(k)

    def write(self, k, v):
        self.writes += 1
        self.data[k] = v


class Cache:
    LATENCY_MS = 0.5

    def __init__(self):
        self.data = {}

    def get(self, k):
        return self.data.get(k)

    def set(self, k, v):
        self.data[k] = v

    def delete(self, k):
        self.data.pop(k, None)


class CacheAside:
    name = "cache-aside"

    def __init__(self):
        self.db, self.cache, self.latency = SlowDB(), Cache(), 0.0

    def read(self, k):
        v = self.cache.get(k)
        self.latency += Cache.LATENCY_MS
        if v is None:
            v = self.db.read(k)
            self.latency += SlowDB.LATENCY_MS
            if v is not None:
                self.cache.set(k, v)
        return v

    def write(self, k, v):
        self.db.write(k, v)
        self.cache.delete(k)
        self.latency += SlowDB.LATENCY_MS + Cache.LATENCY_MS


class WriteThrough(CacheAside):
    name = "write-through"

    def write(self, k, v):
        self.db.write(k, v)
        self.cache.set(k, v)
        self.latency += SlowDB.LATENCY_MS + Cache.LATENCY_MS


class WriteBack(CacheAside):
    name = "write-back"

    def __init__(self):
        super().__init__()
        self.dirty = {}

    def write(self, k, v):
        self.cache.set(k, v)
        self.dirty[k] = v                   # remember to flush later
        self.latency += Cache.LATENCY_MS

    def flush(self):
        # One batched write per dirty KEY, no matter how many times it changed.
        for k, v in self.dirty.items():
            self.db.write(k, v)
        self.dirty.clear()


class WriteAround(CacheAside):
    name = "write-around"

    def write(self, k, v):
        self.db.write(k, v)                 # cache untouched; may hold stale value
        self.latency += SlowDB.LATENCY_MS   # (real systems still delete or rely on TTL)
        self.cache.delete(k)


def simulate(strategy_cls, rng):
    s = strategy_cls()
    keys = [f"k{i}" for i in range(100)]
    for k in keys:
        s.db.data[k] = 0
    ops = 0
    for step in range(20_000):
        k = rng.choice(keys[:20]) if rng.random() < 0.8 else rng.choice(keys)  # skewed
        if rng.random() < 0.2:
            s.write(k, step)
        else:
            s.read(k)
        ops += 1
        if isinstance(s, WriteBack) and step % 1000 == 999:
            s.flush()
    return s, ops


if __name__ == "__main__":
    rng = random.Random(4)
    print("=" * 78)
    print("PART 1: 20,000 ops (80% reads, 20% writes, skewed keys)")
    print("=" * 78)
    print(f"  {'strategy':15s} {'avg latency':>12s} {'DB reads':>9s} {'DB writes':>10s}")
    for cls in (CacheAside, WriteThrough, WriteBack, WriteAround):
        s, ops = simulate(cls, random.Random(4))
        print(f"  {s.name:15s} {s.latency / ops:9.2f} ms {s.db.reads:9d} {s.db.writes:10d}")
    print("  -> write-back has the lowest latency and far fewer DB writes (batched),")
    print("     write-through avoids post-write misses (fewer DB reads than cache-aside).")

    print()
    print("=" * 78)
    print("PART 2: The danger of write-back -- cache node crashes before flushing")
    print("=" * 78)
    wb = WriteBack()
    for i in range(5):
        wb.write(f"order:{i}", "PAID")
    wb.cache = Cache()      # crash! in-memory cache contents lost
    lost = [k for k in wb.dirty if wb.db.read(k) is None]
    print(f"  {len(lost)} acknowledged writes never reached the DB: {lost}")
    print("  -> never use write-back for money unless the cache itself is durable/replicated.")

    print()
    print("=" * 78)
    print("PART 3: Why cache-aside DELETES on write instead of SETTING the new value")
    print("=" * 78)
    db, cache = SlowDB(), Cache()
    # Two writers race. Real-time order of operations:
    db.write("price", 100)        # writer A updates DB
    db.write("price", 200)        # writer B updates DB (the latest truth is 200)
    cache.set("price", 200)       # writer B updates cache
    cache.set("price", 100)       # writer A's cache update arrives late
    print(f"  update-on-write: DB={db.data['price']} cache={cache.get('price')}  <- WRONG until TTL expires")
    cache.delete("price")
    cache.delete("price")         # both writers delete instead
    print(f"  delete-on-write: cache={cache.get('price')} -> next read reloads {db.read('price')} from DB")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Default answer: cache-aside with TTLs, delete-on-write.
* Write-through when reads immediately follow writes and freshness matters.
* Write-back for high-volume, loss-tolerant writes (view counters, metrics),
  with periodic batched flushes.
* Mention invalidation via CDC events for robustness, and TTL as a safety net.
* Say what staleness the product can tolerate -- it decides the TTL.
""")
