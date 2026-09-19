"""
CASE STUDY 01 -- DESIGN A URL SHORTENER (bit.ly / TinyURL)
==========================================================
The classic warm-up question. Small surface, but touches ID generation,
encoding, caching, databases, redirects, and read-heavy scaling.

STEP 1 -- REQUIREMENTS
----------------------
Functional:
  * Given a long URL, return a short URL (optionally a custom alias).
  * Visiting the short URL redirects to the long URL.
  * Optional: expiration, click analytics.
Non-functional:
  * Very read-heavy (100:1). Redirects must be FAST (p99 < 50 ms) and HIGHLY
    AVAILABLE (a dead short link breaks someone's tweet/email forever).
  * Short codes must be unique and ideally not guessable/enumerable.
  * Durable: links shouldn't disappear.

STEP 2 -- ESTIMATION (see 01_fundamentals/05_back_of_envelope.py)
------------------------------------------------------------------
  * 100M new URLs/month -> ~40 writes/s (peak ~120). Reads ~4,000/s (peak ~12k).
  * 10 years -> 12B URLs x ~500 B = ~6 TB (fine for a sharded KV store /
    even a large single DB with replicas; replication x3 = ~18 TB).
  * Code length: base62 (a-z, A-Z, 0-9). 62^7 = 3.5 trillion -> 7 chars is plenty.
  * Cache: 20% of daily hot URLs ~ tens of GB -> fits in a Redis cluster.

API
  POST /api/v1/urls {long_url, custom_alias?, expires_at?} -> 201 {short_url}
  GET  /{code} -> 302 Location: <long_url>   (or 404 / 410 Gone if expired)

DATA MODEL (key-value access pattern: code -> long_url)
  urls(code PK, long_url, user_id, created_at, expires_at)
  A KV store (DynamoDB/Cassandra) or a sharded SQL table keyed by code both work.

STEP 3 -- HIGH-LEVEL DESIGN
---------------------------
   client --> CDN/edge --> LB --> URL service (stateless) --> Redis cache --> DB (code -> url)
                                         |                                  (replicated,
                                         +--> Key Generation Service         sharded by code)
                                         +--> Kafka (click events) --> analytics workers

  Write: validate URL -> get a unique code -> store (code, long_url) -> return.
  Read:  cache lookup -> on miss DB lookup -> cache fill -> 302 redirect.

STEP 4 -- DEEP DIVES
--------------------
A) GENERATING THE SHORT CODE -- three approaches:
   1. HASH the long URL (MD5/SHA) and take the first 7 base62 chars.
      + Same URL -> same code (dedupe). - COLLISIONS: must check the DB and
      retry with a salt; check-then-insert needs a unique constraint.
   2. COUNTER + BASE62 ENCODE: a unique integer ID -> base62 string.
      + No collisions, short codes. - Sequential codes are GUESSABLE (someone
      can enumerate all links) -> shuffle the ID with a keyed bijective
      scramble (e.g. a small Feistel cipher, implemented below) first.
      Needs a distributed unique counter (Snowflake / ranges).
   3. KEY GENERATION SERVICE (KGS): pre-generate random unique codes offline
      and hand out BATCHES to app servers (each server keeps ~1000 in memory).
      + No collision checks on the hot path, unguessable. - KGS must be HA; a
      crashed server wastes its unused batch (fine: the key space is huge).
   Implemented below: counter ranges + scramble + base62, and hash+collision.

B) 301 vs 302 REDIRECT
   * 301 Moved Permanently: browsers CACHE it -> less load, but you lose
     analytics (repeat clicks never reach you) and can't change the target.
   * 302 Found (temporary): every click hits you -> analytics, editable.
   Most shorteners use 302 (or 307) when analytics matter.

C) SCALING READS: cache-aside in Redis (LRU, hot links), CDN/edge caching of
   the redirect for very hot links, read replicas. Hit rates are very high
   (link popularity is extremely skewed).

D) ANALYTICS: don't write to the DB on each click (slows redirects). Emit a
   click event to Kafka asynchronously; stream processors aggregate counts
   (by link, country, referrer) into an analytics store.

E) OTHER: custom aliases (check uniqueness; reserve words), expiration (TTL
   + lazy deletion on read + periodic cleanup job), abuse (rate limit
   creation per user/IP, malware URL scanning), and hot-key protection.
"""

import hashlib
import string
from collections import OrderedDict

ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase   # base62
BASE = len(ALPHABET)
CODE_LEN = 7
SPACE = BASE ** CODE_LEN


def base62_encode(n: int) -> str:
    if n == 0:
        return ALPHABET[0]
    out = []
    while n:
        n, r = divmod(n, BASE)
        out.append(ALPHABET[r])
    return "".join(reversed(out))


def base62_decode(s: str) -> int:
    n = 0
    for ch in s:
        n = n * BASE + ALPHABET.index(ch)
    return n


# A keyed, reversible scramble so sequential IDs don't produce sequential
# (guessable) codes. A FEISTEL NETWORK is a classic way to build a bijection
# (a permutation) on n-bit numbers from any hash function. 62^7 < 2^42, so we
# permute 42-bit numbers and "cycle-walk": if the output is >= 62^7, apply the
# permutation again until it lands inside the code space. Still a bijection.
HALF_BITS = 21
HALF_MASK = (1 << HALF_BITS) - 1
ROUND_KEYS = (b"k1-secret", b"k2-secret", b"k3-secret", b"k4-secret")


def _round(value: int, key: bytes) -> int:
    digest = hashlib.blake2b(value.to_bytes(4, "big"), key=key, digest_size=4).digest()
    return int.from_bytes(digest, "big") & HALF_MASK


def _feistel(n: int, keys) -> int:
    left, right = n >> HALF_BITS, n & HALF_MASK
    for k in keys:
        left, right = right, left ^ _round(right, k)
    return (left << HALF_BITS) | right


def _feistel_inverse(n: int, keys) -> int:
    left, right = n >> HALF_BITS, n & HALF_MASK
    for k in reversed(keys):
        left, right = right ^ _round(left, k), left
    return (left << HALF_BITS) | right


def scramble(n: int) -> int:
    x = _feistel(n, ROUND_KEYS)
    while x >= SPACE:                      # cycle-walk back into [0, 62^7)
        x = _feistel(x, ROUND_KEYS)
    return x


def unscramble(n: int) -> int:
    x = _feistel_inverse(n, ROUND_KEYS)
    while x >= SPACE:
        x = _feistel_inverse(x, ROUND_KEYS)
    return x


class RangeAllocator:
    """Central counter service handing out ranges (like ZooKeeper/etcd-backed allocation)."""

    def __init__(self, range_size=1000):
        self.next_start, self.range_size = 1, range_size

    def get_range(self):
        start = self.next_start
        self.next_start += self.range_size
        return range(start, start + self.range_size)


class LRUCache:
    def __init__(self, capacity):
        self.capacity, self.data = capacity, OrderedDict()
        self.hits = self.misses = 0

    def get(self, k):
        if k in self.data:
            self.data.move_to_end(k)
            self.hits += 1
            return self.data[k]
        self.misses += 1
        return None

    def put(self, k, v):
        self.data[k] = v
        self.data.move_to_end(k)
        if len(self.data) > self.capacity:
            self.data.popitem(last=False)


class URLShortener:
    """One stateless app server's logic, with a shared DB and cache."""

    def __init__(self, allocator, db, cache):
        self.allocator, self.db, self.cache = allocator, db, cache
        self.ids = iter(())                       # local batch of IDs

    def _next_id(self):
        try:
            return next(self.ids)
        except StopIteration:
            self.ids = iter(self.allocator.get_range())   # fetch a new batch (rare network call)
            return next(self.ids)

    def shorten(self, long_url, alias=None):
        if alias:
            if alias in self.db:
                raise ValueError("alias taken")
            code = alias
        else:
            code = base62_encode(scramble(self._next_id())).rjust(CODE_LEN, "0")
        self.db[code] = long_url
        return f"https://sho.rt/{code}"

    def resolve(self, code):
        url = self.cache.get(code)
        if url is None:
            url = self.db.get(code)                # cache miss -> DB
            if url is None:
                return 404, None
            self.cache.put(code, url)
        return 302, url                            # 302 keeps analytics possible


def hash_based_code(long_url, db, attempt=0):
    """Alternative: hash + truncate, with collision handling."""
    digest = hashlib.sha256(f"{long_url}{attempt or ''}".encode()).digest()
    code = base62_encode(int.from_bytes(digest[:8], "big") % SPACE).rjust(CODE_LEN, "0")
    if code in db and db[code] != long_url:
        return hash_based_code(long_url, db, attempt + 1)     # collision -> rehash with salt
    return code


if __name__ == "__main__":
    import random
    rng = random.Random(1)

    print("=" * 76)
    print("Base62 + scrambled counter: sequential IDs -> non-sequential codes")
    print("=" * 76)
    for i in (1, 2, 3, 1_000_000):
        code = base62_encode(scramble(i)).rjust(CODE_LEN, "0")
        back = unscramble(base62_decode(code))
        print(f"  id={i:9,d} -> code {code}  (decodes back to {back:,})")

    print()
    print("=" * 76)
    print("Two app servers sharing an allocator: unique codes, no coordination per request")
    print("=" * 76)
    db, cache, alloc = {}, LRUCache(1000), RangeAllocator(range_size=1000)
    server_a, server_b = URLShortener(alloc, db, cache), URLShortener(alloc, db, cache)
    urls = [f"https://example.com/articles/{i}?utm=x" for i in range(20_000)]
    codes = [(server_a if i % 2 else server_b).shorten(u) for i, u in enumerate(urls)]
    print(f"  created {len(codes):,} short URLs, unique: {len(set(codes)) == len(codes)}, "
          f"ranges handed out: {(alloc.next_start - 1) // 1000}")
    print(f"  examples: {codes[:3]}")
    print(f"  custom alias: {server_a.shorten('https://sysdesign.dev', alias='sysdesign')}")

    print()
    print("=" * 76)
    print("Redirect traffic: 200,000 clicks with skewed popularity, cache of 1,000 entries")
    print("=" * 76)
    weights = [1 / (r ** 1.1) for r in range(1, len(codes) + 1)]
    clicks = rng.choices(codes, weights=weights, k=200_000)
    statuses = [server_a.resolve(c.rsplit("/", 1)[1])[0] for c in clicks]
    print(f"  all redirects 302: {set(statuses) == {302}}; cache hit rate "
          f"{cache.hits / (cache.hits + cache.misses):.1%} -> the DB sees only the misses")
    print(f"  unknown code: {server_a.resolve('zzzzzzz')}")

    print()
    print("=" * 76)
    print("Hash-based alternative (idempotent: same URL -> same code)")
    print("=" * 76)
    hdb = {}
    for u in ("https://a.com", "https://b.com", "https://a.com"):
        code = hash_based_code(u, hdb)
        hdb[code] = u
        print(f"  {u:15s} -> {code}")

    print("""
FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How do you prevent enumeration? (scrambled IDs / random KGS keys)
* What if the counter service dies? (ranges pre-fetched; HA via etcd/ZooKeeper;
  unused ranges are simply skipped)
* How to handle a viral link? (cache + CDN edge caching; hot key replication)
* Analytics without slowing redirects? (async click events to Kafka)
* Multi-region? (region-local reads from replicated KV store; writes can go to
  any region if each region allocates from its own ID ranges)
""")
