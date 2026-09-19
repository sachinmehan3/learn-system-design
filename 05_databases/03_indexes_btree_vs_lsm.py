"""
LESSON 05.03 -- INDEXES AND STORAGE ENGINES: B-TREES vs LSM-TREES
==================================================================

WHAT AN INDEX IS
----------------
Without an index, "WHERE email = 'x'" must scan EVERY row: O(n). An index is
an extra data structure, sorted by the indexed column(s), that points to rows,
making lookups O(log n). Like a book's index vs reading every page.

TRADE-OFF: every index speeds up reads on that column but SLOWS DOWN WRITES
(each insert/update must also update every index) and uses storage. Index the
columns you filter, join, and sort on -- not everything.

INDEX VARIETIES
  * PRIMARY / CLUSTERED index: the table itself is stored in key order
    (InnoDB). One per table.
  * SECONDARY index: separate structure: column value -> primary key/row.
  * COMPOSITE index on (a, b, c): sorted by a, then b, then c. Serves queries
    filtering on a, (a,b), (a,b,c) -- the LEFTMOST PREFIX rule -- but NOT on b
    alone. Column order matters!
  * COVERING index: contains every column a query needs -> no table lookup.
  * HASH index: O(1) equality only; no ranges.
  * Also: full-text (inverted) indexes, geospatial (R-tree / geohash),
    bitmap indexes (analytics).

HOW DATABASES STORE DATA ON DISK -- THE TWO BIG FAMILIES
--------------------------------------------------------
1) B-TREE (B+ tree) -- PostgreSQL, MySQL InnoDB, SQL Server, Oracle
   * A balanced tree of fixed-size PAGES (e.g. 8-16 KB). Each internal node
     holds hundreds of keys -> a tree over a billion rows is only ~4 levels
     deep; top levels are cached in RAM -> ~1 disk read per lookup.
   * Updates happen IN PLACE: find the page, modify it, write it back (with a
     WAL for crash safety). Page splits when full.
   * + Fast, predictable reads; great for range scans; mature transactions.
   * - Random writes (each write touches some random page) -> write
     amplification; fragmentation.

2) LSM-TREE (Log-Structured Merge tree) -- Cassandra, RocksDB, LevelDB,
   HBase, ScyllaDB, DynamoDB internals, InfluxDB
   * WRITES: append to a WAL (sequential, for durability) and insert into an
     in-memory sorted structure (MEMTABLE). When the memtable is full, flush
     it to disk as an immutable sorted file (SSTABLE -- Sorted String Table).
     => ALL disk writes are sequential. Very high write throughput.
   * READS: check memtable, then SSTables from newest to oldest. To avoid
     reading every file: each SSTable has a BLOOM FILTER ("key is definitely
     not here") and a sparse index.
   * COMPACTION: background merging of SSTables (like merge sort) discards
     overwritten values and deleted keys (TOMBSTONES), keeping reads fast.
   * + Superb write throughput, good compression.
   * - Reads may touch several files (read amplification); compaction uses
     CPU/IO in the background and can cause latency spikes.

RULE OF THUMB: B-tree for read-heavy / transactional workloads; LSM for
write-heavy workloads (logs, metrics, messages, time-series, IoT).

The demo: (1) sqlite3 query plans with and without an index, and the
leftmost-prefix rule; (2) a working mini LSM-tree.
"""

import bisect
import hashlib
import random
import sqlite3
import time


# ---------------------------------------------------------------------------
# PART 1: real indexes in sqlite
# ---------------------------------------------------------------------------
def index_demo():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, country TEXT, age INT)")
    rng = random.Random(1)
    countries = ["US", "IN", "DE", "BR", "JP"]
    db.executemany("INSERT INTO users (email, country, age) VALUES (?,?,?)",
                   ((f"user{i}@mail.com", rng.choice(countries), rng.randint(18, 80))
                    for i in range(300_000)))

    def timed(sql, args=()):
        plan = db.execute("EXPLAIN QUERY PLAN " + sql, args).fetchall()[0][-1]
        t = time.perf_counter()
        for _ in range(20):
            db.execute(sql, args).fetchall()
        return (time.perf_counter() - t) / 20 * 1000, plan

    q = "SELECT * FROM users WHERE email = ?"
    ms, plan = timed(q, ("user250000@mail.com",))
    print(f"  no index   : {ms:8.3f} ms   plan: {plan}")
    db.execute("CREATE INDEX idx_email ON users(email)")
    ms, plan = timed(q, ("user250000@mail.com",))
    print(f"  with index : {ms:8.3f} ms   plan: {plan}")

    print("\n  Composite index (country, age) and the LEFTMOST PREFIX rule:")
    db.execute("CREATE INDEX idx_country_age ON users(country, age)")
    for sql in ("SELECT COUNT(*) FROM users WHERE country = 'DE' AND age = 30",
                "SELECT COUNT(*) FROM users WHERE country = 'DE'",
                "SELECT COUNT(*) FROM users WHERE age = 30"):
        ms, plan = timed(sql)
        print(f"    {sql[33:]:32s} {ms:7.3f} ms  plan: {plan}")
    print("    -> 'age' alone can't SEARCH (country, age) -- it's not a leftmost prefix -- so the")
    print("       engine falls back to a full SCAN (here of the index, since it happens to cover the query).")

    print("\n  Write cost of indexes (insert 20,000 rows):")
    for n_idx in (0, 3):
        d = sqlite3.connect(":memory:")
        d.execute("CREATE TABLE t (a INT, b INT, c INT, d INT)")
        for i in range(n_idx):
            d.execute(f"CREATE INDEX i{i} ON t({'abc'[i]})")
        t = time.perf_counter()
        with d:
            d.executemany("INSERT INTO t VALUES (?,?,?,?)",
                          ((rng.random(), rng.random(), rng.random(), 1) for _ in range(20_000)))
        print(f"    {n_idx} secondary indexes: {(time.perf_counter() - t) * 1000:6.1f} ms")


# ---------------------------------------------------------------------------
# PART 2: a mini LSM-tree
# ---------------------------------------------------------------------------
TOMBSTONE = object()   # marker meaning "this key was deleted"


class BloomFilter:
    """Tiny Bloom filter (see module 06 for a full explanation)."""

    def __init__(self, size=2048, k=3):
        self.bits = bytearray(size)
        self.size, self.k = size, k

    def _positions(self, key):
        digest = hashlib.sha256(key.encode()).digest()
        return [int.from_bytes(digest[i * 4:(i + 1) * 4], "big") % self.size for i in range(self.k)]

    def add(self, key):
        for p in self._positions(key):
            self.bits[p] = 1

    def might_contain(self, key):
        return all(self.bits[p] for p in self._positions(key))


class SSTable:
    """Immutable, sorted run of (key, value) pairs + a Bloom filter."""

    def __init__(self, items):
        self.keys = [k for k, _ in items]
        self.values = [v for _, v in items]
        self.bloom = BloomFilter()
        for k in self.keys:
            self.bloom.add(k)

    def get(self, key):
        i = bisect.bisect_left(self.keys, key)     # binary search: sorted file
        if i < len(self.keys) and self.keys[i] == key:
            return True, self.values[i]
        return False, None


class LSMTree:
    def __init__(self, memtable_limit=4, compact_after=4):
        self.wal = []                  # append-only log (would be a file, fsync'd)
        self.memtable = {}             # real engines use a skip list / red-black tree
        self.sstables = []             # newest last
        self.memtable_limit = memtable_limit
        self.compact_after = compact_after
        self.stats = {"sstables_checked": 0, "bloom_skips": 0}

    def put(self, key, value):
        self.wal.append((key, value))  # 1) durability: sequential append
        self.memtable[key] = value     # 2) fast in-memory write
        if len(self.memtable) >= self.memtable_limit:
            self._flush()

    def delete(self, key):
        self.put(key, TOMBSTONE)       # deletes are writes too!

    def _flush(self):
        self.sstables.append(SSTable(sorted(self.memtable.items())))
        self.memtable = {}
        self.wal = []                  # data is now safely in an SSTable
        if len(self.sstables) >= self.compact_after:
            self._compact()

    def _compact(self):
        """Merge all SSTables; newer values win; drop tombstones."""
        merged = {}
        for table in self.sstables:    # oldest -> newest, so newer overwrite older
            merged.update(zip(table.keys, table.values))
        live = sorted((k, v) for k, v in merged.items() if v is not TOMBSTONE)
        self.sstables = [SSTable(live)]

    def get(self, key):
        if key in self.memtable:
            v = self.memtable[key]
            return None if v is TOMBSTONE else v
        for table in reversed(self.sstables):       # newest first
            if not table.bloom.might_contain(key):
                self.stats["bloom_skips"] += 1      # skipped a disk read!
                continue
            self.stats["sstables_checked"] += 1
            found, v = table.get(key)
            if found:
                return None if v is TOMBSTONE else v
        return None

    def describe(self):
        return (f"memtable={dict(self.memtable)} | " +
                " | ".join(f"SST{i}{t.keys}" for i, t in enumerate(self.sstables)))


def lsm_demo():
    lsm = LSMTree(memtable_limit=3, compact_after=3)
    ops = [("put", "apple", 1), ("put", "cherry", 2), ("put", "banana", 3),
           ("put", "apple", 10), ("put", "date", 4), ("del", "cherry", None),
           ("put", "fig", 5), ("put", "grape", 6), ("put", "kiwi", 7)]
    for op, k, v in ops:
        lsm.put(k, v) if op == "put" else lsm.delete(k)
        print(f"  {op} {k:7s} -> {lsm.describe()}")
    print("  (compaction merged the SSTables, kept apple=10 over apple=1, dropped deleted cherry)")
    for k in ("apple", "cherry", "kiwi", "zucchini"):
        print(f"  get({k!r}) = {lsm.get(k)}")

    big = LSMTree(memtable_limit=100, compact_after=10)
    for i in range(900):
        big.put(f"key{i:05d}", i)
    for i in range(0, 2000, 7):
        big.get(f"key{i:05d}")
    print(f"\n  900 keys in {len(big.sstables)} SSTables; lookups skipped "
          f"{big.stats['bloom_skips']} SSTable reads thanks to Bloom filters "
          f"(and did {big.stats['sstables_checked']} real reads).")


if __name__ == "__main__":
    print("=" * 76)
    print("PART 1: Indexes in a real SQL engine (300,000 rows)")
    print("=" * 76)
    index_demo()
    print()
    print("=" * 76)
    print("PART 2: A mini LSM-tree (memtable -> SSTables -> compaction)")
    print("=" * 76)
    lsm_demo()

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Add an index on (user_id, created_at) so 'latest posts by user' is an
   index range scan instead of a table scan." (Composite, right column order.)
* Every index costs write throughput and storage -- don't index everything.
* Write-heavy (chat messages, metrics, logs): LSM-based stores like
  Cassandra/RocksDB. Read-heavy transactional: B-tree stores like Postgres/MySQL.
* LSM reads are made fast with memtable + Bloom filters + compaction.
""")
