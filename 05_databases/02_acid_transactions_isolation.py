"""
LESSON 05.02 -- ACID, TRANSACTIONS, ISOLATION LEVELS, AND CONCURRENCY CONTROL
==============================================================================

A TRANSACTION groups several reads/writes into one logical unit: either all
of it happens, or none of it. The classic example: transfer $100 from A to B
= debit A + credit B. Crash in the middle must never lose or create money.

ACID
----
* ATOMICITY: all-or-nothing. On error/crash, partial work is ROLLED BACK.
  (Implemented with a write-ahead log (WAL) / undo log.)
* CONSISTENCY: the transaction moves the DB from one valid state to another
  (constraints, foreign keys, invariants like "balance >= 0" hold). Mostly the
  application's job, enforced with constraints. (Not the C in CAP!)
* ISOLATION: concurrent transactions don't see each other's half-done work;
  ideally the result is as if they ran one at a time (serializable).
* DURABILITY: once committed, it survives crashes (fsync'd WAL, replication).

ISOLATION LEVELS AND THE ANOMALIES THEY ALLOW
---------------------------------------------
Full serializability is expensive, so databases offer weaker levels:

    Level              Dirty read  Non-repeatable read  Phantom   Lost update / write skew
    Read Uncommitted   possible    possible             possible  possible
    Read Committed     prevented   possible             possible  possible     <- Postgres/Oracle default
    Repeatable Read /  prevented   prevented            mostly    lost update prevented in PG;
      Snapshot Isol.                                    prevented write skew still possible  <- MySQL InnoDB default
    Serializable       prevented   prevented            prevented prevented

* DIRTY READ: you see another transaction's UNCOMMITTED write (which may
  roll back).
* NON-REPEATABLE READ: you read a row twice in one transaction and get
  different values because someone committed in between.
* PHANTOM: you run the same range query twice and new rows appear.
* LOST UPDATE: two transactions read-modify-write the same value; one
  overwrites the other's change. (Two users like a post at the same time:
  both read 10, both write 11. One like is lost.)
* WRITE SKEW: two transactions read the same data, then update DIFFERENT
  rows based on it, breaking an invariant. (Two on-call doctors both check
  "is someone else on call?" -> yes -> both go off call. Nobody's on call.)

HOW DATABASES IMPLEMENT ISOLATION
---------------------------------
* PESSIMISTIC LOCKING (two-phase locking, 2PL): lock rows as you touch them;
  others wait. SELECT ... FOR UPDATE takes an explicit write lock.
  Safe; can cause contention and DEADLOCKS (DB detects and aborts one).
* MVCC (multi-version concurrency control): writers create new VERSIONS;
  readers see a consistent SNAPSHOT as of their start time. Readers never
  block writers and vice versa. (Postgres, MySQL InnoDB, Oracle.)
* OPTIMISTIC CONCURRENCY CONTROL (OCC): don't lock; at commit/update time
  check that nothing changed (e.g. a version column), else retry:
      UPDATE items SET qty = 4, version = 8 WHERE id = 1 AND version = 7
      -- 0 rows updated => someone else won; re-read and retry.
  Great when conflicts are rare. Common in web apps and APIs (ETags!).

FIXING LOST UPDATES -- THREE STANDARD ANSWERS
    1. Atomic operation:   UPDATE posts SET likes = likes + 1 WHERE id = 7
    2. Pessimistic lock:   SELECT likes FROM posts WHERE id=7 FOR UPDATE; ...
    3. Optimistic (CAS):   UPDATE ... WHERE id=7 AND version=<what I read>

DISTRIBUTED TRANSACTIONS (across services/shards) are much harder -- see
module 06 (two-phase commit and sagas).

The demo shows atomicity with sqlite3, then simulates a lost update with
threads and fixes it three ways.
"""

import sqlite3
import threading
import time


def atomicity_demo():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE accounts (name TEXT PRIMARY KEY, balance INTEGER CHECK (balance >= 0))")
    db.executemany("INSERT INTO accounts VALUES (?, ?)", [("alice", 100), ("bob", 50)])
    db.commit()

    def transfer(src, dst, amount):
        try:
            with db:   # BEGIN ... COMMIT, or ROLLBACK if an exception escapes
                db.execute("UPDATE accounts SET balance = balance + ? WHERE name = ?", (amount, dst))
                db.execute("UPDATE accounts SET balance = balance - ? WHERE name = ?", (amount, src))
            return "committed"
        except sqlite3.IntegrityError as e:
            return f"ROLLED BACK ({e})"

    show = lambda: dict(db.execute("SELECT name, balance FROM accounts"))
    print(f"  start                  : {show()}")
    print(f"  transfer 30 alice->bob : {transfer('alice', 'bob', 30)}  {show()}")
    print(f"  transfer 500 alice->bob: {transfer('alice', 'bob', 500)}")
    print(f"                           {show()}  <- bob's credit was undone too (atomic)")


class Row:
    """A counter row with a version number, protected like a DB row would be."""

    def __init__(self):
        self.likes = 0
        self.version = 0
        self.lock = threading.Lock()     # what SELECT ... FOR UPDATE would take
        self._latch = threading.Lock()   # internal: makes single statements atomic


def naive_increment(row):
    # read-modify-write in application code with no protection
    current = row.likes
    time.sleep(0)                        # yield: another transaction runs here
    row.likes = current + 1


def atomic_increment(row):
    # UPDATE posts SET likes = likes + 1  -- the DB does the read+write atomically
    with row._latch:
        row.likes += 1


def pessimistic_increment(row):
    # SELECT ... FOR UPDATE; UPDATE ...; COMMIT
    with row.lock:
        current = row.likes
        time.sleep(0)
        row.likes = current + 1


def optimistic_increment(row, stats):
    # read with version, then compare-and-set; retry on conflict
    while True:
        seen_likes, seen_version = row.likes, row.version
        time.sleep(0)
        with row._latch:                 # "UPDATE ... WHERE version = seen_version"
            if row.version == seen_version:
                row.likes = seen_likes + 1
                row.version += 1
                return
        stats["retries"] += 1            # 0 rows updated -> someone else won; retry


def run_concurrent(fn, n_threads=20, per_thread=200, **kw):
    row = Row()

    def worker():
        for _ in range(per_thread):
            fn(row, **kw)

    ts = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return row.likes, n_threads * per_thread


if __name__ == "__main__":
    print("=" * 76)
    print("PART 1: Atomicity -- a failed transfer is fully rolled back")
    print("=" * 76)
    atomicity_demo()

    print()
    print("=" * 76)
    print("PART 2: Lost updates -- 20 concurrent users each 'like' 200 times")
    print("=" * 76)
    got, expected = run_concurrent(naive_increment)
    print(f"  naive read-modify-write : {got:5d} / {expected}  <- {expected - got} likes LOST")
    got, expected = run_concurrent(atomic_increment)
    print(f"  atomic UPDATE x = x + 1 : {got:5d} / {expected}")
    got, expected = run_concurrent(pessimistic_increment)
    print(f"  pessimistic FOR UPDATE  : {got:5d} / {expected}")
    stats = {"retries": 0}
    got, expected = run_concurrent(optimistic_increment, stats=stats)
    print(f"  optimistic version check: {got:5d} / {expected}  ({stats['retries']} conflict retries)")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Money/inventory: ACID transactions in a relational DB; use atomic updates
  or SELECT ... FOR UPDATE; consider SERIALIZABLE for invariants across rows.
* Name the anomaly you're preventing ("two buyers could both see 1 item left
  and both check out -- a lost update / oversell").
* Optimistic concurrency (version numbers / ETags) for low-contention user
  edits; pessimistic locks or a single-writer queue for hot contended rows.
* Hot counters (likes, views) at massive scale: shard the counter or
  aggregate asynchronously instead of locking one row.
""")
