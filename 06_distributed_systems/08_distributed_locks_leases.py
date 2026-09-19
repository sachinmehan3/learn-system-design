"""
LESSON 06.08 -- DISTRIBUTED LOCKS, LEASES, AND FENCING TOKENS
==============================================================

WHY DISTRIBUTED LOCKS?
----------------------
Ensure only ONE process across many machines does something at a time:
  * only one worker processes a given job / sends a given email,
  * only one scheduler instance runs the nightly cron,
  * only one node acts as leader for a partition,
  * mutual exclusion on an external resource (a file in S3, a legacy system).
Two different goals (Martin Kleppmann's distinction):
  * EFFICIENCY: avoid doing work twice. Occasional double work is OK.
    -> A simple Redis lock (SET key val NX PX 30000) is fine.
  * CORRECTNESS: two holders would corrupt data. Needs a consensus-backed
    lock (ZooKeeper, etcd) AND FENCING TOKENS.

LOCKS MUST EXPIRE: LEASES
-------------------------
If a lock holder crashes, the lock must not be held forever. So locks are
LEASES: valid for a TTL (e.g. 30 s); the holder must renew before expiry.

THE PROBLEM WITH LEASES: PAUSED PROCESSES
-----------------------------------------
    1. Client 1 acquires the lease (TTL 10 s).
    2. Client 1 hits a 15-second GC pause (or VM migration, or network delay).
    3. The lease expires; Client 2 acquires it and writes to storage.
    4. Client 1 wakes up, still BELIEVES it holds the lock, and writes too.
       -> CORRUPTION. Client 1 can't detect this: checking "do I still hold
          it?" right before writing is racy (it could pause right after the check).

THE FIX: FENCING TOKENS
-----------------------
The lock service returns a monotonically increasing TOKEN with each grant
(ZooKeeper's zxid, etcd's revision). Every write to the protected resource
includes the token, and the RESOURCE rejects any token lower than the
highest it has seen. Client 1's late write (token 33) is rejected because
Client 2 already wrote with token 34. (Same idea as Raft terms / DB failover
epochs.)

IMPLEMENTATIONS
---------------
* Redis: SET lock:job42 <random-id> NX PX 30000; release only if the value
  matches your id (Lua script, compare-and-delete). Single Redis node = SPOF
  and async replication can lose locks on failover. REDLOCK (majority of 5
  independent Redis nodes) is debated -- it relies on timing assumptions and
  gives no fencing token. Fine for efficiency, questionable for correctness.
* ZooKeeper: ephemeral sequential znodes; lowest sequence number holds the
  lock; others watch their predecessor (no herd effect). Session expiry
  releases the lock automatically.
* etcd: leases + revisions (usable as fencing tokens); Kubernetes leader
  election uses this.
* Database: SELECT ... FOR UPDATE, or advisory locks (pg_advisory_lock);
  fine when all parties already share one DB.

ALTERNATIVES TO LOCKING
  * Make operations IDEMPOTENT so double execution is harmless.
  * Single-writer designs: partition work so each key has exactly one owner
    (e.g. Kafka partition -> one consumer).
  * Optimistic concurrency (compare-and-set with versions).
"""


class LockService:
    """A lease-based lock service that issues fencing tokens (like ZooKeeper/etcd)."""

    def __init__(self):
        self.holder = None
        self.expires_at = 0
        self.token = 33

    def acquire(self, client, now, ttl):
        if self.holder is None or now >= self.expires_at:
            self.token += 1
            self.holder, self.expires_at = client, now + ttl
            return self.token
        return None


class Storage:
    def __init__(self, check_fencing):
        self.check_fencing = check_fencing
        self.max_token_seen = 0
        self.data = []

    def write(self, client, token, value):
        if self.check_fencing and token < self.max_token_seen:
            return f"REJECTED (token {token} < {self.max_token_seen})"
        self.max_token_seen = max(self.max_token_seen, token)
        self.data.append((client, value))
        return "ok"


def scenario(check_fencing):
    locks, storage = LockService(), Storage(check_fencing)
    t1 = locks.acquire("client1", now=0, ttl=10)
    print(f"    t=0   client1 acquires lease (ttl 10s), token={t1}")
    print("    t=1   client1 enters a long GC pause...")
    t2 = locks.acquire("client2", now=12, ttl=10)
    print(f"    t=12  lease expired; client2 acquires it, token={t2}")
    print(f"    t=13  client2 writes 'B': {storage.write('client2', t2, 'B')}")
    print(f"    t=16  client1 wakes up, thinks it still holds the lock, writes 'A': "
          f"{storage.write('client1', t1, 'A')}")
    print(f"    storage contents: {storage.data}")


if __name__ == "__main__":
    print("=" * 76)
    print("A paused lock holder -- WITHOUT fencing tokens")
    print("=" * 76)
    scenario(check_fencing=False)
    print("    -> both clients wrote while 'holding' the lock: mutual exclusion violated.")

    print()
    print("=" * 76)
    print("The same timeline -- WITH fencing tokens checked by storage")
    print("=" * 76)
    scenario(check_fencing=True)
    print("    -> the stale holder's write was rejected by the resource itself.")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Distinguish efficiency locks (Redis SET NX PX is fine) from correctness
  locks (ZooKeeper/etcd + fencing tokens).
* Locks are leases with TTLs; any holder can be paused past its lease, so the
  protected resource must enforce fencing tokens.
* Often better: avoid locks via idempotency, single-writer partitioning, or
  optimistic concurrency control.
""")
