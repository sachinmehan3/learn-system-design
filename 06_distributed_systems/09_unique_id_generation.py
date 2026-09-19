"""
LESSON 06.09 -- GENERATING UNIQUE IDs IN A DISTRIBUTED SYSTEM
==============================================================

With one database, AUTO_INCREMENT gives unique, ordered IDs. With many
shards/servers generating IDs concurrently, you need another approach.
Classic interview question: "Design a unique ID generator."

REQUIREMENTS TO CLARIFY
  * Unique (always). Numeric? Fits in 64 bits?
  * Sortable by time? (Very useful: "latest posts" = ORDER BY id DESC,
    and B-tree inserts stay append-only, which is fast.)
  * Throughput (10k+/s per node?), availability (no single point of failure).
  * Unguessable? (Sequential IDs leak business volume and enable enumeration
    attacks on public URLs.)

OPTIONS
-------
1) UUID v4 (128 random bits)
   + Generated anywhere, no coordination, practically never collide.
   - 128 bits (big indexes); NOT time-ordered -> random inserts scatter across
     the B-tree index (page splits, poor cache locality).
   UUID v7 (2024, RFC 9562) fixes ordering: 48-bit ms timestamp + random bits.

2) DATABASE AUTO_INCREMENT with offsets (Flickr "ticket servers")
   Server 1 generates 1, 3, 5...; server 2 generates 2, 4, 6...
   (auto_increment_increment=2, offset=1/2).
   + Simple, numeric, compact.
   - Adding servers is awkward; ticket DBs are a dependency; not time-sortable
     across servers.

3) RANGE / BLOCK ALLOCATION
   A central service hands each app server a block of 1000 IDs; the server
   allocates from memory until the block runs out.
   + Very fast, compact numbers. - Central service (make it HA); gaps on
   crash (usually fine).

4) TWITTER SNOWFLAKE (the standard interview answer) -- 64-bit integer:
      | 1 bit sign | 41 bits timestamp (ms since custom epoch) |
      | 10 bits machine id | 12 bits sequence |
   * 41 bits of milliseconds ~ 69 years.
   * 10 bits -> 1024 machines (often split as 5 datacenter + 5 worker bits).
   * 12 bits -> 4096 IDs per millisecond PER MACHINE (~4M/s/machine).
   + No coordination at generation time; roughly time-sortable; 64-bit.
   - Needs unique machine IDs (assign via ZooKeeper/etcd or config).
   - CLOCK MOVING BACKWARDS could create duplicates: refuse to generate (wait
     or error) until the clock catches up. Implemented below.
   Variants: Instagram (shard id embedded, generated in Postgres), Sonyflake,
   Discord, ULID (128-bit, timestamp + random, lexicographically sortable).
"""

import threading
import time
import uuid


class Snowflake:
    EPOCH_MS = 1_704_067_200_000          # custom epoch: 2024-01-01 UTC -> more years of headroom
    MACHINE_BITS = 10
    SEQUENCE_BITS = 12
    MAX_MACHINE = (1 << MACHINE_BITS) - 1
    MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1

    def __init__(self, machine_id, clock=lambda: int(time.time() * 1000)):
        if not 0 <= machine_id <= self.MAX_MACHINE:
            raise ValueError("machine id out of range")
        self.machine_id = machine_id
        self.clock = clock
        self.last_ms = -1
        self.sequence = 0
        self.lock = threading.Lock()

    def next_id(self):
        with self.lock:
            now = self.clock()
            if now < self.last_ms:
                # Clock went backwards (NTP adjustment). Generating now could repeat IDs.
                raise RuntimeError(f"clock moved backwards by {self.last_ms - now} ms; refusing")
            if now == self.last_ms:
                self.sequence = (self.sequence + 1) & self.MAX_SEQUENCE
                if self.sequence == 0:                 # 4096 IDs used this ms: wait for next ms
                    while now <= self.last_ms:
                        now = self.clock()
            else:
                self.sequence = 0
            self.last_ms = now
            return ((now - self.EPOCH_MS) << (self.MACHINE_BITS + self.SEQUENCE_BITS)) \
                | (self.machine_id << self.SEQUENCE_BITS) | self.sequence

    @classmethod
    def parse(cls, sid):
        seq = sid & cls.MAX_SEQUENCE
        machine = (sid >> cls.SEQUENCE_BITS) & cls.MAX_MACHINE
        ts = (sid >> (cls.MACHINE_BITS + cls.SEQUENCE_BITS)) + cls.EPOCH_MS
        return {"timestamp_ms": ts, "machine": machine, "sequence": seq,
                "time": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts / 1000))}


class TicketServer:
    """Flickr-style: N servers each hand out start, start+N, start+2N..."""

    def __init__(self, offset, step):
        self.next, self.step = offset, step

    def next_id(self):
        v = self.next
        self.next += self.step
        return v


if __name__ == "__main__":
    print("=" * 76)
    print("UUID v4: random, unordered, 128 bits")
    print("=" * 76)
    for _ in range(3):
        print("  ", uuid.uuid4())

    print()
    print("=" * 76)
    print("Ticket servers with offsets (2 servers)")
    print("=" * 76)
    s1, s2 = TicketServer(1, 2), TicketServer(2, 2)
    print("   server1:", [s1.next_id() for _ in range(5)], " server2:", [s2.next_id() for _ in range(5)])

    print()
    print("=" * 76)
    print("Snowflake IDs")
    print("=" * 76)
    gen_a, gen_b = Snowflake(machine_id=1), Snowflake(machine_id=2)
    ids = [gen_a.next_id(), gen_b.next_id(), gen_a.next_id()]
    for i in ids:
        print(f"   {i:20d}  = {Snowflake.parse(i)}")

    # Throughput + uniqueness across 4 threads on 4 "machines".
    gens = [Snowflake(machine_id=m) for m in range(4)]
    per_gen = [[] for _ in gens]            # one output list per "machine"

    def worker(i):
        per_gen[i].extend(gens[i].next_id() for _ in range(50_000))

    t = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(len(gens))]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    elapsed = time.perf_counter() - t
    results = [x for ids in per_gen for x in ids]
    print(f"\n   generated {len(results):,} IDs on 4 machines in {elapsed:.2f}s; "
          f"unique: {len(set(results)) == len(results)}")
    print(f"   each machine's IDs are strictly increasing: "
          f"{all(all(a < b for a, b in zip(s, s[1:])) for s in per_gen)}")

    print("\n   Clock moving backwards:")
    fake_now = [1_750_000_000_000]
    g = Snowflake(machine_id=9, clock=lambda: fake_now[0])
    g.next_id()
    fake_now[0] -= 5                       # NTP steps the clock back 5 ms
    try:
        g.next_id()
    except RuntimeError as e:
        print(f"   -> {e} (instead of risking duplicate IDs)")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Default answer: Snowflake-style 64-bit IDs = timestamp | machine id | sequence.
  Time-sortable, no coordination per ID, ~4M IDs/s per machine.
* Machine IDs assigned via ZooKeeper/etcd (or from the pod ordinal).
* Handle clock skew: refuse/wait if the clock goes backwards.
* UUIDv4 when you don't need ordering or compactness; UUIDv7/ULID when you
  want sortable 128-bit IDs without machine-id coordination.
""")
