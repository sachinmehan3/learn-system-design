"""
LESSON 03.02 -- CONSISTENT HASHING
===================================

THE PROBLEM
-----------
You have N cache servers (or DB shards) and must decide which one holds key K.
The obvious approach:

        server = hash(K) % N

works until N changes. Add one server (N=4 -> 5) and ~80% of keys map to a
DIFFERENT server. For a cache, that means ~80% of requests suddenly miss and
hammer the database (a "cache avalanche"). For a database, it means moving
80% of your data. Unacceptable.

THE IDEA (Karger et al., 1997; made famous by Amazon Dynamo)
------------------------------------------------------------
1. Hash BOTH servers and keys onto the same circular space (a "ring"),
   e.g. 0 .. 2^32-1, wrapping around.
2. A key belongs to the first server found walking CLOCKWISE from the key's
   position.

                    0 / 2^32
                 .--- A ---.
               /             \\
         key1 *                * key2 -> goes to B
             |                  |
             C                  B
               \\             /
                 '--- * ----'
                     key3 -> goes to C

3. Add a server D: it lands at one point on the ring and takes over ONLY the
   keys between its predecessor and itself. Every other key stays put.
   Remove a server: only its keys move (to its successor).
   => On average only K/N keys move, instead of almost all of them.

VIRTUAL NODES (VNODES)
----------------------
With few servers, their random positions leave very uneven arcs: one server
might own 50% of the ring. Fix: place each physical server at MANY points
(e.g. 100-200 "virtual nodes": hash("A#0"), hash("A#1"), ...).
  + Load evens out (law of large numbers).
  + When a server dies, its load spreads across MANY servers, not just one
    neighbour (which might then overload and cascade).
  + Heterogeneous hardware: give a bigger server more vnodes.

REPLICATION ON THE RING
-----------------------
To store R copies, put the key on the first R DISTINCT physical servers
clockwise (the "preference list" in Dynamo/Cassandra). Skip vnodes that
belong to a server already chosen.

WHERE IT'S USED
---------------
Amazon Dynamo, Apache Cassandra, Riak, Memcached client libraries (ketama),
Discord, load balancers doing cache-affinity routing (Envoy "ring hash" and
"Maglev"), CDNs choosing which edge server caches an object.

ALTERNATIVES WORTH NAMING
-------------------------
* RENDEZVOUS (highest random weight) hashing: for each key, score every
  server with hash(key, server) and pick the max. Same minimal-movement
  property, no ring, O(N) per lookup.
* JUMP CONSISTENT HASH (Google): tiny, fast, perfectly even, but servers must
  be numbered 0..N-1 (only add/remove at the end) -- good for shards.
* Fixed number of PARTITIONS (e.g. 1024) mapped to nodes via a lookup table:
  what Kafka, Elasticsearch, Redis Cluster (16384 slots), and Couchbase do.
  Moving a partition = updating the table. Simple and very common.
"""

import bisect
import hashlib
from collections import Counter


def h(value: str) -> int:
    """Stable 32-bit hash. (Python's built-in hash() is randomised per process!)"""
    return int.from_bytes(hashlib.md5(value.encode()).digest()[:4], "big")


class ConsistentHashRing:
    def __init__(self, vnodes_per_server=100):
        self.vnodes = vnodes_per_server
        self.ring = []        # sorted list of vnode positions
        self.owner = {}       # position -> physical server name

    def add_server(self, server: str, weight: int = 1):
        for i in range(self.vnodes * weight):
            pos = h(f"{server}#vnode{i}")
            bisect.insort(self.ring, pos)
            self.owner[pos] = server

    def remove_server(self, server: str):
        self.ring = [p for p in self.ring if self.owner[p] != server]
        self.owner = {p: s for p, s in self.owner.items() if s != server}

    def get_server(self, key: str) -> str:
        """First vnode clockwise from the key's position (wrapping around)."""
        if not self.ring:
            raise RuntimeError("empty ring")
        idx = bisect.bisect_right(self.ring, h(key)) % len(self.ring)
        return self.owner[self.ring[idx]]

    def get_replicas(self, key: str, n: int):
        """Preference list: first n DISTINCT physical servers clockwise."""
        result = []
        idx = bisect.bisect_right(self.ring, h(key))
        for step in range(len(self.ring)):
            server = self.owner[self.ring[(idx + step) % len(self.ring)]]
            if server not in result:
                result.append(server)
                if len(result) == n:
                    break
        return result


class ModuloHashing:
    """The naive approach, for comparison."""

    def __init__(self, servers):
        self.servers = list(servers)

    def get_server(self, key):
        return self.servers[h(key) % len(self.servers)]


def moved_fraction(before, after):
    return sum(1 for k in before if before[k] != after[k]) / len(before)


def spread(assignments):
    counts = Counter(assignments.values())
    ideal = len(assignments) / len(counts)
    return ", ".join(f"{s}={c / ideal:4.2f}x" for s, c in sorted(counts.items()))


if __name__ == "__main__":
    keys = [f"user:{i}" for i in range(20_000)]
    servers = ["cache-A", "cache-B", "cache-C", "cache-D"]

    print("=" * 76)
    print("PART 1: Adding a 5th server -- how many keys move?")
    print("=" * 76)
    mod = ModuloHashing(servers)
    before = {k: mod.get_server(k) for k in keys}
    mod.servers.append("cache-E")
    after = {k: mod.get_server(k) for k in keys}
    print(f"  hash % N          : {moved_fraction(before, after):5.1%} of keys moved")

    ring = ConsistentHashRing(vnodes_per_server=150)
    for s in servers:
        ring.add_server(s)
    before = {k: ring.get_server(k) for k in keys}
    ring.add_server("cache-E")
    after = {k: ring.get_server(k) for k in keys}
    print(f"  consistent hashing: {moved_fraction(before, after):5.1%} of keys moved "
          f"(ideal = 1/5 = 20%)")
    movers = Counter(before[k] for k in keys if before[k] != after[k])
    print(f"  keys moved TO cache-E came FROM: {dict(movers)}  <- taken evenly from everyone")

    print()
    print("=" * 76)
    print("PART 2: Why virtual nodes matter (load relative to perfectly even = 1.00x)")
    print("=" * 76)
    for v in (1, 10, 100, 500):
        r = ConsistentHashRing(vnodes_per_server=v)
        for s in servers:
            r.add_server(s)
        print(f"  {v:3d} vnode(s)/server: {spread({k: r.get_server(k) for k in keys})}")

    print()
    print("=" * 76)
    print("PART 3: A server dies -- its load spreads over all survivors")
    print("=" * 76)
    ring.remove_server("cache-B")
    after_death = {k: ring.get_server(k) for k in keys}
    orphaned = Counter(after_death[k] for k in keys if after[k] == "cache-B")
    print(f"  cache-B's keys were re-homed to: {dict(orphaned)}")

    print()
    print("=" * 76)
    print("PART 4: Replication -- preference list of 3 distinct servers")
    print("=" * 76)
    for k in ("user:1", "user:42", "cart:777"):
        print(f"  {k:9s} -> {ring.get_replicas(k, 3)}")

    print()
    print("=" * 76)
    print("PART 5: Weighted servers (cache-F has 2x RAM -> 2x vnodes)")
    print("=" * 76)
    ring.add_server("cache-F", weight=2)
    print(f"  {spread({k: ring.get_server(k) for k in keys})}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "I'll partition keys across cache/DB nodes with consistent hashing and
   ~100+ virtual nodes per server, so adding or losing a node moves only ~1/N
   of the keys and spreads the load evenly."
* Replicate to the next R distinct nodes on the ring (Dynamo-style).
* Mention the alternative: a fixed number of logical partitions mapped to
  nodes (Redis Cluster's 16384 hash slots, Kafka partitions).
* Consistent hashing doesn't solve HOT KEYS (one celebrity key gets 1M QPS) --
  that needs replication of the hot key or key splitting (see module 04).
""")
