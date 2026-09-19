"""
CASE STUDY 06 -- DESIGN A DISTRIBUTED KEY-VALUE STORE (Dynamo / Cassandra)
==========================================================================
This question is a tour of module 06: it ties partitioning, replication,
quorums, versioning, failure handling and repair into one system.

STEP 1 -- REQUIREMENTS
----------------------
Functional: put(key, value), get(key). Values small (< 10 KB).
Non-functional: huge scale (petabytes, millions of ops/s), always writable
("add to cart must never fail" -- Amazon's motivation), low latency,
tunable consistency, automatic scaling, no single point of failure.
=> An AP (PA/EL) design.

STEP 2 -- CORE TECHNIQUES (each maps to a lesson)
-------------------------------------------------
  Problem                        Technique                                Lesson
  -----------------------------  ---------------------------------------  -------------
  Partition data                 Consistent hashing + virtual nodes      03.02
  High availability              Replicate to next N nodes on the ring   03.02 / 05.04
  Tunable consistency            Quorum: R + W > N                       06.01
  Concurrent writes              Vector clocks (or LWW timestamps)       06.02
  Temporary failures             Sloppy quorum + HINTED HANDOFF          06.01
  Permanent failures / drift     Anti-entropy with MERKLE TREES          06.07
  Membership, failure detection  GOSSIP protocol                         06.05
  Storage engine                 LSM-tree (commit log + memtable +       05.03
                                 SSTables + Bloom filters + compaction)

STEP 3 -- ARCHITECTURE
----------------------
Every node is identical (no leader, no master): any node can act as the
COORDINATOR for a request.
  PUT(k, v): coordinator hashes k -> finds the preference list (first N
    healthy distinct nodes clockwise) -> sends the write to all N -> returns
    success after W acks. If a target is down, the write goes to the next
    healthy node with a HINT ("this belongs to node X"); that node hands it
    back when X recovers (hinted handoff).
  GET(k): coordinator asks the N replicas, waits for R responses, returns the
    newest version; if replicas disagree, it repairs stale ones (read repair).

STEP 4 -- DEEP DIVES
--------------------
* N=3, W=2, R=2 is the common default; W=1 for max write availability.
* Conflict resolution: LWW (simple, can lose writes) vs vector clocks with
  siblings returned to the client (Dynamo) vs CRDTs.
* Rebalancing when nodes join: only neighbouring ranges move (consistent hashing).
* Write path in each node: append to commit log -> memtable -> flush to SSTable.
* Multi-datacenter: replicate across DCs; LOCAL_QUORUM for latency.

The implementation below is a mini-Dynamo: ring with vnodes, N/R/W quorum,
LWW versioning, hinted handoff, and read repair, with a node failure.
"""

import bisect
import hashlib
import itertools


def h(s):
    return int.from_bytes(hashlib.md5(s.encode()).digest()[:4], "big")


class StorageNode:
    def __init__(self, name):
        self.name = name
        self.up = True
        self.data = {}              # key -> (version, value)
        self.hints = []             # (intended_node, key, version, value)

    def write(self, key, version, value):
        if version > self.data.get(key, (0, None))[0]:
            self.data[key] = (version, value)

    def read(self, key):
        return self.data.get(key, (0, None))


class MiniDynamo:
    def __init__(self, node_names, n=3, r=2, w=2, vnodes=50):
        self.nodes = {name: StorageNode(name) for name in node_names}
        self.n, self.r, self.w = n, r, w
        self.ring = sorted((h(f"{name}#{i}"), name) for name in node_names for i in range(vnodes))
        self.positions = [p for p, _ in self.ring]
        self.clock = itertools.count(1)       # stand-in for timestamps (LWW)
        self.events = []

    def preference_list(self, key, healthy_only=False):
        """Walk clockwise collecting distinct nodes."""
        out, idx = [], bisect.bisect(self.positions, h(key))
        for i in range(len(self.ring)):
            name = self.ring[(idx + i) % len(self.ring)][1]
            if name in out or (healthy_only and not self.nodes[name].up):
                continue
            out.append(name)
            if len(out) == (self.n if not healthy_only else self.n):
                break
        return out

    def put(self, key, value):
        version = next(self.clock)
        home = self.preference_list(key)                    # where the key SHOULD live
        acks = 0
        spares = [x for x in self.preference_list(key + "#spare-walk", healthy_only=True) if x not in home]
        for target in home:
            node = self.nodes[target]
            if node.up:
                node.write(key, version, value)
                acks += 1
            elif spares:
                # Sloppy quorum: a healthy stand-in stores it with a HINT.
                stand_in = self.nodes[spares.pop(0)]
                stand_in.write(key, version, value)
                stand_in.hints.append((target, key, version, value))
                acks += 1
                self.events.append(f"hinted handoff: {stand_in.name} holds '{key}' for {target}")
        return "OK" if acks >= self.w else "FAILED"

    def get(self, key):
        home = self.preference_list(key)
        responses = [(name, self.nodes[name].read(key)) for name in home if self.nodes[name].up]
        if len(responses) < self.r:
            return "ERROR: not enough replicas"
        newest = max(v for _, v in responses)
        for name, v in responses:                           # read repair
            if v < newest:
                self.nodes[name].write(key, *newest)
                self.events.append(f"read repair: updated '{key}' on {name}")
        return newest[1]

    def recover(self, name):
        """Node comes back; stand-ins deliver their hints to it."""
        self.nodes[name].up = True
        for node in self.nodes.values():
            keep = []
            for intended, key, version, value in node.hints:
                if intended == name:
                    self.nodes[name].write(key, version, value)
                    self.events.append(f"hint delivered: {node.name} -> {name} ('{key}')")
                else:
                    keep.append((intended, key, version, value))
            node.hints = keep


if __name__ == "__main__":
    kv = MiniDynamo([f"node{i}" for i in range(6)], n=3, r=2, w=2)

    print("=" * 78)
    print("Normal operation: N=3, R=2, W=2 on a 6-node ring")
    print("=" * 78)
    for k, v in (("cart:alice", ["book"]), ("cart:bob", ["pen"]), ("profile:carol", {"city": "Oslo"})):
        print(f"  put {k:14s} -> {kv.put(k, v)}   replicas: {kv.preference_list(k)}")
    print(f"  get cart:alice -> {kv.get('cart:alice')}")

    victim = kv.preference_list("cart:alice")[0]
    print()
    print("=" * 78)
    print(f"{victim} (a replica of cart:alice) goes DOWN; writes keep succeeding")
    print("=" * 78)
    kv.nodes[victim].up = False
    print(f"  put cart:alice ['book','lamp'] -> {kv.put('cart:alice', ['book', 'lamp'])}")
    print(f"  get cart:alice -> {kv.get('cart:alice')}   (still readable: R=2 of the 2 live replicas)")
    for e in kv.events:
        print("   ", e)
    kv.events.clear()

    print()
    print("=" * 78)
    print(f"{victim} recovers: hinted handoff returns the missed write")
    print("=" * 78)
    print(f"  before recovery {victim} has: {kv.nodes[victim].read('cart:alice')}")
    kv.recover(victim)
    print(f"  after recovery  {victim} has: {kv.nodes[victim].read('cart:alice')}")
    for e in kv.events:
        print("   ", e)
    kv.events.clear()

    print()
    print("=" * 78)
    print("A replica silently missed a write -> read repair fixes it")
    print("=" * 78)
    replicas = kv.preference_list("cart:bob")
    kv.nodes[replicas[2]].data["cart:bob"] = (0, ["STALE"])
    print(f"  {replicas[2]} holds stale value; get cart:bob -> {kv.get('cart:bob')}")
    kv.get("cart:bob")
    for e in kv.events:
        print("   ", e)

    print("""
FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How do you handle concurrent writes to the same key? (LWW vs vector clocks
  + client-side merge vs CRDTs; trade-off: simplicity vs lost updates)
* What happens when a node is gone for good? (Merkle-tree anti-entropy streams
  missing ranges to the replacement; ring rebalances only neighbours)
* How do clients find the coordinator? (any node, or a partition-aware client
  library that goes straight to a replica)
* How would you make it strongly consistent? (R+W>N plus no sloppy quorum,
  or a consensus-based design like etcd/Spanner -- at the cost of availability)
""")
