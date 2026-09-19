"""
LESSON 06.02 -- TIME AND ORDERING: CLOCK SKEW, LAMPORT CLOCKS, VECTOR CLOCKS, CRDTs
===================================================================================

WHY TIME IS HARD IN DISTRIBUTED SYSTEMS
---------------------------------------
Each machine has its own clock (a quartz crystal) that DRIFTS. NTP syncs them,
but typically only to within a few milliseconds -- and sometimes far worse
(misconfigured NTP, VM pauses, leap seconds). Clocks can even jump BACKWARDS.

So "which write happened last?" can't be reliably answered with wall-clock
timestamps from different machines.

LAST-WRITE-WINS (LWW) with timestamps -- used by Cassandra and many others --
silently DROPS writes when clocks are skewed: a write that really happened
later but carries an earlier timestamp loses. (Demo below.)

LOGICAL CLOCKS
--------------
Instead of measuring real time, track CAUSALITY: "A happened-before B" if
A could have influenced B (same process, earlier; or A is a message send
and B its receive; or transitively).

1) LAMPORT CLOCK: each node keeps a counter.
     - increment before each event;
     - attach it to messages; on receive: counter = max(local, received) + 1.
   Guarantee: if A -> B (happened-before) then L(A) < L(B).
   Gives a TOTAL order consistent with causality (tie-break by node id).
   BUT: L(A) < L(B) does NOT mean A caused B -- can't detect concurrency.

2) VECTOR CLOCK: each node keeps a vector of counters, one per node.
     - on local event: increment your own entry;
     - on receive: element-wise max, then increment your own entry.
   Compare two vectors V1, V2:
     V1 <= V2 element-wise  => V1 happened-before V2 (V2 supersedes V1)
     neither <= the other   => CONCURRENT -> a real CONFLICT to resolve.
   Used by Dynamo (for shopping carts: return both "siblings" to the client
   to merge), Riak. Cost: vector grows with number of nodes/clients.

(Google Spanner sidesteps this with TrueTime: GPS + atomic clocks give a
bounded uncertainty interval, and Spanner WAITS out the uncertainty before
committing. CockroachDB uses Hybrid Logical Clocks.)

CRDTs -- CONFLICT-FREE REPLICATED DATA TYPES
--------------------------------------------
Data structures designed so replicas can be updated independently and ALWAYS
merge to the same result, in any order, without coordination.
  * G-COUNTER: each node has its own counter slot; value = sum; merge =
    element-wise max. (Like counts, view counts across regions.)
  * PN-COUNTER: two G-counters (increments, decrements).
  * OR-SET, LWW-register, sequence CRDTs (collaborative text editing: Figma,
    Apple Notes, Automerge, Yjs).
Great for AP systems and multi-leader / offline-first apps.
"""


# ---------------------------------------------------------------------------
# 1) LWW with skewed clocks loses data
# ---------------------------------------------------------------------------
def lww_demo():
    print("  Node A's clock is correct. Node B's clock runs 5 seconds SLOW.")
    writes = [
        # (real time, node, clock skew, value)
        (100.0, "A", 0.0, "cart=[book]"),
        (102.0, "B", -5.0, "cart=[book, pen]"),   # really happened LATER
    ]
    store = None
    for real, node, skew, value in writes:
        ts = real + skew
        print(f"    real t={real}: node {node} writes {value!r} with timestamp {ts}")
        if store is None or ts > store[0]:
            store = (ts, value)
    print(f"  LWW result: {store[1]!r}  <- the later write (pen) was silently DISCARDED")


# ---------------------------------------------------------------------------
# 2) Lamport clocks
# ---------------------------------------------------------------------------
class LamportNode:
    def __init__(self, name):
        self.name, self.clock = name, 0

    def local_event(self, what):
        self.clock += 1
        print(f"    {self.name}: {what:26s} L={self.clock}")
        return self.clock

    def send(self, what):
        return self.local_event(f"send '{what}'")

    def receive(self, msg_clock, what):
        self.clock = max(self.clock, msg_clock) + 1
        print(f"    {self.name}: {'recv ' + repr(what):26s} L={self.clock}  (max(local, {msg_clock}) + 1)")
        return self.clock


# ---------------------------------------------------------------------------
# 3) Vector clocks
# ---------------------------------------------------------------------------
def vc_increment(vc, node):
    vc = dict(vc)
    vc[node] = vc.get(node, 0) + 1
    return vc


def vc_merge(a, b):
    return {k: max(a.get(k, 0), b.get(k, 0)) for k in set(a) | set(b)}


def vc_compare(a, b):
    keys = set(a) | set(b)
    a_le_b = all(a.get(k, 0) <= b.get(k, 0) for k in keys)
    b_le_a = all(b.get(k, 0) <= a.get(k, 0) for k in keys)
    if a_le_b and b_le_a:
        return "equal"
    if a_le_b:
        return "a happened-before b"
    if b_le_a:
        return "b happened-before a"
    return "CONCURRENT (conflict!)"


def vector_clock_demo():
    # A shopping cart replicated on servers X and Y (Dynamo paper's example).
    v1 = vc_increment({}, "X")
    print(f"    client adds book via X          -> {{book}}        vc={v1}")
    v2 = vc_increment(v1, "X")
    print(f"    client adds pen via X           -> {{book, pen}}   vc={v2}")
    print(f"      compare v1 vs v2: {vc_compare(v1, v2)} -> v2 safely replaces v1")
    # Network partition: two clients update different replicas starting from v2.
    v3 = vc_increment(v2, "Y")
    print(f"    client1 adds mug via Y          -> {{book, pen, mug}} vc={v3}")
    v4 = vc_increment(v2, "X")
    print(f"    client2 removes pen via X       -> {{book}}        vc={v4}")
    print(f"      compare v3 vs v4: {vc_compare(v3, v4)}")
    merged = vc_increment(vc_merge(v3, v4), "X")
    print(f"    application merges siblings     -> {{book, mug}}   vc={merged}")
    print("      (Dynamo returned both versions to the cart service to merge;")
    print("       LWW would have silently dropped one of them.)")


# ---------------------------------------------------------------------------
# 4) G-Counter CRDT
# ---------------------------------------------------------------------------
class GCounter:
    def __init__(self, node):
        self.node, self.slots = node, {}

    def increment(self, n=1):
        self.slots[self.node] = self.slots.get(self.node, 0) + n

    def value(self):
        return sum(self.slots.values())

    def merge(self, other):
        for k, v in other.slots.items():
            self.slots[k] = max(self.slots.get(k, 0), v)


if __name__ == "__main__":
    print("=" * 76)
    print("PART 1: Last-write-wins with clock skew")
    print("=" * 76)
    lww_demo()

    print()
    print("=" * 76)
    print("PART 2: Lamport clocks -- causality-respecting order without real time")
    print("=" * 76)
    a, b = LamportNode("A"), LamportNode("B")
    a.local_event("write x=1")
    m = a.send("x=1")
    b.local_event("unrelated work")
    b.local_event("more unrelated work")
    b.local_event("even more work")
    b.receive(m, "x=1")
    a.local_event("write x=2")
    print("  -> The receive (L=4) is ordered after the send (L=2): causality preserved.")
    print("     But A's 'write x=2' (L=3) vs B's work (L=3): Lamport can't tell they're concurrent.")

    print()
    print("=" * 76)
    print("PART 3: Vector clocks detect concurrent writes (conflicts)")
    print("=" * 76)
    vector_clock_demo()

    print()
    print("=" * 76)
    print("PART 4: G-Counter CRDT -- 'likes' counted in 3 regions, merged in any order")
    print("=" * 76)
    us, eu, asia = GCounter("us"), GCounter("eu"), GCounter("asia")
    for _ in range(5):
        us.increment()
    for _ in range(3):
        eu.increment()
    asia.increment(7)
    print(f"  local values before sync: us={us.value()} eu={eu.value()} asia={asia.value()}")
    us.merge(eu); us.merge(asia)
    asia.merge(us); eu.merge(asia)
    eu.merge(us)            # merging twice is harmless (idempotent)
    print(f"  after gossip merges     : us={us.value()} eu={eu.value()} asia={asia.value()}  "
          "(all converge to 15)")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Never order events across machines by wall-clock time alone; clocks skew.
* LWW is simple but loses concurrent writes -- say so if you use it.
* Vector clocks detect conflicts; CRDTs avoid them by construction
  (counters, sets, collaborative text).
* Spanner's TrueTime / hybrid logical clocks for globally ordered transactions.
""")
