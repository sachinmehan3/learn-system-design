"""
LESSON 01.04 -- THE CAP THEOREM AND PACELC
==========================================

THE CAP THEOREM (Brewer, 2000; proved by Gilbert & Lynch, 2002)
---------------------------------------------------------------
A distributed data store can't simultaneously guarantee all three of:

  C -- CONSISTENCY (specifically LINEARIZABILITY): every read sees the most
      recent write. The system behaves as if there's one copy of the data.
  A -- AVAILABILITY: every request to a non-failed node gets a (non-error)
      response -- without waiting forever.
  P -- PARTITION TOLERANCE: the system keeps working even when the network
      drops or delays messages between nodes.

THE COMMON MISREADING: "pick any 2 of 3". That's wrong in practice.
Network partitions WILL happen (switch failures, cable cuts, GC pauses that
look like partitions, misconfigured firewalls). You don't get to "opt out" of P.

So the real statement is:
    WHEN A PARTITION HAPPENS, you must choose between C and A.

  * CP system: during a partition, nodes that can't confirm they have the
    latest data REFUSE requests (return an error / time out). Correct but
    partially unavailable.
    Examples: ZooKeeper, etcd, HBase, Spanner, a SQL DB with synchronous
    replication, MongoDB with majority writes/reads.
    Use for: bank balances, inventory counts, leader election, locks,
    anything where a stale read causes real damage.

  * AP system: during a partition, every node keeps answering with whatever
    data it has -- possibly stale -- and reconciles later.
    Examples: Cassandra, DynamoDB (default), Riak, CouchDB, DNS.
    Use for: social feeds, likes counters, shopping carts, product catalogues,
    anything where "slightly stale" is better than "down".

Note: it's often a per-operation choice, not per-database. Cassandra and
DynamoDB let you choose consistency levels per request.

PACELC (Abadi, 2010) -- THE MORE USEFUL VERSION
-----------------------------------------------
CAP only talks about what happens DURING a partition, which is rare. PACELC
adds what happens the rest of the time:

    if Partition:   choose Availability or Consistency     (the CAP part)
    Else:           choose Latency     or Consistency      (the everyday part)

Why is there a latency trade-off with no partition? Because strong consistency
requires coordination -- waiting for replicas (maybe in other regions) to
acknowledge before replying. Skipping that wait = lower latency but possibly
stale reads.

    PA/EL: Cassandra, DynamoDB  -> available in partition; fast otherwise
    PC/EC: Spanner, etcd, HBase -> consistent always; pays latency
    PA/EC: MongoDB (typical configs) -- varies by settings
    PC/EL: rare (Yahoo PNUTS)

In an interview, PACELC is a great way to show depth:
  "Normally we're trading latency for consistency -- the replica read might be
   a few ms stale -- and during a partition we'd rather serve stale data than
   error, so this is a PA/EL design."

The simulation below builds a 2-node replicated key-value store, cuts the
network between the nodes, and shows exactly how CP and AP modes behave.
"""


class Node:
    """One replica holding a copy of the data, plus a version number per key."""

    def __init__(self, name):
        self.name = name
        self.data = {}            # key -> (value, version)

    def local_write(self, key, value, version):
        current = self.data.get(key, (None, 0))
        if version > current[1]:  # only accept newer versions
            self.data[key] = (value, version)

    def local_read(self, key):
        return self.data.get(key, (None, 0))


class ReplicatedStore:
    """
    Two replicas. A client talks to one of them. On write, the node tries to
    replicate to its peer.

      mode="CP": a write/read must reach BOTH nodes (the "majority" of 2 is 2).
                 If the peer is unreachable -> reject the request.
      mode="AP": write/read locally, replicate if possible, queue changes
                 ("hinted handoff") and sync when the partition heals.
    """

    def __init__(self, mode):
        assert mode in ("CP", "AP")
        self.mode = mode
        self.nodes = {"A": Node("A"), "B": Node("B")}
        self.partitioned = False
        self.version = 0
        self.pending = []   # writes that couldn't replicate during the partition

    def _peer(self, name):
        return self.nodes["B" if name == "A" else "A"]

    def write(self, via, key, value):
        self.version += 1   # simplified global version (a real system would use vector clocks -- see module 06)
        node, peer = self.nodes[via], self._peer(via)
        if self.partitioned:
            if self.mode == "CP":
                return f"ERROR: cannot reach {peer.name}; refusing write to stay consistent"
            node.local_write(key, value, self.version)
            self.pending.append((peer, key, value, self.version))
            return f"OK (written only on {node.name}; will sync later)"
        node.local_write(key, value, self.version)
        peer.local_write(key, value, self.version)
        return "OK (replicated to both)"

    def read(self, via, key):
        node, peer = self.nodes[via], self._peer(via)
        if self.partitioned and self.mode == "CP":
            return f"ERROR: cannot confirm latest value with {peer.name}"
        value, _ = node.local_read(key)
        return repr(value)

    def heal(self):
        """Partition heals: replay the writes that couldn't be delivered."""
        self.partitioned = False
        for peer, key, value, version in self.pending:
            peer.local_write(key, value, version)
        self.pending.clear()


def run_scenario(mode):
    print(f"\n--- {mode} mode " + "-" * 50)
    s = ReplicatedStore(mode)
    print("  write x=1 via A          :", s.write("A", "x", 1))
    print("  read  x   via B          :", s.read("B", "x"))
    print("  ** network partition between A and B **")
    s.partitioned = True
    print("  write x=2 via A          :", s.write("A", "x", 2))
    print("  read  x   via B          :", s.read("B", "x"),
          "  <- stale!" if mode == "AP" else "")
    print("  ** partition heals **")
    s.heal()
    print("  read  x   via B          :", s.read("B", "x"))


if __name__ == "__main__":
    print("=" * 70)
    print("CAP in action: a 2-replica key-value store under a network partition")
    print("=" * 70)
    run_scenario("CP")
    run_scenario("AP")

    print("""
WHAT YOU SAW
------------
* CP: during the partition, requests FAILED, but no one ever read stale data.
* AP: during the partition, everything SUCCEEDED, but B returned stale x=1.
      After healing, replicas CONVERGED (eventual consistency).

INTERVIEW TALKING POINTS
------------------------
* "Partitions are a fact of life, so the real choice is C vs A when one happens."
* Pick per feature: "Payments/inventory -> CP. Likes, feed, view counts -> AP."
* Use PACELC to discuss the normal case: "Even without partitions, synchronous
   cross-region replication adds ~100ms, so for the feed we'll accept
   eventual consistency (EL) to keep latency low."
* Beware: 'C' in CAP (linearizability) is NOT the 'C' in ACID (constraints/invariants).
""")
