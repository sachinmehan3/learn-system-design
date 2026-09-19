"""
LESSON 06.05 -- FAILURE DETECTION, HEARTBEATS, AND GOSSIP PROTOCOLS
====================================================================

FAILURE DETECTION
-----------------
In a distributed system you can't tell a DEAD node from a SLOW node or a
BROKEN NETWORK LINK. All you see is "no reply yet". So failure detectors are
based on timeouts and are always a trade-off:
  * Short timeout: fast detection, but false positives (a GC pause or a slow
    link declares a healthy node dead -> unnecessary failovers, data
    movement, even cascading failures).
  * Long timeout: fewer false alarms, but a real failure causes longer
    unavailability.

HEARTBEATS: each node periodically says "I'm alive". Missing k heartbeats
-> suspect. PHI ACCRUAL detector (Cassandra, Akka): instead of a fixed
timeout, compute a suspicion level from the observed distribution of
heartbeat arrival times -- adapts to network conditions.

Centralised vs decentralised:
  * CENTRAL MONITOR (ZooKeeper sessions, a load balancer's health checks):
    simple, but the monitor must itself be highly available.
  * ALL-TO-ALL heartbeats: O(N^2) messages -- doesn't scale to 1000s of nodes.
  * GOSSIP: scales.

GOSSIP (EPIDEMIC) PROTOCOLS
---------------------------
Every round, each node picks a few RANDOM peers and exchanges its state
(membership list with heartbeat counters, or any data). Like a rumour
spreading through a crowd:
  * Information reaches all N nodes in O(log N) rounds.
  * Each node sends a constant number of messages per round -> scales to
    thousands of nodes.
  * Robust: no central point of failure; tolerates message loss and node crashes.
  * Eventually consistent.
Used by: Cassandra and DynamoDB (membership, schema), Consul/Serf (SWIM
protocol), Redis Cluster, Bitcoin (transaction propagation), Amazon S3.

MEMBERSHIP VIA GOSSIP: each node tracks (node -> heartbeat counter, last time
it increased). Nodes increment their OWN counter; gossip merges by taking the
max counter. If a node's counter hasn't increased for T_fail -> mark failed.
"""

import math
import random


def rumor_spread(n_nodes, fanout, rng, loss=0.0):
    """Returns how many rounds until everyone has heard the rumour."""
    informed = {0}
    rounds = 0
    history = [1]
    while len(informed) < n_nodes and rounds < 100:
        rounds += 1
        new = set()
        for node in informed:
            for peer in rng.sample(range(n_nodes), fanout):
                if rng.random() >= loss:
                    new.add(peer)
        informed |= new
        history.append(len(informed))
    return rounds, history


class GossipNode:
    def __init__(self, node_id, n):
        self.id = node_id
        self.alive = True
        # membership table: node -> [heartbeat counter, local round when it last increased]
        self.table = {i: [0, 0] for i in range(n)}
        self.suspected = set()

    def beat(self, now):
        self.table[self.id] = [self.table[self.id][0] + 1, now]

    def merge(self, other_table, now):
        for node, (hb, _) in other_table.items():
            if hb > self.table[node][0]:
                self.table[node] = [hb, now]

    def detect(self, now, t_fail):
        self.suspected = {n for n, (_, last) in self.table.items()
                          if n != self.id and now - last > t_fail}


def membership_demo(rng, n=20, t_fail=8):
    nodes = [GossipNode(i, n) for i in range(n)]
    crash_at, crashed = 10, 7
    detected_at = {}
    for now in range(1, 40):
        if now == crash_at:
            nodes[crashed].alive = False
            print(f"    round {now}: node {crashed} crashes (silently -- nobody is told)")
        for node in nodes:
            if node.alive:
                node.beat(now)
        for node in nodes:
            if not node.alive:
                continue
            for peer in rng.sample([p for p in nodes if p is not node], 2):   # fanout 2
                if peer.alive:
                    peer.merge(node.table, now)
                    node.merge(peer.table, now)
        for node in nodes:
            if node.alive:
                node.detect(now, t_fail)
                if crashed in node.suspected and node.id not in detected_at:
                    detected_at[node.id] = now
        if len(detected_at) == n - 1:
            break
    rounds = sorted(detected_at.values())
    print(f"    all {n - 1} live nodes marked node {crashed} failed between rounds "
          f"{rounds[0]} and {rounds[-1]} (T_fail = {t_fail} rounds after its last heartbeat)")
    false_pos = sum(1 for nd in nodes if nd.alive and nd.suspected - {crashed})
    print(f"    false positives (healthy nodes wrongly suspected): {false_pos}")


if __name__ == "__main__":
    rng = random.Random(21)

    print("=" * 72)
    print("PART 1: How fast does gossip spread? (fanout = 2 peers per round)")
    print("=" * 72)
    for n in (10, 100, 1_000, 10_000):
        rounds, _ = rumor_spread(n, 2, rng)
        print(f"  {n:6d} nodes: everyone informed after {rounds:2d} rounds   "
              f"(log2(N) = {math.log2(n):4.1f})")
    print("  -> 1000x more nodes costs only a handful more rounds: O(log N).")

    print()
    print("=" * 72)
    print("PART 2: Robustness -- 30% of gossip messages are lost")
    print("=" * 72)
    rounds, hist = rumor_spread(1000, 2, rng, loss=0.3)
    print(f"  1000 nodes, 30% loss: still converges in {rounds} rounds; "
          f"spread per round = {hist}")

    print()
    print("=" * 72)
    print("PART 3: Gossip-based membership & failure detection (20 nodes)")
    print("=" * 72)
    membership_demo(rng)

    print("""
INTERVIEW TALKING POINTS
------------------------
* You can't distinguish slow from dead -- detectors are timeout trade-offs.
* Heartbeats to a coordinator (ZooKeeper/etcd sessions) for small clusters;
  gossip for large peer-to-peer clusters (Cassandra-style membership).
* Gossip spreads in O(log N) rounds with O(1) messages per node per round.
* Be careful acting on suspicion: failovers are expensive and can cascade.
""")
