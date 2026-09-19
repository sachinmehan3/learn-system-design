"""
LESSON 06.04 -- DISTRIBUTED TRANSACTIONS: TWO-PHASE COMMIT (2PC) vs SAGAS
==========================================================================

THE PROBLEM
-----------
Placing an order touches several services/databases:
    Order service (create order) + Payment service (charge card) +
    Inventory service (reserve stock) + Shipping service (schedule)
Within ONE database, a transaction makes this atomic. Across services each
with its OWN database, there's no shared transaction. How do we avoid
"charged the card but never reserved the item"?

OPTION 1: TWO-PHASE COMMIT (2PC) -- atomic commit across participants
---------------------------------------------------------------------
A COORDINATOR runs:
  Phase 1 (PREPARE / vote): "Can you commit?" Each participant does the work,
      writes it durably but doesn't commit, holds its LOCKS, and votes YES/NO.
      A YES is a PROMISE: "I will commit if told to, even after a crash."
  Phase 2 (COMMIT / ABORT): if ALL voted yes -> COMMIT to everyone; if any
      voted no (or timed out) -> ABORT to everyone.
  + True atomicity (ACID across nodes). Used inside distributed databases
    (Spanner, CockroachDB) and XA transactions.
  - BLOCKING: if the coordinator crashes after participants voted YES, they
    are stuck holding locks, unable to commit or abort, until it recovers
    ("in-doubt" transactions).
  - Latency: 2 round trips + locks held throughout; throughput suffers.
  - Every participant must support it; availability = product of all of them.
  => Rarely used between microservices.
  (3PC adds a phase to reduce blocking but breaks under network partitions;
  modern systems make the coordinator itself fault-tolerant with consensus.)

OPTION 2: SAGA -- a sequence of LOCAL transactions + COMPENSATIONS
------------------------------------------------------------------
Each step is a normal local transaction in one service that commits
immediately. If a later step fails, run COMPENSATING transactions for the
completed steps, in reverse order:
     T1 create order  ->  T2 reserve stock  ->  T3 charge card  ->  T4 ship
     C1 cancel order  <-  C2 release stock  <-  C3 refund card
  + No distributed locks; each service stays autonomous and available.
  - NOT isolated: other transactions can see intermediate states (an order
    "pending" while payment runs). Design for it (status fields, "semantic
    locks").
  - Compensations must be written for every step and must be IDEMPOTENT
    (they may be retried) -- and some actions can't be undone (an email
    sent -> compensate with an apology email).
  Two styles:
  * ORCHESTRATION: a central saga orchestrator tells each service what to do
    next (explicit, easy to follow and monitor; e.g. Temporal, AWS Step
    Functions, Camunda). Implemented below.
  * CHOREOGRAPHY: services react to each other's EVENTS
    ("OrderCreated" -> inventory reserves -> "StockReserved" -> payment
    charges...). Loosely coupled, but the overall flow is implicit and harder
    to debug.

Related: the TRANSACTIONAL OUTBOX pattern (module 09) solves "update my DB
AND publish an event" atomically -- a building block for reliable sagas.

RULE OF THUMB: inside one DB -> ACID transaction. Across microservices ->
saga (+ outbox + idempotency). 2PC only when you truly need atomic isolation
and control all participants.
"""

import random


# ---------------------------------------------------------------------------
# Two-phase commit
# ---------------------------------------------------------------------------
class Participant:
    def __init__(self, name, will_vote_yes=True):
        self.name = name
        self.will_vote_yes = will_vote_yes
        self.state = "idle"
        self.locked = False

    def prepare(self):
        if self.will_vote_yes:
            self.state, self.locked = "prepared", True   # durable, locks held
            return True
        self.state = "aborted"
        return False

    def commit(self):
        self.state, self.locked = "committed", False

    def abort(self):
        self.state, self.locked = "aborted", False


def two_phase_commit(participants, coordinator_crashes_after_prepare=False):
    votes = {p.name: p.prepare() for p in participants}
    print(f"    phase 1 votes: {votes}")
    if coordinator_crashes_after_prepare:
        print("    COORDINATOR CRASHES before phase 2!")
        for p in participants:
            print(f"      {p.name}: state={p.state}, holding locks={p.locked}  <- BLOCKED, in doubt")
        return
    decision = all(votes.values())
    for p in participants:
        (p.commit if decision else p.abort)()
    print(f"    phase 2 decision: {'COMMIT' if decision else 'ABORT'} -> "
          f"{ {p.name: p.state for p in participants} }")


# ---------------------------------------------------------------------------
# Saga (orchestrated)
# ---------------------------------------------------------------------------
class Services:
    """Four microservices, each with its own little 'database'."""

    def __init__(self, fail_at=None):
        self.orders, self.stock, self.payments, self.shipments = {}, {"widget": 5}, {}, {}
        self.fail_at = fail_at
        self.log = []

    def _maybe_fail(self, step):
        if step == self.fail_at:
            raise RuntimeError(f"{step} failed")

    # Forward steps (each a LOCAL transaction that commits immediately)
    def create_order(self, oid):
        self._maybe_fail("create_order")
        self.orders[oid] = "PENDING"

    def reserve_stock(self, oid):
        self._maybe_fail("reserve_stock")
        self.stock["widget"] -= 1

    def charge_card(self, oid):
        self._maybe_fail("charge_card")
        self.payments[oid] = 20.00

    def schedule_shipping(self, oid):
        self._maybe_fail("schedule_shipping")
        self.shipments[oid] = "SCHEDULED"
        self.orders[oid] = "CONFIRMED"

    # Compensations (semantic undo; idempotent)
    def cancel_order(self, oid):
        if self.orders.get(oid) != "CANCELLED":
            self.orders[oid] = "CANCELLED"

    def release_stock(self, oid):
        self.stock["widget"] += 1

    def refund_card(self, oid):
        if oid in self.payments:
            self.payments[oid] = "REFUNDED"


def run_saga(svc, oid):
    steps = [
        ("create_order", svc.create_order, svc.cancel_order),
        ("reserve_stock", svc.reserve_stock, svc.release_stock),
        ("charge_card", svc.charge_card, svc.refund_card),
        ("schedule_shipping", svc.schedule_shipping, None),
    ]
    done = []
    for name, action, compensation in steps:
        try:
            action(oid)
            done.append((name, compensation))
            print(f"    [ok]   {name}")
        except RuntimeError as e:
            print(f"    [FAIL] {e} -> compensating in reverse order")
            for prev_name, comp in reversed(done):
                if comp:
                    comp(oid)
                    print(f"    [undo] {comp.__name__} (for {prev_name})")
            return False
    return True


if __name__ == "__main__":
    print("=" * 76)
    print("TWO-PHASE COMMIT")
    print("=" * 76)
    print("  a) everyone votes yes:")
    two_phase_commit([Participant("orders-db"), Participant("payments-db"), Participant("stock-db")])
    print("  b) stock-db votes no (out of stock):")
    two_phase_commit([Participant("orders-db"), Participant("payments-db"), Participant("stock-db", False)])
    print("  c) the blocking problem:")
    two_phase_commit([Participant("orders-db"), Participant("payments-db")],
                     coordinator_crashes_after_prepare=True)

    print()
    print("=" * 76)
    print("SAGA (orchestrated) -- happy path")
    print("=" * 76)
    svc = Services()
    run_saga(svc, "o1")
    print(f"    final: order={svc.orders['o1']} stock={svc.stock} payments={svc.payments}")

    print()
    print("=" * 76)
    print("SAGA -- shipping fails after the card was charged")
    print("=" * 76)
    svc = Services(fail_at="schedule_shipping")
    run_saga(svc, "o2")
    print(f"    final: order={svc.orders['o2']} stock={svc.stock} payments={svc.payments}")
    print("    -> consistent again: stock restored, customer refunded, order cancelled.")
    print("       (But for a moment, another request could have SEEN the charge -- no isolation.)")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Across microservices I'd use an orchestrated saga: create order (PENDING)
   -> reserve stock -> charge payment -> confirm; each step has an idempotent
   compensation. Steps communicate via a durable queue with the outbox pattern."
* 2PC gives atomicity but blocks on coordinator failure and hurts
  availability/latency -- fine inside a database, avoid across services.
* Make every step and compensation idempotent (retries WILL happen).
* Expose intermediate states in the domain (PENDING/CONFIRMED/CANCELLED).
""")
