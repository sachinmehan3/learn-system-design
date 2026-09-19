"""
LESSON 09.02 -- EVENT-DRIVEN ARCHITECTURE, EVENT SOURCING, AND CQRS
===================================================================

EVENT-DRIVEN ARCHITECTURE (EDA)
-------------------------------
Services communicate by publishing EVENTS -- facts about things that
happened ("OrderPlaced", "PaymentFailed") -- to a broker (Kafka, SNS/SQS,
EventBridge). Other services subscribe and react. The producer doesn't know
who listens.
  + Loose coupling; easy to add new consumers; natural audit trail;
    resilience (consumers can be down and catch up later).
  - Eventual consistency; harder to follow the flow ("who reacts to what?");
    event schema evolution (use a schema registry, versioned events);
    duplicates/out-of-order delivery (idempotent consumers).

EVENT SOURCING
--------------
Normally a DB stores CURRENT STATE (balance = 70) and overwrites it on
change. With event sourcing, the source of truth is the APPEND-ONLY LOG OF
EVENTS (Deposited 100, Withdrew 30). Current state is DERIVED by replaying
(folding) the events.
  + Complete audit history for free (finance, compliance, "why is this value
    what it is?").
  + TIME TRAVEL: rebuild the state as of any past moment.
  + Fix bugs retroactively: change the projection code, replay, get corrected
    state. Build brand-new read models from old events.
  - Replaying millions of events is slow -> periodic SNAPSHOTS (state at event
    N; replay only events after it).
  - Events are immutable forever: schema evolution and GDPR deletion ("crypto-
    shredding": encrypt per-user data, delete the key) need care.
  - More complex than CRUD; querying current state needs projections.
  Examples: bank ledgers, accounting (the original event-sourced system!),
  Git, shopping carts, order lifecycles.

CQRS -- COMMAND QUERY RESPONSIBILITY SEGREGATION
------------------------------------------------
Use DIFFERENT MODELS for writes (commands) and reads (queries):
  * WRITE side: validates commands, enforces invariants, stores events or
    normalized data. Optimised for correctness.
  * READ side: one or more denormalized PROJECTIONS / materialized views
    shaped exactly for each query (a search index, a per-user feed table, a
    reporting DB), updated asynchronously from the write side's events.
  + Scale reads and writes independently; each read model is fast and simple.
  - Read models are EVENTUALLY consistent with writes (a few ms-seconds lag);
    more moving parts.
CQRS pairs naturally with event sourcing but doesn't require it.
Interview example: Twitter's home timeline is a precomputed read model
(fan-out on write) separate from the tweets store.
"""

from collections import defaultdict


class EventStore:
    """Append-only log of events per aggregate (bank account)."""

    def __init__(self):
        self.events = []                               # (seq, account, type, amount)
        self.subscribers = []

    def append(self, account, etype, amount):
        event = (len(self.events) + 1, account, etype, amount)
        self.events.append(event)
        for fn in self.subscribers:                    # publish to projections
            fn(event)
        return event

    def stream(self, account, up_to_seq=None):
        return [e for e in self.events if e[1] == account and (up_to_seq is None or e[0] <= up_to_seq)]


def fold_balance(events, start=0):
    """Current state = a pure function of the events."""
    balance = start
    for _, _, etype, amount in events:
        balance += amount if etype == "Deposited" else -amount
    return balance


class AccountCommandHandler:
    """WRITE side: validates commands against current state, emits events."""

    def __init__(self, store):
        self.store = store

    def deposit(self, account, amount):
        if amount <= 0:
            raise ValueError("amount must be positive")
        return self.store.append(account, "Deposited", amount)

    def withdraw(self, account, amount):
        if fold_balance(self.store.stream(account)) < amount:    # invariant: no overdraft
            raise ValueError("insufficient funds")
        return self.store.append(account, "Withdrew", amount)


class BalanceProjection:
    """READ model #1: current balance per account (like a cache table)."""

    def __init__(self):
        self.balances = defaultdict(int)

    def on_event(self, e):
        _, account, etype, amount = e
        self.balances[account] += amount if etype == "Deposited" else -amount


class BigTransactionsProjection:
    """READ model #2: added later for the fraud team -- built by REPLAYING history."""

    def __init__(self, threshold):
        self.threshold, self.alerts = threshold, []

    def on_event(self, e):
        if e[3] >= self.threshold:
            self.alerts.append(e)


if __name__ == "__main__":
    store = EventStore()
    balances = BalanceProjection()
    store.subscribers.append(balances.on_event)
    commands = AccountCommandHandler(store)

    print("=" * 76)
    print("WRITE side: commands validated, stored as immutable events")
    print("=" * 76)
    for op, acct, amt in [("deposit", "alice", 100), ("deposit", "bob", 50), ("withdraw", "alice", 30),
                          ("deposit", "alice", 5000), ("withdraw", "bob", 80), ("withdraw", "alice", 1200)]:
        try:
            e = getattr(commands, op)(acct, amt)
            print(f"  {op:8s} {acct:5s} {amt:5d} -> event {e}")
        except ValueError as err:
            print(f"  {op:8s} {acct:5s} {amt:5d} -> REJECTED: {err} (no event written)")

    print()
    print("=" * 76)
    print("READ side (CQRS projection): current balances, updated from events")
    print("=" * 76)
    print(f"  {dict(balances.balances)}")

    print()
    print("=" * 76)
    print("Time travel: alice's balance as of event #3")
    print("=" * 76)
    print(f"  alice @ seq 3 = {fold_balance(store.stream('alice', up_to_seq=3))}  "
          f"(today: {fold_balance(store.stream('alice'))})")
    print("  audit trail:", [(s, t, a) for s, _, t, a in store.stream("alice")])

    print()
    print("=" * 76)
    print("A NEW read model added months later, built by replaying all history")
    print("=" * 76)
    fraud = BigTransactionsProjection(threshold=1000)
    for e in store.events:
        fraud.on_event(e)
    store.subscribers.append(fraud.on_event)
    print(f"  large transactions found in history: {fraud.alerts}")

    print()
    print("=" * 76)
    print("Snapshots: avoid replaying everything")
    print("=" * 76)
    snapshot_seq, snapshot_balance = 4, fold_balance(store.stream("alice", up_to_seq=4))
    later = [e for e in store.stream("alice") if e[0] > snapshot_seq]
    print(f"  snapshot at seq {snapshot_seq}: balance {snapshot_balance}; replay {len(later)} later event(s) "
          f"-> {fold_balance(later, start=snapshot_balance)}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Event sourcing when history/audit matters (payments, ledgers, orders):
  append-only events are the source of truth; state is a projection;
  snapshots keep replay fast.
* CQRS when read and write shapes/loads differ a lot: normalized write model,
  denormalized read models (search index, feed tables) updated asynchronously.
* Always acknowledge the cost: eventual consistency between write and read
  sides, event schema evolution, operational complexity.
""")
