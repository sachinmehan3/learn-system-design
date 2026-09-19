"""
LESSON 09.03 -- THE DUAL-WRITE PROBLEM, TRANSACTIONAL OUTBOX, AND CDC
======================================================================

THE DUAL-WRITE PROBLEM
----------------------
A service must (1) update its database AND (2) publish an event to a broker
("OrderPlaced"). These are two different systems -- no shared transaction.
  * DB commit, then crash before publishing -> event LOST; downstream
    services (email, inventory) never hear about the order.
  * Publish first, then DB commit fails -> a GHOST event for an order that
    doesn't exist.
  * Same problem: updating the DB and a cache, or the DB and a search index.
No ordering of the two writes is safe.

SOLUTION 1: TRANSACTIONAL OUTBOX
--------------------------------
  1. In the SAME local DB transaction as the business change, INSERT the
     event into an OUTBOX table. Both commit or neither does (atomic!).
  2. A separate RELAY process reads unpublished outbox rows, publishes them
     to the broker, then marks them published.
  3. The relay may crash after publishing but before marking -> it
     republishes -> at-least-once delivery -> consumers are idempotent
     (dedupe on event ID -- module 07).

SOLUTION 2: CHANGE DATA CAPTURE (CDC)
-------------------------------------
Tail the database's own replication log (MySQL binlog, Postgres WAL/logical
replication, DynamoDB Streams, MongoDB change streams) and turn every
committed change into an event (Debezium -> Kafka is the classic stack).
  + No application changes; can't forget to publish; exact commit order.
  - Events mirror table rows (low-level), not domain events -- often combined
    WITH an outbox table: CDC tails the outbox table (the "outbox + Debezium"
    pattern), avoiding a polling relay.
CDC is also how you keep caches, search indexes (Elasticsearch), data
warehouses, and read models in sync with the primary DB.

The demo uses sqlite3 to show a crash between "DB write" and "publish",
first with naive dual writes, then with an outbox.
"""

import json
import sqlite3


class Broker:
    def __init__(self):
        self.messages = []

    def publish(self, event):
        self.messages.append(event)


class Crash(Exception):
    pass


def setup():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT, status TEXT);
        CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT, published INTEGER DEFAULT 0);
    """)
    return db


def naive_place_order(db, broker, order_id, item, crash_after_commit=False):
    with db:
        db.execute("INSERT INTO orders VALUES (?, ?, 'PLACED')", (order_id, item))
    if crash_after_commit:
        raise Crash("process died between DB commit and publish")
    broker.publish({"type": "OrderPlaced", "order_id": order_id})


def outbox_place_order(db, order_id, item):
    with db:   # ONE atomic local transaction: business row + event row
        db.execute("INSERT INTO orders VALUES (?, ?, 'PLACED')", (order_id, item))
        db.execute("INSERT INTO outbox (event) VALUES (?)",
                   (json.dumps({"type": "OrderPlaced", "order_id": order_id}),))


def relay(db, broker, crash_before_marking=False):
    """Background process: publish unpublished outbox rows, then mark them."""
    rows = db.execute("SELECT id, event FROM outbox WHERE published = 0 ORDER BY id").fetchall()
    for row_id, event in rows:
        broker.publish(dict(json.loads(event), event_id=row_id))
        if crash_before_marking:
            raise Crash("relay died after publishing, before marking")
        with db:
            db.execute("UPDATE outbox SET published = 1 WHERE id = ?", (row_id,))


if __name__ == "__main__":
    print("=" * 76)
    print("NAIVE DUAL WRITE: DB commit, then crash before publishing")
    print("=" * 76)
    db, broker = setup(), Broker()
    naive_place_order(db, broker, 1, "book")
    try:
        naive_place_order(db, broker, 2, "lamp", crash_after_commit=True)
    except Crash as e:
        print(f"  !! {e}")
    orders = db.execute("SELECT id FROM orders").fetchall()
    print(f"  orders in DB: {[o[0] for o in orders]}   events published: {[m['order_id'] for m in broker.messages]}")
    print("  -> order 2 exists but no service was ever told. Inventory/email are now wrong.")

    print()
    print("=" * 76)
    print("TRANSACTIONAL OUTBOX")
    print("=" * 76)
    db, broker = setup(), Broker()
    outbox_place_order(db, 1, "book")
    outbox_place_order(db, 2, "lamp")
    print("  both orders + their events committed atomically; the process may die now -- no loss.")
    try:
        relay(db, broker, crash_before_marking=True)
    except Crash as e:
        print(f"  !! {e}")
    relay(db, broker)                       # relay restarts and resumes
    print(f"  published events: {broker.messages}")
    seen, applied = set(), []
    for m in broker.messages:               # idempotent consumer dedupes on event_id
        if m["event_id"] not in seen:
            seen.add(m["event_id"])
            applied.append(m["order_id"])
    print(f"  consumer applied orders: {applied}  (the duplicate from the relay crash was skipped)")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Whenever a design says "write to the DB and then publish an event / update
  the cache / update the search index", call out the dual-write problem.
* Fix: transactional outbox (event row in the same transaction) + relay, or
  CDC (Debezium) on the DB log. Both give at-least-once -> idempotent consumers.
* CDC is the standard way to feed caches, search indexes, warehouses, and
  CQRS read models from the source-of-truth database.
""")
