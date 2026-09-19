"""
LESSON 07.03 -- IDEMPOTENCY: MAKING RETRIES AND DUPLICATES SAFE
================================================================

DEFINITION
----------
An operation is IDEMPOTENT if doing it once or N times has the same effect.
    SET balance = 50           idempotent
    balance = balance - 10     NOT idempotent
    DELETE order 7             idempotent
    INSERT order (auto id)     NOT idempotent

WHY IT'S EVERYWHERE IN SYSTEM DESIGN
------------------------------------
Distributed systems retry: clients retry on timeouts, load balancers retry,
queues redeliver (at-least-once), users double-click "Pay". A timeout doesn't
tell you whether the request failed or succeeded-but-the-reply-was-lost. The
ONLY safe way to retry a non-idempotent action is to make it idempotent.

TECHNIQUES
----------
1) IDEMPOTENCY KEY (API level; Stripe, PayPal, AWS ClientToken)
   The client generates a unique key per LOGICAL operation (UUID) and sends
   it with every retry. The server stores key -> result; a repeat with the
   same key returns the stored result without re-executing.
   Details that matter:
     * Store the key and the side effect ATOMICALLY (same DB transaction),
       otherwise a crash between them breaks the guarantee.
     * Handle concurrent duplicates (both in flight at once): insert the key
       first with status IN_PROGRESS under a UNIQUE constraint; the second
       request sees the conflict and waits/returns 409.
     * Keys expire after a retention window (e.g. 24 h).
     * Check the payload matches the original (same key, different body -> error).

2) DEDUPLICATION TABLE (consumer level)
   Each message carries a unique message ID. The consumer, in ONE
   transaction: checks "processed_messages" for the ID, applies the effect,
   inserts the ID. Duplicates find the ID and are skipped.

3) NATURAL IDEMPOTENCY / UPSERTS
   Design the write so repeating it is harmless: "set status=SHIPPED where
   order_id=7" or "INSERT ... ON CONFLICT (order_id) DO NOTHING". Use
   business keys (order_id) rather than generated ones.

4) VERSION / SEQUENCE CHECKS
   "Apply update v8 only if current version is v7" -- duplicates and
   out-of-order old messages are ignored (also prevents reordering bugs).

The demo applies at-least-once deliveries (with duplicates) to a payment
consumer with and without a dedup table, using sqlite3 so the check+effect
is a real atomic transaction.
"""

import random
import sqlite3
import uuid


def make_db():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INTEGER);
        CREATE TABLE processed_messages (message_id TEXT PRIMARY KEY);   -- the dedup table
        INSERT INTO accounts VALUES ('alice', 1000);
    """)
    return db


def naive_consumer(db, msg):
    with db:
        db.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (msg["amount"], msg["account"]))


def idempotent_consumer(db, msg):
    try:
        with db:   # ONE transaction: record the message id AND apply the effect
            db.execute("INSERT INTO processed_messages VALUES (?)", (msg["id"],))
            db.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?",
                       (msg["amount"], msg["account"]))
        return "applied"
    except sqlite3.IntegrityError:
        return "duplicate skipped"   # PRIMARY KEY violation -> we've seen this message


class PaymentAPI:
    """API-level idempotency keys: key -> stored response."""

    def __init__(self):
        self.responses = {}
        self.charges = []

    def charge(self, idempotency_key, amount):
        if idempotency_key in self.responses:
            return self.responses[idempotency_key] + " (replayed, not charged again)"
        charge_id = f"ch_{len(self.charges) + 1}"
        self.charges.append((charge_id, amount))
        self.responses[idempotency_key] = f"charged {amount} as {charge_id}"
        return self.responses[idempotency_key]


if __name__ == "__main__":
    rng = random.Random(2)
    messages = [{"id": str(uuid.uuid4()), "account": "alice", "amount": 10} for _ in range(50)]
    # At-least-once delivery: ~20% of messages get delivered twice (or three times).
    deliveries = []
    for m in messages:
        deliveries += [m] * rng.choice([1, 1, 1, 1, 2, 3])
    rng.shuffle(deliveries)

    print("=" * 76)
    print(f"50 payments of $10, delivered {len(deliveries)} times (at-least-once)")
    print("=" * 76)
    db = make_db()
    for m in deliveries:
        naive_consumer(db, m)
    print(f"  naive consumer      : alice balance = {db.execute('SELECT balance FROM accounts').fetchone()[0]}"
          f"  (expected 500)  <- overcharged!")

    db = make_db()
    outcomes = [idempotent_consumer(db, m) for m in deliveries]
    print(f"  idempotent consumer : alice balance = {db.execute('SELECT balance FROM accounts').fetchone()[0]}"
          f"  (expected 500)  skipped {outcomes.count('duplicate skipped')} duplicates")

    print()
    print("=" * 76)
    print("API idempotency key: client times out and retries the same logical payment")
    print("=" * 76)
    api = PaymentAPI()
    key = str(uuid.uuid4())                     # generated ONCE per logical operation
    print("  attempt 1:", api.charge(key, 99), " <- response lost in network; client times out")
    print("  attempt 2:", api.charge(key, 99))
    print("  attempt 3:", api.charge(key, 99))
    print(f"  total charges actually made: {len(api.charges)}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Every mutating API accepts an Idempotency-Key; we store key -> response
   in the same transaction as the side effect, with a unique constraint."
* "Queue consumers dedupe on message ID in the same transaction as the write."
* Prefer naturally idempotent writes (upserts on business keys, absolute
  SETs, version checks) over increments.
* Idempotency is what turns at-least-once delivery into effectively-once processing.
""")
