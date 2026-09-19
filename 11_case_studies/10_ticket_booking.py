"""
CASE STUDY 10 -- DESIGN A TICKET BOOKING SYSTEM (Ticketmaster, BookMyShow, hotel/flight booking)
===============================================================================================
The interesting part is CONTENTION: thousands of people fighting for the
same few seats at the same instant. It tests concurrency control,
consistency, and handling traffic spikes.

STEP 1 -- REQUIREMENTS
----------------------
Functional: browse events and see seat availability; select seats and HOLD
them while paying (e.g. 10 minutes); confirm booking after payment; release
holds that expire.
Non-functional: NEVER double-book a seat (strong consistency for booking);
availability views can be slightly stale; survive massive spikes when
popular sales open (millions of users in minutes); fairness.

STEP 2 -- DATA MODEL
--------------------
  events(event_id, venue, time)
  seats(event_id, seat_id, status [AVAILABLE|HELD|BOOKED], hold_owner,
        hold_expires_at, version)      <- the contended rows (SQL, ACID)
  bookings(booking_id, user_id, event_id, seats, payment_id, status)

STEP 3 -- HIGH-LEVEL DESIGN
---------------------------
  users --> CDN (static pages, cached seat map snapshot) --> VIRTUAL WAITING ROOM
        --> Booking service --> SQL DB (seats, bookings)  <-- source of truth
                     |                 |
                     |                 +--> hold-expiry job (release HELD seats past expiry)
                     +--> Payment service (idempotency key per booking)
                     +--> Redis (seat map cache for browsing; not the source of truth)

STEP 4 -- DEEP DIVES
--------------------
A) PREVENTING DOUBLE BOOKING -- the core question. Options:
   1. PESSIMISTIC: SELECT ... FOR UPDATE on the seat rows inside a
      transaction, check AVAILABLE, set HELD. Safe; row locks serialize
      contenders (fine: each lock is held for milliseconds).
   2. OPTIMISTIC / CONDITIONAL UPDATE (usually best):
        UPDATE seats SET status='HELD', hold_owner=?, hold_expires_at=?
        WHERE event_id=? AND seat_id=? AND status='AVAILABLE'
      -> "1 row updated" = you got it; "0 rows" = someone else did. Atomic,
      no explicit locks, one round trip. Multi-seat holds: do it in one
      transaction and require ALL rows to update, else roll back.
   3. Distributed lock per seat in Redis with TTL (hold = lock). Fast, but
      the DB must still be the final authority (fencing, lesson 06.08).
   NOT OK: "read availability, then write" without a condition -> race.

B) HOLDS WITH EXPIRY: the hold is a lease. If payment isn't completed by
   hold_expires_at, the seat returns to AVAILABLE (background job, or lazily:
   treat expired holds as available in the conditional UPDATE's WHERE clause).

C) SPIKES: a VIRTUAL WAITING ROOM / queue admits users at a controlled rate
   (tokens), protecting the booking service and adding fairness (FIFO-ish,
   bot defence). Browsing is served from caches/CDN; only the booking write
   path hits the DB. Rate limit per user; CAPTCHA against bots.

D) PAYMENTS: idempotency key per booking attempt; if payment succeeds but
   the confirmation fails, a reconciliation job fixes it (or saga compensation:
   refund if the hold had expired).

E) SCALE: shard by event_id (all seats of an event in one shard ->
   single-shard transactions). A mega event is a hot shard -> a strong
   primary + waiting room is usually enough, since the seat count is small
   (tens of thousands of rows).

The demo runs 500 concurrent users trying to book 100 seats against a real
sqlite3 database, first with a racy read-then-write, then with a
conditional UPDATE, plus hold expiry.
"""

import os
import random
import sqlite3
import tempfile
import threading
import time


def make_db(path, n_seats=100):
    db = sqlite3.connect(path)
    db.executescript("""
        DROP TABLE IF EXISTS seats; DROP TABLE IF EXISTS bookings;
        CREATE TABLE seats (seat_id INTEGER PRIMARY KEY, status TEXT, owner TEXT, hold_expires REAL);
        CREATE TABLE bookings (seat_id INTEGER, owner TEXT);
    """)
    db.executemany("INSERT INTO seats VALUES (?, 'AVAILABLE', NULL, NULL)", [(i,) for i in range(n_seats)])
    db.commit()
    db.close()


def racy_book(path, user, seat, barrier):
    db = sqlite3.connect(path, timeout=30)
    status = db.execute("SELECT status FROM seats WHERE seat_id=?", (seat,)).fetchone()[0]
    barrier.wait()                                         # everyone has read "AVAILABLE"...
    if status == "AVAILABLE":                              # ...and acts on the stale read
        with db:
            db.execute("UPDATE seats SET status='BOOKED', owner=? WHERE seat_id=?", (user, seat))
            db.execute("INSERT INTO bookings VALUES (?, ?)", (seat, user))
    db.close()


def safe_hold(path, user, seat, barrier, hold_seconds=600, now=None):
    db = sqlite3.connect(path, timeout=30)
    barrier.wait()
    now = now or time.time()
    with db:
        cur = db.execute("""
            UPDATE seats SET status='HELD', owner=?, hold_expires=?
            WHERE seat_id=? AND (status='AVAILABLE' OR (status='HELD' AND hold_expires < ?))
        """, (user, now + hold_seconds, seat, now))
        got_it = cur.rowcount == 1                         # 1 row = we won; 0 = someone else did
        if got_it:
            db.execute("INSERT INTO bookings VALUES (?, ?)", (seat, user))
    db.close()
    return got_it


def run_contest(path, fn, n_users=500, n_seats=100, rng=None):
    barrier = threading.Barrier(n_users)
    threads = []
    for u in range(n_users):
        seat = rng.randrange(n_seats) if rng.random() < 0.8 else rng.randrange(10)   # front rows are hot
        threads.append(threading.Thread(target=fn, args=(path, f"user{u}", seat, barrier)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    db = sqlite3.connect(path)
    bookings = db.execute("SELECT seat_id, COUNT(*) FROM bookings GROUP BY seat_id").fetchall()
    db.close()
    double = sum(1 for _, c in bookings if c > 1)
    return len(bookings), double, sum(c for _, c in bookings)


if __name__ == "__main__":
    path = os.path.join(tempfile.gettempdir(), "ticket_booking_demo.db")
    rng = random.Random(1)

    print("=" * 78)
    print("500 users hit 'book' at the same instant for 100 seats (front rows are hot)")
    print("=" * 78)
    make_db(path)
    seats, double, total = run_contest(path, racy_book, rng=random.Random(1))
    print(f"  read-then-write     : {total} tickets issued for {seats} seats; "
          f"{double} seats DOUBLE-BOOKED")
    make_db(path)
    seats, double, total = run_contest(path, safe_hold, rng=random.Random(1))
    print(f"  conditional UPDATE  : {total} tickets issued for {seats} seats; "
          f"{double} double-booked")

    print()
    print("=" * 78)
    print("Hold expiry: a held-but-unpaid seat becomes bookable after its hold lapses")
    print("=" * 78)
    make_db(path, n_seats=1)
    one = threading.Barrier(1)
    t0 = time.time()
    print(f"  alice holds seat 0 (10 min hold)        : {safe_hold(path, 'alice', 0, one, now=t0)}")
    print(f"  bob tries 1 minute later                : {safe_hold(path, 'bob', 0, one, now=t0 + 60)}")
    print(f"  bob tries 11 minutes later (hold lapsed): {safe_hold(path, 'bob', 0, one, now=t0 + 660)}")
    os.remove(path)

    print("""
FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How exactly do you prevent double booking? (conditional UPDATE ... WHERE
  status='AVAILABLE' -- check rowcount; or SELECT FOR UPDATE)
* How do holds expire? (hold_expires column; expired holds count as
  available in the WHERE clause; a sweeper job cleans up)
* 2 million users when sales open? (virtual waiting room admitting users at a
  controlled rate, cached browsing, per-user rate limits, bot protection)
* Payment succeeds but booking confirmation fails? (idempotency keys +
  reconciliation job / saga compensation with refund)
""")
