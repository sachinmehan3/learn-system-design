"""
LESSON 07.02 -- DELIVERY SEMANTICS, ACKS, RETRIES, AND DEAD-LETTER QUEUES
==========================================================================

The fundamental problem: a consumer processes a message and then tells the
broker "done" (ACK). A crash can happen BETWEEN those two steps, or the ACK
can be lost. So what does the broker do?

AT-MOST-ONCE
------------
Ack (or commit the offset) BEFORE processing. If the consumer crashes
mid-processing, the message is LOST -- never retried.
  Fine for: metrics samples, logs, sensor readings where a gap is OK.

AT-LEAST-ONCE  (the default in practice: SQS, RabbitMQ, Kafka as usually used)
-------------
Process FIRST, ack AFTER. If the consumer crashes after processing but
before acking, the broker redelivers -> the message is processed TWICE.
  No loss, but DUPLICATES. Consumers MUST be idempotent (next lesson).
  SQS: a received message becomes invisible for a VISIBILITY TIMEOUT; if not
  deleted by then, it reappears for another consumer.

EXACTLY-ONCE
------------
Truly exactly-once DELIVERY over an unreliable network is impossible (the
"two generals" problem). What systems offer is EXACTLY-ONCE *PROCESSING*
(effectively-once): at-least-once delivery + idempotent/deduplicated
processing, or atomically committing the result and the consumer offset
together (Kafka transactions for read-process-write within Kafka).
  In interviews say: "at-least-once delivery with idempotent consumers gives
  us effectively-once processing."

RETRIES AND POISON MESSAGES
---------------------------
Some messages fail EVERY time (malformed payload, a bug). Retrying forever
blocks the queue (or burns CPU). So:
  * Retry with backoff a limited number of times (maxReceiveCount).
  * Then move it to a DEAD-LETTER QUEUE (DLQ) for humans/tools to inspect,
    fix, and redrive later. Alert on DLQ depth.

ORDERING vs RETRIES
-------------------
Retrying a failed message while later ones proceed breaks strict order. If
order matters per key, retry in place (block that partition/key) or park the
whole key's messages. (SQS FIFO + message group IDs; Kafka partitions.)

The simulation below processes 1,000 "charge card" messages with a consumer
that crashes 5% of the time between processing and acking, plus a few
poison messages.
"""

import random
from collections import Counter, deque


def simulate(mode, rng, n=1000, crash_rate=0.05, poison=frozenset({13, 666}), max_attempts=3):
    queue = deque((i, 0) for i in range(n))       # (message id, attempts)
    processed = Counter()                          # side effects (e.g. card charges)
    dlq = []
    while queue:
        msg, attempts = queue.popleft()
        attempts += 1
        crash = rng.random() < crash_rate

        if mode == "at-most-once":
            # ack first -> the message is gone from the queue no matter what
            if crash or msg in poison:
                continue                           # lost forever
            processed[msg] += 1
            continue

        # at-least-once: process, then ack
        if msg in poison:
            if attempts >= max_attempts:
                dlq.append(msg)                    # give up -> dead-letter queue
            else:
                queue.append((msg, attempts))      # redeliver later (after visibility timeout)
            continue
        processed[msg] += 1                        # side effect happens...
        if crash:
            queue.append((msg, attempts))          # ...but the ack was never sent -> redelivered

    lost = n - len(poison) - len(processed)
    dups = sum(c - 1 for c in processed.values())
    return lost, dups, dlq


if __name__ == "__main__":
    rng = random.Random(1)
    print("=" * 76)
    print("1,000 'charge card' messages; consumer crashes 5% of the time;")
    print("messages 13 and 666 are poison (always fail)")
    print("=" * 76)
    for mode in ("at-most-once", "at-least-once"):
        lost, dups, dlq = simulate(mode, rng)
        print(f"  {mode:14s}: lost={lost:3d}  duplicate processings={dups:3d}  DLQ={dlq}")

    print("""
WHAT YOU SAW
------------
* at-most-once: ~5% of payments silently lost (plus the poison ones).
* at-least-once: nothing lost, but ~50 customers charged TWICE -- unless the
  consumer is idempotent. Poison messages went to the DLQ after 3 attempts
  instead of blocking the queue forever.
-> Next lesson: making the consumer idempotent so duplicates are harmless.

INTERVIEW TALKING POINTS
------------------------
* "We'll use at-least-once delivery with idempotent consumers (dedup on a
   message/idempotency key), which gives effectively-once processing."
* Bounded retries with exponential backoff, then a DLQ with alerting.
* Visibility timeout must exceed normal processing time (or extend it via heartbeat).
* Strict ordering + retries conflict: order per key via partitions/message groups.
""")
