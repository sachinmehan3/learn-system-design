"""
LESSON 07.01 -- MESSAGE QUEUES, PUB/SUB, AND DISTRIBUTED LOGS (KAFKA)
======================================================================

WHY ASYNCHRONOUS MESSAGING?
---------------------------
Synchronous call chains (A calls B calls C) couple everyone: if C is slow or
down, A is slow or down. Putting a MESSAGE BROKER between producer and
consumer gives you:
  * DECOUPLING: the producer doesn't know or care who consumes, or when.
  * BUFFERING / LOAD LEVELING: absorb traffic spikes; consumers process at
    their own steady pace (Black Friday orders pile up in the queue instead
    of crashing the payment service).
  * RELIABILITY: messages are persisted; if a consumer crashes, another
    picks the message up later.
  * ASYNC UX: return "202 Accepted" immediately; do the slow work (send
    emails, transcode video, generate thumbnails, update search index) in
    the background.
  * FAN-OUT: one event, many independent consumers.
Cost: eventual consistency, harder debugging/tracing, duplicate delivery,
ordering concerns, another system to operate.

THREE MODELS
------------
1) MESSAGE QUEUE (point-to-point) -- RabbitMQ, Amazon SQS, ActiveMQ
   Each message is delivered to ONE of the consumers ("competing consumers"),
   then deleted once acknowledged. Scale processing by adding consumers.
   Use for: task/job queues (send this email, resize this image).

2) PUBLISH/SUBSCRIBE -- SNS, Google Pub/Sub, Redis Pub/Sub, RabbitMQ fanout exchange
   Producers publish to a TOPIC; EVERY subscriber gets its own copy.
   Use for: events many services care about ("OrderPlaced" -> email service,
   analytics, inventory, fraud detection each react independently).

3) DISTRIBUTED LOG (event streaming) -- Apache Kafka, AWS Kinesis, Pulsar, Redpanda
   An APPEND-ONLY, PERSISTENT, ORDERED log split into PARTITIONS.
   * Messages are NOT deleted when read; they're retained (days, forever).
   * Each consumer tracks its own OFFSET (position) -> consumers can REPLAY
     history, and new consumers can start from the beginning.
   * A partition is consumed by exactly one consumer within a CONSUMER GROUP
     -> parallelism = number of partitions. Different groups each get
     everything (pub/sub semantics); within a group, queue semantics.
   * ORDERING is guaranteed only WITHIN a partition. Messages with the same
     KEY (e.g. user_id, order_id) go to the same partition -> per-key order.
   * Very high throughput (sequential disk I/O, batching, zero-copy):
     millions of messages/s per cluster. Replicated across brokers.
   Use for: event sourcing, CDC streams, activity tracking, metrics/log
   pipelines, stream processing (Flink, Kafka Streams), feeding many
   downstream systems from one source of truth.

QUICK CHOICE GUIDE
  * "Do this job once, somewhere" -> queue (SQS/RabbitMQ).
  * "Tell everyone this happened" -> pub/sub or Kafka topic.
  * "Durable, replayable, ordered stream at high volume" -> Kafka.
"""

import hashlib
import itertools
from collections import defaultdict, deque


class WorkQueue:
    """Point-to-point: each message goes to exactly one consumer (round-robin here)."""

    def __init__(self):
        self.messages = deque()

    def publish(self, msg):
        self.messages.append(msg)

    def run(self, consumers):
        received = defaultdict(list)
        rr = itertools.cycle(consumers)
        while self.messages:
            received[next(rr)].append(self.messages.popleft())   # delivered + deleted
        return received


class PubSubTopic:
    """Every subscriber gets every message."""

    def __init__(self):
        self.subscribers = {}

    def subscribe(self, name):
        self.subscribers[name] = []

    def publish(self, msg):
        for inbox in self.subscribers.values():
            inbox.append(msg)


class KafkaLikeTopic:
    """Partitioned append-only log with per-group offsets."""

    def __init__(self, n_partitions):
        self.partitions = [[] for _ in range(n_partitions)]
        self.offsets = defaultdict(lambda: [0] * n_partitions)   # group -> offset per partition

    def produce(self, key, value):
        # Same key -> same partition (Kafka's default partitioner hashes the key with murmur2).
        p = int(hashlib.md5(key.encode()).hexdigest(), 16) % len(self.partitions)
        self.partitions[p].append((key, value))
        return p

    def assign(self, consumers):
        """Each partition is owned by exactly one consumer in the group."""
        return {p: consumers[p % len(consumers)] for p in range(len(self.partitions))}

    def poll(self, group, partition, max_records=100):
        start = self.offsets[group][partition]
        records = self.partitions[partition][start:start + max_records]
        self.offsets[group][partition] += len(records)                # "commit offset"
        return records

    def seek(self, group, partition, offset):
        self.offsets[group][partition] = offset                       # replay!


if __name__ == "__main__":
    print("=" * 76)
    print("1) WORK QUEUE: 6 resize-image jobs, 3 competing workers")
    print("=" * 76)
    q = WorkQueue()
    for i in range(6):
        q.publish(f"resize img{i}")
    for worker, jobs in q.run(["worker-A", "worker-B", "worker-C"]).items():
        print(f"  {worker}: {jobs}")
    print("  -> each job processed ONCE; add workers to go faster.")

    print()
    print("=" * 76)
    print("2) PUB/SUB: 'OrderPlaced' events fan out to every interested service")
    print("=" * 76)
    topic = PubSubTopic()
    for svc in ("email-service", "analytics", "inventory"):
        topic.subscribe(svc)
    topic.publish("OrderPlaced#1001")
    topic.publish("OrderPlaced#1002")
    for svc, inbox in topic.subscribers.items():
        print(f"  {svc:14s} got {inbox}")

    print()
    print("=" * 76)
    print("3) KAFKA-LIKE LOG: 3 partitions, keyed by user_id")
    print("=" * 76)
    log = KafkaLikeTopic(3)
    events = [("alice", "login"), ("grace", "login"), ("alice", "add_to_cart"),
              ("erin", "login"), ("alice", "checkout"), ("grace", "logout")]
    for user, action in events:
        p = log.produce(user, action)
        print(f"  produce key={user:5s} {action:12s} -> partition {p}")
    for p, records in enumerate(log.partitions):
        print(f"  partition {p}: {records}")
    print("  -> all of alice's events are in ONE partition, in order.")

    print("\n  Consumer group 'fraud' (2 consumers) splits the partitions:")
    assignment = log.assign(["fraud-1", "fraud-2"])
    for p, consumer in assignment.items():
        print(f"    {consumer} reads partition {p}: {log.poll('fraud', p)}")

    print("\n  A NEW consumer group 'analytics' still sees EVERYTHING from offset 0:")
    total = sum(len(log.poll("analytics", p)) for p in range(3))
    print(f"    analytics read {total} events (messages weren't deleted by the fraud group)")

    print("\n  Replay: a bug in 'fraud' is fixed -> rewind partition offsets and reprocess:")
    for p in range(3):
        log.seek("fraud", p, 0)
    print(f"    re-read: {[log.poll('fraud', p) for p in range(3)]}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Put a queue between the API and slow work: "POST /videos returns 202; a
  transcoding job goes on a queue; workers autoscale on queue depth."
* Use Kafka when you need a durable, replayable, ordered event stream consumed
  by multiple independent systems; partition by the key whose order matters.
* Parallelism in Kafka is capped by partition count -- choose it up front.
* Remember the costs: at-least-once delivery -> consumers must be idempotent
  (next lessons); eventual consistency; ordering only per partition.
""")
