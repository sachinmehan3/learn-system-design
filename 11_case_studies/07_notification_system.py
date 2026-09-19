"""
CASE STUDY 07 -- DESIGN A NOTIFICATION SYSTEM (push, SMS, email, in-app)
========================================================================

STEP 1 -- REQUIREMENTS
----------------------
Functional: other services send notifications (order shipped, new message,
password reset, marketing campaigns) to users via iOS/Android push, SMS,
email, in-app. Respect user preferences and opt-outs. Templates. Scheduling.
Non-functional: high throughput (millions/min for campaigns), reliable (no
lost critical notifications), no duplicates, prioritisation (an OTP code
must not wait behind a 10M-user marketing blast), rate limiting (don't spam
users; stay within provider limits), observability (sent/delivered/opened).

STEP 2 -- ESTIMATION
--------------------
  10M push + 1M SMS + 5M email per day; campaign peaks of ~1M in minutes.
  Third-party providers are the bottleneck and cost: APNs/FCM (push),
  Twilio (SMS -- expensive!), SendGrid/SES (email).

STEP 3 -- HIGH-LEVEL DESIGN
---------------------------
  services --> Notification API (auth, validate, idempotency key)
                   |
                   v
             Notification service:
               1. load user preferences + contact info (device tokens,
                  phone, email) from cache/DB
               2. apply opt-outs, quiet hours, per-user rate limits, dedupe
               3. render template (+ localisation)
               4. enqueue per CHANNEL and PRIORITY
                   |
     +-------------+--------------+----------------+
     v             v              v                v
  push queue    SMS queue     email queue     in-app queue     (Kafka/SQS; high & low priority)
     |             |              |                |
  push workers  SMS workers   email workers   in-app workers  (retry w/ backoff, DLQ)
     |             |              |                |
   APNs/FCM      Twilio        SendGrid        WebSocket / inbox table
                   \\             |               /
                    +--> delivery status / analytics (Kafka -> store)

STEP 4 -- DEEP DIVES
--------------------
* DECOUPLING BY CHANNEL: separate queues + worker pools so a slow SMS
  provider doesn't delay push notifications (bulkheads, lesson 08.04).
* PRIORITY: separate high-priority queues (OTP, security alerts) and
  low-priority (marketing), workers drain high first.
* RELIABILITY: persist the notification before acknowledging the caller;
  at-least-once delivery from queues -> dedupe by notification ID
  (idempotency, lesson 07.03) so users don't get two texts.
* RETRIES: exponential backoff with jitter on provider errors; respect
  provider rate limits (token bucket per provider); DLQ after N attempts;
  fail over to a secondary provider for critical messages.
* USER RATE LIMITING / BATCHING: cap marketing notifications per user per day;
  collapse bursts ("Alice and 12 others liked your photo").
* DEVICE TOKENS go stale -> remove on provider "unregistered" responses.
* TRACKING: delivery receipts / open tracking pixels / click redirects feed
  analytics.

The simulation pushes a mix of critical and marketing notifications through
preference filtering, dedup, per-user limits, priority queues, and flaky
providers with retries.
"""

import heapq
import random
from collections import Counter, defaultdict

PRIORITY = {"otp": 0, "security": 0, "message": 1, "marketing": 2}   # lower = more urgent


class NotificationService:
    def __init__(self, rng, prefs, marketing_daily_cap=2):
        self.rng = rng
        self.prefs = prefs                                 # user -> set of allowed channels
        self.cap = marketing_daily_cap
        self.marketing_sent = Counter()
        self.seen_ids = set()
        self.queues = defaultdict(list)                    # channel -> heap of (priority, seq, notif)
        self.seq = 0
        self.stats = Counter()
        self.delivered = []

    def submit(self, notif):
        if notif["id"] in self.seen_ids:                   # idempotency: caller retried
            self.stats["deduplicated"] += 1
            return
        self.seen_ids.add(notif["id"])
        user, kind = notif["user"], notif["kind"]
        channels = [c for c in notif["channels"] if c in self.prefs[user] or kind in ("otp", "security")]
        if not channels:
            self.stats["dropped: user opted out"] += 1
            return
        if kind == "marketing":
            if self.marketing_sent[user] >= self.cap:
                self.stats["dropped: marketing cap"] += 1
                return
            self.marketing_sent[user] += 1
        for ch in channels:
            self.seq += 1
            heapq.heappush(self.queues[ch], (PRIORITY[kind], self.seq, dict(notif, channel=ch, attempts=0)))

    def provider_send(self, notif):
        return self.rng.random() > 0.15                    # providers fail 15% of the time

    def run_workers(self, per_tick=50, max_attempts=4):
        tick = 0
        while any(self.queues.values()):
            tick += 1
            for ch, q in self.queues.items():               # each channel has its own worker pool
                retry_later = []
                for _ in range(min(per_tick, len(q))):
                    prio, seq, n = heapq.heappop(q)         # most urgent first
                    n["attempts"] += 1
                    if self.provider_send(n):
                        self.delivered.append((tick, n["kind"], ch))
                        self.stats[f"delivered via {ch}"] += 1
                    elif n["attempts"] < max_attempts:
                        retry_later.append((prio, seq, n))  # (a real system waits with backoff)
                        self.stats["retries"] += 1
                    else:
                        self.stats["dead-lettered"] += 1
                for item in retry_later:
                    heapq.heappush(q, item)
        return tick


if __name__ == "__main__":
    rng = random.Random(12)
    users = [f"u{i}" for i in range(300)]
    prefs = {u: set(rng.sample(["push", "email", "sms"], rng.randint(0, 3))) for u in users}
    svc = NotificationService(rng, prefs)

    notifs = []
    for i in range(1500):                                  # a marketing blast arrives first...
        notifs.append({"id": f"mkt-{i}", "user": rng.choice(users), "kind": "marketing",
                       "channels": ["email", "push"]})
    for i in range(100):                                   # ...then urgent OTPs
        notifs.append({"id": f"otp-{i}", "user": rng.choice(users), "kind": "otp", "channels": ["sms"]})
    for i in range(300):
        notifs.append({"id": f"msg-{i}", "user": rng.choice(users), "kind": "message", "channels": ["push"]})
    notifs += notifs[1500:1520]                            # some callers retried (duplicates)

    for n in notifs:
        svc.submit(n)
    ticks = svc.run_workers()

    print("=" * 78)
    print(f"{len(notifs)} submissions (1500 marketing, 100 OTP, 300 chat messages, 20 retried duplicates)")
    print("=" * 78)
    for k, v in sorted(svc.stats.items()):
        print(f"  {k:28s} {v}")
    first_tick = defaultdict(lambda: None)
    avg_tick = defaultdict(list)
    for tick, kind, ch in svc.delivered:
        avg_tick[kind].append(tick)
    print("\n  average delivery tick by kind (lower = sooner):")
    for kind in ("otp", "message", "marketing"):
        ts = avg_tick[kind]
        print(f"    {kind:10s} {sum(ts) / len(ts):5.1f}  (n={len(ts)})")
    print("  -> OTPs submitted AFTER the marketing blast were still delivered first:")
    print("     separate channel queues + priority ordering.")

    print("""
FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How do you guarantee no duplicates? (notification ID dedup at submit AND in
  workers before calling the provider; provider idempotency keys where offered)
* A provider is down? (retries with backoff, circuit breaker, secondary provider)
* 10M-user campaign? (batch expansion job: user segments -> queue in chunks,
  throttled to provider limits; low priority so it never delays OTPs)
* How do you track delivery/open rates? (provider callbacks/webhooks + open
  pixels -> Kafka -> analytics store)
""")
