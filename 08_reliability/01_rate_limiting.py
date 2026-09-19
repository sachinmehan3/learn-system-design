"""
LESSON 08.01 -- RATE LIMITING (FIVE ALGORITHMS) -- a top-5 interview question
==============================================================================

WHY RATE LIMIT?
---------------
  * Protect services from overload (accidental or malicious: DoS, scrapers,
    brute-force login attempts, a client's retry-loop bug).
  * Fairness: one noisy tenant shouldn't starve the others.
  * Cost control (expensive downstream calls, paid third-party APIs).
  * Business tiers: free = 100 req/min, pro = 10,000 req/min.

WHERE
  * Client side (politeness, can't be trusted), API gateway / edge (most
    common: Nginx, Envoy, Kong, AWS API Gateway, Cloudflare), or inside
    each service. Key by: user ID, API key, IP address, endpoint, or combos.
  * Response when limited: HTTP 429 Too Many Requests + headers
    (Retry-After, X-RateLimit-Limit / -Remaining / -Reset).

THE ALGORITHMS
--------------
1) TOKEN BUCKET  (AWS, Stripe, most APIs)
   A bucket holds up to `capacity` tokens and refills at `rate` tokens/s.
   Each request takes one token; no token -> reject.
   + Allows BURSTS up to capacity while enforcing the long-run average.
   + O(1) memory per key (tokens + last refill time). The default choice.

2) LEAKY BUCKET  (traffic shaping; Nginx limit_req, Shopify)
   Requests enter a FIFO queue drained at a constant rate; full queue -> reject.
   + Perfectly smooth output rate -- protects a downstream that can't take bursts.
   - Bursts are delayed/queued, adding latency; old requests can starve new ones.

3) FIXED WINDOW COUNTER
   Count requests per key per calendar window (e.g. per minute); reset at the
   boundary. Redis: INCR key:12:05 + EXPIRE.
   + Simplest, tiny memory.
   - BOUNDARY BURST: limit 100/min -> 100 requests at 12:00:59 and 100 more at
     12:01:00 = 200 in two seconds. (Demonstrated below.)

4) SLIDING WINDOW LOG
   Store the timestamp of every request (Redis sorted set); count those in
   the last 60 s.
   + Exact.  - Memory O(limit) per key; expensive at high limits.

5) SLIDING WINDOW COUNTER  (Cloudflare's approach)
   Weighted blend of the current and previous fixed windows:
       estimate = prev_count * (1 - elapsed_fraction_of_current) + curr_count
   + O(1) memory, smooths the boundary problem; approximate (assumes the
     previous window's requests were evenly spread) -- Cloudflare measured
     ~0.003% error in practice.

DISTRIBUTED RATE LIMITING
-------------------------
With 50 API gateway nodes, per-node counters don't enforce a global limit.
  * Centralised counters in Redis, updated with an atomic Lua script (avoid
    race conditions between GET and SET). Adds a network hop; Redis must be HA.
  * Or local limits = global / N (simple, imprecise if traffic is uneven).
  * Or local counting + periodic sync (eventually consistent, slightly lenient).
  * Fail OPEN (allow) or CLOSED (deny) if Redis is down? Usually open for
    general APIs, closed for security-sensitive ones (login attempts).
"""

import random
from collections import deque


class TokenBucket:
    def __init__(self, capacity, refill_per_sec):
        self.capacity, self.rate = capacity, refill_per_sec
        self.tokens, self.last = capacity, 0.0

    def allow(self, now):
        # Lazy refill: compute tokens earned since last call (no background timer needed).
        self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


class LeakyBucket:
    """Queue with constant drain rate. allow() = 'accepted into the queue'."""

    def __init__(self, queue_size, leak_per_sec):
        self.size, self.rate = queue_size, leak_per_sec
        self.water, self.last = 0.0, 0.0

    def allow(self, now):
        self.water = max(0.0, self.water - (now - self.last) * self.rate)
        self.last = now
        if self.water + 1 <= self.size:
            self.water += 1
            return True
        return False


class FixedWindow:
    def __init__(self, limit, window):
        self.limit, self.window = limit, window
        self.current_window, self.count = None, 0

    def allow(self, now):
        w = int(now // self.window)
        if w != self.current_window:
            self.current_window, self.count = w, 0
        if self.count < self.limit:
            self.count += 1
            return True
        return False


class SlidingWindowLog:
    def __init__(self, limit, window):
        self.limit, self.window = limit, window
        self.log = deque()

    def allow(self, now):
        while self.log and self.log[0] <= now - self.window:
            self.log.popleft()
        if len(self.log) < self.limit:
            self.log.append(now)
            return True
        return False


class SlidingWindowCounter:
    def __init__(self, limit, window):
        self.limit, self.window = limit, window
        self.curr_window, self.curr, self.prev = 0, 0, 0

    def allow(self, now):
        w = int(now // self.window)
        if w != self.curr_window:
            self.prev = self.curr if w == self.curr_window + 1 else 0
            self.curr, self.curr_window = 0, w
        elapsed = (now % self.window) / self.window
        estimate = self.prev * (1 - elapsed) + self.curr
        if estimate < self.limit:
            self.curr += 1
            return True
        return False


def max_in_any_window(times, window):
    """The true worst case: most accepted requests in ANY sliding window."""
    best, j = 0, 0
    for i in range(len(times)):
        while times[i] - times[j] >= window:
            j += 1
        best = max(best, i - j + 1)
    return best


def make_limiters():
    # All configured as "10 requests per 10 seconds" (avg 1 req/s).
    return {
        "token bucket": TokenBucket(capacity=10, refill_per_sec=1.0),
        "leaky bucket": LeakyBucket(queue_size=10, leak_per_sec=1.0),
        "fixed window": FixedWindow(limit=10, window=10),
        "sliding log": SlidingWindowLog(limit=10, window=10),
        "sliding counter": SlidingWindowCounter(limit=10, window=10),
    }


if __name__ == "__main__":
    print("=" * 80)
    print("PART 1: The fixed-window boundary burst (limit = 10 req per 10 s)")
    print("=" * 80)
    # 10 requests at t=9.5..9.95 and 10 more at t=10.0..10.45
    burst = [9.5 + i * 0.05 for i in range(10)] + [10.0 + i * 0.05 for i in range(10)]
    for name, lim in make_limiters().items():
        accepted = [t for t in burst if lim.allow(t)]
        print(f"  {name:16s} accepted {len(accepted):2d}/20; most in any 10s window = "
              f"{max_in_any_window(accepted, 10):2d}")
    print("  -> fixed window let 20 requests through within ONE second (2x the limit).")

    print()
    print("=" * 80)
    print("PART 2: Bursty client -- 2 minutes of traffic averaging ~2 req/s (limit avg 1/s)")
    print("=" * 80)
    rng = random.Random(3)
    t, times = 0.0, []
    while t < 120:
        if rng.random() < 0.1:                   # occasional bursts of 15 requests
            times += [t + i * 0.02 for i in range(15)]
            t += 5
        else:
            t += rng.expovariate(1.0)
            times.append(t)
    times = sorted(x for x in times if x < 120)
    for name, lim in make_limiters().items():
        acc = [x for x in times if lim.allow(x)]
        print(f"  {name:16s} accepted {len(acc):3d}/{len(times)}   worst 10s window = "
              f"{max_in_any_window(acc, 10):2d}")
    print("  -> Token/leaky buckets deliberately allow up to capacity + refill (~20) in a")
    print("     window: bursts are OK, the long-run AVERAGE is enforced. The sliding log")
    print("     is the only strict 'never more than 10 in any 10s' limiter.")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Token bucket per API key at the gateway, state in Redis updated atomically
   via a Lua script; return 429 with Retry-After."
* Know the boundary problem of fixed windows and how sliding window fixes it.
* Leaky bucket when the DOWNSTREAM needs a smooth rate (shaping, not just policing).
* Discuss: key choice (user vs IP), distributed counters, fail-open vs
  fail-closed, and different limits per tier/endpoint.
""")
