"""
LESSON 08.03 -- TIMEOUTS, RETRIES, EXPONENTIAL BACKOFF, AND JITTER
===================================================================

TIMEOUTS -- ALWAYS SET THEM
---------------------------
A call without a timeout can hang forever, holding a thread, a connection,
memory. Every network call needs: a CONNECT timeout (short, ~1s) and a
REQUEST timeout (based on the dependency's p99.9 latency, not its average).
DEADLINE PROPAGATION: if the user's request has 2s left, pass "deadline =
now + 2s" down the call chain (gRPC does this) so deep services don't keep
working on requests the user has already given up on.

RETRIES -- HELPFUL AND DANGEROUS
--------------------------------
Retries turn TRANSIENT failures (a dropped packet, a node restarting, a
brief 503) into successes. But:
  * Only retry IDEMPOTENT operations (or use idempotency keys -- module 07).
  * Only retry RETRYABLE errors: timeouts, 503, 429 (honour Retry-After),
    connection resets. Never 400/401/403/404 -- they'll fail again.
  * RETRY STORMS / AMPLIFICATION: if each of 5 layers retries 3 times, one
    user request can become 3^5 = 243 calls to the bottom service -- exactly
    when it's already struggling. Retry at ONE layer (usually the edge/client)
    or use a RETRY BUDGET (retries <= 10% of normal traffic).

EXPONENTIAL BACKOFF
-------------------
Wait longer after each failure: base * 2^attempt (100ms, 200ms, 400ms, 800ms
...) capped at some max. Gives the dependency time to recover.

JITTER -- THE PART PEOPLE FORGET
--------------------------------
If 10,000 clients all failed at the same moment (the server restarted), then
with plain exponential backoff they ALL retry at exactly +100ms, then exactly
+300ms, ... -- synchronized waves (a "thundering herd") that knock the server
over again each time. Add RANDOMNESS:
  * FULL JITTER:       sleep = random(0, min(cap, base * 2^attempt))
  * EQUAL JITTER:      sleep = half + random(0, half)
  * DECORRELATED:      sleep = min(cap, random(base, prev_sleep * 3))
AWS's analysis ("Exponential Backoff and Jitter") found full jitter spreads
load best and completes work with the fewest total calls.

The simulation: 1,000 clients hit a server that just restarted and can
handle 150 requests per 10ms tick. Compare retry strategies.
"""

import random
from collections import Counter

SERVER_CAPACITY_PER_TICK = 150   # requests the server can handle per 10ms tick
BASE_TICKS, CAP_TICKS, MAX_ATTEMPTS = 1, 64, 8


def effective_capacity(arrivals):
    """Each excess request wastes some server work, eating into useful capacity."""
    excess = max(0, arrivals - SERVER_CAPACITY_PER_TICK)
    return max(10, int(SERVER_CAPACITY_PER_TICK - 0.15 * excess))


def backoff(strategy, attempt, rng, prev):
    # Every retry waits at least 1 tick (a retry can't be sent in the same instant it failed).
    return max(1, _backoff(strategy, attempt, rng, prev))


def _backoff(strategy, attempt, rng, prev):
    exp = min(CAP_TICKS, BASE_TICKS * 2 ** attempt)
    if strategy == "immediate":
        return 1
    if strategy == "exponential":
        return exp
    if strategy == "exp + full jitter":
        return rng.randint(1, max(1, exp))
    if strategy == "exp + equal jitter":
        return exp // 2 + rng.randint(0, max(1, exp // 2))
    if strategy == "decorrelated jitter":
        return min(CAP_TICKS, rng.randint(BASE_TICKS, max(BASE_TICKS + 1, prev * 3)))
    raise ValueError(strategy)


def simulate(strategy, n_clients=1000, seed=0):
    rng = random.Random(seed)
    # (tick when the client sends, client id, attempt number, previous sleep)
    pending = [(0, c, 0, BASE_TICKS) for c in range(n_clients)]
    done, gave_up, total_calls, tick = set(), 0, 0, 0
    peak = 0
    while pending:
        now_sending = [p for p in pending if p[0] == tick]
        pending = [p for p in pending if p[0] != tick]
        peak = max(peak, len(now_sending))
        total_calls += len(now_sending)
        rng.shuffle(now_sending)
        # The server handles up to its capacity; the rest get 503 (overloaded).
        # Rejecting requests isn't free (accept connection, parse, TLS...), so a
        # flood REDUCES useful throughput: "congestion collapse".
        useful = effective_capacity(len(now_sending))
        for i, (_, c, attempt, prev) in enumerate(now_sending):
            if i < useful:
                done.add(c)
            elif attempt + 1 >= MAX_ATTEMPTS:
                gave_up += 1
            else:
                sleep = backoff(strategy, attempt, rng, prev)
                pending.append((tick + sleep, c, attempt + 1, sleep))
        tick += 1
    return {"calls": total_calls, "finished_tick": tick, "gave_up": gave_up, "peak": peak}


def load_profile(strategy, seed=0, n_clients=1000):
    """Requests arriving per tick for the first 40 ticks (to visualise waves)."""
    rng = random.Random(seed)
    pending = [(0, c, 0, BASE_TICKS) for c in range(n_clients)]
    arrivals = Counter()
    tick = 0
    while pending and tick < 40:
        now_sending = [p for p in pending if p[0] == tick]
        pending = [p for p in pending if p[0] != tick]
        arrivals[tick] = len(now_sending)
        useful = effective_capacity(len(now_sending))
        for i, (_, c, attempt, prev) in enumerate(now_sending):
            if i >= useful and attempt + 1 < MAX_ATTEMPTS:
                sleep = backoff(strategy, attempt, rng, prev)
                pending.append((tick + sleep, c, attempt + 1, sleep))
        tick += 1
    return [arrivals[t] for t in range(40)]


def sparkline(values, width_max=1000):
    blocks = " .:-=+*#%@"
    return "".join(blocks[min(9, int(v / width_max * 9 + (0.999 if v else 0)))] for v in values)


if __name__ == "__main__":
    strategies = ["immediate", "exponential", "exp + equal jitter", "exp + full jitter", "decorrelated jitter"]
    print("=" * 84)
    print(f"1,000 clients vs a server handling {SERVER_CAPACITY_PER_TICK} req per 10ms tick")
    print("=" * 84)
    print(f"  {'strategy':22s} {'total calls':>11s} {'done after':>11s} {'gave up':>8s} {'peak/tick':>9s}")
    for s in strategies:
        r = simulate(s)
        print(f"  {s:22s} {r['calls']:11d} {r['finished_tick'] * 10:9d}ms {r['gave_up']:8d} {r['peak']:9d}")

    print("\n  Arrivals per tick, first 400ms (taller = more load; server handles 150):")
    for s in ("exponential", "exp + full jitter"):
        print(f"    {s:18s} |{sparkline(load_profile(s))}|")

    print("""
WHAT YOU SAW
------------
* immediate retries flood the server every tick; the flood itself cuts useful
  throughput (congestion collapse) and ~70% of clients exhaust their retries.
* plain exponential backoff is no better: every client that failed together
  retries TOGETHER -- look at the synchronized spikes -- so each wave collapses
  the server again. Same failures, just spread over more time.
* jitter de-synchronizes the clients, the server stays near capacity, and
  EVERY client succeeds with roughly half the total calls.

INTERVIEW TALKING POINTS
------------------------
* "Clients retry idempotent requests on timeouts/503/429 with capped
   exponential backoff and full jitter, at most 3 attempts, within a retry
   budget; every call has a timeout and propagates its deadline."
* Retry at one layer only, to avoid multiplicative retry storms.
* Combine with circuit breakers so a dead dependency isn't retried forever.
""")
