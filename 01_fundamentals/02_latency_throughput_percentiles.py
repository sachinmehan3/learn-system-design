"""
LESSON 01.02 -- LATENCY, THROUGHPUT, PERCENTILES, AND TAIL LATENCY
==================================================================

DEFINITIONS
-----------
* LATENCY: how long ONE request takes (e.g. 45 ms).
* THROUGHPUT: how many requests the system completes per unit time (e.g. 2,000 req/s).
* BANDWIDTH: the max data rate of a link (e.g. 10 Gbit/s). Throughput is what
  you actually achieve; bandwidth is the theoretical ceiling.

Analogy: a highway. Latency = how long one car takes to drive it. Throughput =
cars per hour arriving at the end. Adding lanes raises throughput but doesn't
make any single car faster.

LITTLE'S LAW -- THE MOST USEFUL EQUATION IN CAPACITY PLANNING
-------------------------------------------------------------
        L = lambda x W
    L = average number of requests in the system (concurrency)
    lambda = throughput (arrival rate)
    W = average latency

Example: 1,000 req/s x 0.2 s latency = 200 concurrent requests in flight.
If each app server has a 50-thread pool, you need at least 4 servers (plus
headroom). Also: if your DB gets slower (W doubles), concurrency doubles and
you can exhaust thread pools / connection pools -- a classic cascading failure.

WHY AVERAGES LIE -- USE PERCENTILES
----------------------------------
Latency distributions are skewed: most requests are fast, a few are VERY slow
(GC pauses, cache misses, disk seeks, retransmits). The mean hides this.

* p50 (median): half the requests are faster than this. "Typical" experience.
* p95 / p99: 95% / 99% of requests are faster. The "tail".
* p99.9: 1 in 1,000 requests is slower than this.

SLAs and SLOs are written in percentiles: "p99 latency < 300 ms".

Why care about the 1%? Because your MOST VALUABLE users often see the tail --
they have the most data, the biggest carts, the most followers. And because
of...

TAIL LATENCY AMPLIFICATION (FAN-OUT)
------------------------------------
If serving one page requires calls to N backend services IN PARALLEL, the
page is as slow as the SLOWEST call. If each backend is slow 1% of the time:

    P(page is fast) = 0.99 ^ N
        N = 1   -> 99%   of pages fast
        N = 10  -> 90.4%
        N = 100 -> 36.6%   <- most page loads hit at least one slow backend!

This is from Google's paper "The Tail at Scale". Mitigations:
  * HEDGED REQUESTS: if no reply by the p95 time, send a duplicate request to
    another replica; use whichever returns first. Costs a few % extra load.
  * Reduce fan-out; set timeouts; return partial results (graceful degradation).

LATENCY NUMBERS EVERY PROGRAMMER SHOULD KNOW (approximate, order of magnitude)
------------------------------------------------------------------------------
    L1 cache reference ................................... 1 ns
    Main memory reference .............................. 100 ns
    Compress 1 KB with a fast codec ................... 2,000 ns  =   2 us
    Read 1 MB sequentially from memory ................ 3,000 ns  =   3 us
    Round trip within same datacenter ............... 500,000 ns  = 0.5 ms
    Read 1 MB sequentially from SSD ................. 1,000,000 ns  =   1 ms
    Random read from SSD ................................ ~100 us
    Disk (HDD) seek ................................ 10,000,000 ns  =  10 ms
    Read 1 MB sequentially from HDD ................ 20,000,000 ns  =  20 ms
    Packet round trip California -> Netherlands ..... 150,000,000 ns = 150 ms

Takeaways:
  * Memory is ~1000x faster than a network round trip -> caching works.
  * Sequential I/O is vastly faster than random I/O -> append-only logs,
    LSM-trees, Kafka are all built on this fact.
  * Cross-continent round trips are ~150 ms -> CDNs and multi-region deploys.
"""

import random
import statistics


def percentile(sorted_values, p):
    """Nearest-rank percentile. `sorted_values` must be sorted ascending."""
    if not sorted_values:
        raise ValueError("no data")
    k = max(0, min(len(sorted_values) - 1, int(round(p / 100 * len(sorted_values))) - 1))
    return sorted_values[k]


def sample_backend_latency(rng: random.Random) -> float:
    """
    A realistic-ish backend: usually ~10 ms, but 1% of the time something bad
    happens (GC pause, cache miss, retransmit) and it takes 100-500 ms.
    """
    if rng.random() < 0.01:
        return rng.uniform(100, 500)
    return rng.gauss(10, 2)


def page_latency_with_fanout(n_backends: int, rng: random.Random, hedge_after_ms=None) -> float:
    """
    A page calls n_backends in parallel; page latency = slowest backend.
    If hedge_after_ms is set, any call slower than that gets a second
    ("hedged") request to another replica, and we take the faster result.
    """
    worst = 0.0
    for _ in range(n_backends):
        t = sample_backend_latency(rng)
        if hedge_after_ms is not None and t > hedge_after_ms:
            # The hedge is sent at hedge_after_ms and has its own latency.
            t = min(t, hedge_after_ms + sample_backend_latency(rng))
        worst = max(worst, t)
    return worst


if __name__ == "__main__":
    rng = random.Random(7)

    print("=" * 70)
    print("PART 1: Mean vs percentiles for a single backend")
    print("=" * 70)
    samples = sorted(sample_backend_latency(rng) for _ in range(100_000))
    print(f"  mean  = {statistics.mean(samples):6.1f} ms   <- looks fine!")
    for p in (50, 90, 99, 99.9):
        print(f"  p{p:<5} = {percentile(samples, p):6.1f} ms")
    print("  -> The mean hides the slow tail that 1 in 100 users feels.\n")

    print("=" * 70)
    print("PART 2: Tail latency amplification with fan-out")
    print("=" * 70)
    for n in (1, 10, 50, 100):
        pages = sorted(page_latency_with_fanout(n, rng) for _ in range(5_000))
        slow = sum(1 for x in pages if x > 50) / len(pages)
        print(f"  fan-out={n:3d}: p50={percentile(pages, 50):6.1f} ms  "
              f"p99={percentile(pages, 99):6.1f} ms  pages>50ms: {slow:5.1%}  "
              f"(theory: {1 - 0.99 ** n:5.1%})")
    print()

    print("=" * 70)
    print("PART 3: Hedged requests fix the tail (fan-out = 100)")
    print("=" * 70)
    for hedge in (None, 20):
        pages = sorted(page_latency_with_fanout(100, rng, hedge_after_ms=hedge) for _ in range(5_000))
        label = "no hedging     " if hedge is None else f"hedge at {hedge} ms"
        print(f"  {label}: p50={percentile(pages, 50):6.1f} ms  p99={percentile(pages, 99):6.1f} ms")
    print()

    print("=" * 70)
    print("PART 4: Little's Law  L = lambda x W")
    print("=" * 70)
    for qps, latency_s in ((1000, 0.05), (1000, 0.2), (1000, 2.0)):
        in_flight = qps * latency_s
        print(f"  {qps} req/s x {latency_s:>4} s = {in_flight:6.0f} concurrent requests "
              f"-> need {int(-(-in_flight // 50))} servers with 50 threads each")
    print("  -> If a dependency slows from 50ms to 2s, you need 40x the threads!")
    print("     This is how one slow DB causes a whole-site outage (cascading failure).")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Always talk in percentiles: "p99 read latency under 100 ms".
* Recognise fan-out: "the feed page queries 50 shards in parallel, so tail
  latency dominates; I'd use hedged requests / timeouts with partial results."
* Use Little's law to size thread pools and connection pools.
* Know the latency ladder: memory << same-DC network << SSD << HDD << cross-continent.
""")
