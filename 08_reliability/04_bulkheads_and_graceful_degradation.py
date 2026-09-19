"""
LESSON 08.04 -- BULKHEADS, FALLBACKS, AND GRACEFUL DEGRADATION
===============================================================

BULKHEADS
---------
Ships are divided into watertight compartments (bulkheads) so one breach
doesn't sink the whole ship. In software: ISOLATE RESOURCES so one failing
dependency or tenant can't exhaust resources everyone needs.
  * Separate THREAD POOLS / connection pools per dependency: if the
    "recommendations" service hangs, only its 10 threads get stuck; the
    checkout path's pool is untouched.
  * Separate CLUSTERS / queues per customer tier or workload (don't let a
    batch job share a DB replica with user-facing traffic).
  * CELL-BASED ARCHITECTURE (AWS, Slack): split the whole system into
    independent cells, each serving a subset of customers. A bad deploy or
    poison request takes out one cell, not everyone ("blast radius").
  * SHUFFLE SHARDING: give each customer a random small subset of workers;
    two customers rarely share the SAME subset, so one bad customer affects
    very few others.

GRACEFUL DEGRADATION
--------------------
When a non-critical dependency fails, serve a WORSE but still USEFUL response
instead of an error:
  * Recommendations down -> show "popular items" from a static cache.
  * Personalised feed slow -> show a cached / chronological feed.
  * Reviews service down -> product page without reviews.
  * Search overloaded -> disable expensive filters / fuzzy matching.
  * Under extreme load, turn off features by FEATURE FLAG ("brownout").
Decide in advance which features are CRITICAL (checkout, login) and which are
OPTIONAL -- and design the page so optional parts can fail independently.

The simulation: a product page needs "inventory" (critical, fast) and
"recommendations" (optional, becomes very slow). Compare one shared thread
pool vs separate bulkheaded pools with timeouts and fallbacks.
"""

import concurrent.futures as cf
import threading
import time

RECS_HANG = threading.Event()


def inventory_service():
    time.sleep(0.01)
    return "in stock"


def recommendations_service():
    if RECS_HANG.is_set():
        time.sleep(1.0)                      # the dependency is hanging
    else:
        time.sleep(0.01)
    return ["book", "pen"]


def run(design, n_requests=60):
    """Serve n concurrent product-page requests; return success count and p50 latency."""
    if design == "shared pool":
        shared = cf.ThreadPoolExecutor(max_workers=10)
        inv_pool = recs_pool = shared
    else:
        inv_pool = cf.ThreadPoolExecutor(max_workers=5)     # bulkhead for the critical path
        recs_pool = cf.ThreadPoolExecutor(max_workers=5)    # bulkhead for the optional path

    def handle_request():
        start = time.perf_counter()
        recs_future = recs_pool.submit(recommendations_service)
        inv_future = inv_pool.submit(inventory_service)
        try:
            stock = inv_future.result(timeout=0.5)          # critical: must succeed
        except cf.TimeoutError:
            return None, time.perf_counter() - start        # page fails
        try:
            recs = recs_future.result(timeout=0.05)         # optional: short timeout
        except cf.TimeoutError:
            recs = ["popular items (cached fallback)"]      # graceful degradation
        return (stock, recs), time.perf_counter() - start

    with cf.ThreadPoolExecutor(max_workers=n_requests) as frontend:
        results = list(frontend.map(lambda _: handle_request(), range(n_requests)))
    ok = [r for r, _ in results if r is not None]
    latencies = sorted(t for _, t in results)
    degraded = sum(1 for r in ok if "fallback" in r[1][0])
    for p in {inv_pool, recs_pool}:
        p.shutdown(wait=False, cancel_futures=True)
    return len(ok), degraded, latencies[len(latencies) // 2]


if __name__ == "__main__":
    print("=" * 78)
    print("60 concurrent product-page requests while RECOMMENDATIONS hangs (1s per call)")
    print("=" * 78)
    RECS_HANG.set()
    for design in ("shared pool", "bulkheads"):
        ok, degraded, p50 = run(design)
        print(f"  {design:12s}: pages served {ok:2d}/60 (degraded {degraded}), p50 latency {p50 * 1000:5.0f} ms")
        time.sleep(1.2)                   # let the hung calls from this run drain
    RECS_HANG.clear()

    print("""
WHAT YOU SAW
------------
* SHARED POOL: hung recommendation calls occupied all 10 threads, so the fast,
  CRITICAL inventory calls couldn't get a thread -> pages failed. An OPTIONAL
  feature took down the whole page.
* BULKHEADS: recommendation calls could only exhaust THEIR pool. Inventory
  stayed fast, and pages were served with a cached "popular items" fallback.

INTERVIEW TALKING POINTS
------------------------
* "Critical and non-critical dependencies get separate pools (bulkheads),
   each call has a timeout, and optional features have fallbacks."
* Cell-based architecture / shuffle sharding to limit blast radius.
* Classify features as critical vs optional; use feature flags to shed
  optional work under load (brownouts).
""")
