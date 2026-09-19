"""
LESSON 01.03 -- AVAILABILITY, RELIABILITY, AND "THE NINES"
==========================================================

DEFINITIONS
-----------
* AVAILABILITY: fraction of time the system is up and serving correctly.
        availability = uptime / (uptime + downtime)
                     = MTBF / (MTBF + MTTR)
      MTBF = mean time between failures, MTTR = mean time to recovery.
  => You improve availability by failing LESS often (higher MTBF) or
     RECOVERING faster (lower MTTR). In practice, lowering MTTR (automation,
     fast rollbacks, auto-failover) is often cheaper and more effective.

* RELIABILITY: the system keeps working correctly even when things go wrong
  (faults). A system can be available but unreliable (up, but returning
  wrong data).

* FAULT vs FAILURE: a FAULT is one component deviating from spec (a disk
  dies). A FAILURE is the whole system stopping service to users. The goal
  of fault-tolerant design: prevent faults from becoming failures.

* DURABILITY: once data is acknowledged as written, it's never lost.
  (S3 advertises 11 nines of durability -- different from availability!)

THE NINES
---------
    Availability   Downtime per year   Downtime per month
    99%            3.65 days           7.3 hours
    99.9%          8.77 hours          43.8 minutes
    99.99%         52.6 minutes        4.4 minutes
    99.999%        5.26 minutes        26 seconds

Each extra nine is ~10x harder and more expensive. At 99.99% there's no time
for a human to even notice an outage -- recovery must be automated.

SLA / SLO / SLI
---------------
* SLI (indicator): a measured metric. "Fraction of requests served < 300ms."
* SLO (objective): the internal target for an SLI. "99.9% over 30 days."
* SLA (agreement): a contract with customers, with penalties. Usually looser
  than the SLO so you have margin.
* ERROR BUDGET: 100% - SLO. At 99.9%, you may "spend" 43 min/month on
  failures -- including risky deploys. Budget exhausted -> freeze features,
  fix reliability.

COMPOSING AVAILABILITY
----------------------
SERIES (A calls B, both needed):      A_total = A1 x A2 x ...
    Three 99.9% services in a chain -> 99.7%. Every dependency LOWERS availability.

PARALLEL (redundant replicas, any one suffices):
                                      A_total = 1 - (1 - A1) x (1 - A2) x ...
    Two 99% replicas -> 1 - 0.01^2 = 99.99%. Redundancy RAISES availability.

This is the math behind "eliminate single points of failure": put redundant
copies of every critical component (load balancers, app servers, DB replicas,
even whole datacenters / availability zones).

CAVEAT: the parallel formula assumes INDEPENDENT failures. Two replicas in the
same rack, on the same power supply, running the same buggy deploy, are NOT
independent. Correlated failures are why we spread replicas across racks,
availability zones (AZs), and regions, and roll out deploys gradually.

REDUNDANCY PATTERNS
-------------------
* ACTIVE-PASSIVE (failover): a standby takes over when the primary dies.
  Simple; standby capacity sits idle; failover takes time (seconds-minutes).
* ACTIVE-ACTIVE: all nodes serve traffic. Better utilization, instant
  "failover", but harder: for stateful systems you must handle concurrent
  writes on multiple nodes (conflicts!).
* N+1 / N+2 capacity: provision enough that losing one (or two) nodes still
  leaves enough capacity for peak load.
"""

import random


def nines_table():
    for a in (0.99, 0.999, 0.9999, 0.99999):
        down_min_year = (1 - a) * 365.25 * 24 * 60
        print(f"  {a:.3%}  ->  {down_min_year:9.1f} min/year  "
              f"({down_min_year / 12:7.2f} min/month)")


def series(*avail):
    """All components required: multiply availabilities."""
    total = 1.0
    for a in avail:
        total *= a
    return total


def parallel(*avail):
    """Any one component suffices: 1 - P(all down)."""
    all_down = 1.0
    for a in avail:
        all_down *= (1 - a)
    return 1 - all_down


def monte_carlo_system(trials=200_000, seed=1):
    """
    Verify the formulas by brute-force simulation.

    Architecture:
        LB pair (active-passive, 99.9% each)
          -> 3 app servers (any 1 enough, 99% each)
          -> DB primary + 1 replica with auto-failover (99.5% each)
    """
    rng = random.Random(seed)
    up = 0
    for _ in range(trials):
        lb_up = rng.random() < 0.999 or rng.random() < 0.999
        app_up = any(rng.random() < 0.99 for _ in range(3))
        db_up = rng.random() < 0.995 or rng.random() < 0.995
        up += lb_up and app_up and db_up
    return up / trials


if __name__ == "__main__":
    print("=" * 70)
    print("PART 1: The nines")
    print("=" * 70)
    nines_table()

    print()
    print("=" * 70)
    print("PART 2: Dependencies in SERIES reduce availability")
    print("=" * 70)
    chain = [0.999] * 5
    print(f"  5 services each 99.9%, all required: {series(*chain):.4%}")
    print(f"  10 services each 99.9%:              {series(*([0.999] * 10)):.4%}")
    print("  -> Microservice call chains quietly erode availability.")

    print()
    print("=" * 70)
    print("PART 3: Redundancy in PARALLEL increases availability")
    print("=" * 70)
    for n in (1, 2, 3):
        print(f"  {n} replica(s) at 99% each: {parallel(*([0.99] * n)):.6%}")

    print()
    print("=" * 70)
    print("PART 4: Whole architecture -- formula vs Monte Carlo simulation")
    print("=" * 70)
    formula = series(parallel(0.999, 0.999), parallel(0.99, 0.99, 0.99), parallel(0.995, 0.995))
    print(f"  formula:     {formula:.5%}")
    print(f"  simulation:  {monte_carlo_system():.5%}")
    no_redundancy = series(0.999, 0.99, 0.995)
    print(f"  same system with NO redundancy: {no_redundancy:.3%}  "
          f"(~{(1 - no_redundancy) * 525960:.0f} min/yr down)")

    print()
    print("=" * 70)
    print("PART 5: MTBF vs MTTR -- faster recovery is often the cheaper win")
    print("=" * 70)
    for mtbf_h, mttr_h in ((720, 4), (720, 0.5), (1440, 4)):
        a = mtbf_h / (mtbf_h + mttr_h)
        print(f"  fail every {mtbf_h:5d}h, recover in {mttr_h:>4}h  ->  {a:.4%}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "There's no single point of failure: two LBs (active-passive with a floating
   IP), N+1 stateless app servers across 3 AZs, and a DB with a synchronous
   standby in another AZ and automatic failover."
* Distinguish availability (can I reach it?) from durability (is my data safe?).
* Every synchronous dependency multiplies in; prefer async (queues) for
  non-critical paths so their outages don't take you down.
* Mention correlated failures: spread across AZs/regions; canary deploys.
""")
