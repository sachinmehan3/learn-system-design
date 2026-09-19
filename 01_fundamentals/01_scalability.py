"""
LESSON 01.01 -- SCALABILITY: VERTICAL vs HORIZONTAL, AND WHY QUEUES EXPLODE
===========================================================================

WHAT IS SCALABILITY?
--------------------
A system is *scalable* if it can handle more load by adding resources, without
a redesign, while keeping performance acceptable.

"Load" is described by LOAD PARAMETERS specific to your system:
    - requests per second (QPS) to a web server
    - ratio of reads to writes in a database
    - number of simultaneously active users in a chat room
    - cache hit rate
    - fan-out: e.g. one tweet delivered to 10 million followers

Always ask "scalable with respect to WHAT?" A system that handles 1M reads/sec
might fall over at 10k writes/sec.

TWO WAYS TO SCALE
-----------------
1) VERTICAL SCALING ("scale up"): buy a bigger machine.
   + Dead simple. No code changes. No distributed-systems headaches.
   + Great first step. A single modern server can have 100+ cores and TBs of
     RAM. Stack Overflow ran for years on a handful of big SQL servers.
   - Hard ceiling: the biggest machine money can buy.
   - Cost grows super-linearly (a 2x bigger box costs more than 2x).
   - Single point of failure (SPOF): one box dies -> everything is down.

2) HORIZONTAL SCALING ("scale out"): add more machines.
   + Near-unlimited ceiling; commodity hardware; built-in redundancy.
   - You now have a DISTRIBUTED SYSTEM: network failures, partial failures,
     data consistency, coordination. (That's what most of this course is about.)

THE KEY ENABLER OF HORIZONTAL SCALING: STATELESSNESS
----------------------------------------------------
If an application server keeps user session data in its own memory, then the
user MUST keep hitting that same server ("sticky sessions"). That makes
scaling and failover painful.

Instead, make app servers STATELESS: push state out to a shared store (a
database, Redis, or a signed token held by the client). Then ANY server can
serve ANY request, and you can add/remove servers freely behind a load
balancer. This is the #1 pattern in interviews:

        clients --> load balancer --> [app1] [app2] [app3]   (stateless, cloneable)
                                          |      |      |
                                          +------+------+--> shared state (DB / cache)

Scaling the stateless tier is easy. Scaling the STATEFUL tier (the database)
is the hard part -- see module 05 (replication and sharding).

WHY YOU CAN'T RUN SERVERS AT 100% -- A TASTE OF QUEUEING THEORY
---------------------------------------------------------------
A server with utilization rho (rho = arrival rate / service rate) doesn't get
linearly slower as load rises. For the classic M/M/1 queue model:

        average time in system  W = service_time / (1 - rho)

    rho = 50%  -> W = 2x the service time
    rho = 80%  -> W = 5x
    rho = 90%  -> W = 10x
    rho = 99%  -> W = 100x   <- latency explodes near saturation

That's why capacity planning targets ~60-70% peak utilization, and why adding
ONE more server to a hot cluster can dramatically cut latency. The simulation
below shows this empirically.

AMDAHL'S LAW -- THE LIMIT OF "JUST ADD MACHINES"
-----------------------------------------------
If a fraction `s` of the work is inherently serial (e.g. every request takes
a lock on one database row), the max speedup with N machines is:

        speedup(N) = 1 / (s + (1 - s) / N)

With s = 5%, even infinite machines give at most 20x. In system design terms:
find and eliminate the SHARED BOTTLENECK (single DB, global lock, one hot key)
or horizontal scaling stops helping.
"""

import random
import heapq


# ---------------------------------------------------------------------------
# A tiny discrete-event simulation of a cluster of servers sharing one queue.
# ---------------------------------------------------------------------------
def simulate_cluster(num_servers: int, arrival_rate: float, service_rate: float,
                     num_requests: int = 50_000, seed: int = 42) -> dict:
    """
    Simulate requests arriving at a load-balanced cluster.

    arrival_rate : average requests arriving per second (Poisson process --
                   the standard model for independent users hitting a site).
    service_rate : requests ONE server can finish per second.

    Returns average and p99 latency (queue wait + service time).
    """
    rng = random.Random(seed)

    # Each server is represented by "the time at which it next becomes free".
    # A min-heap lets us quickly grab the server that frees up soonest --
    # which is exactly what a "least busy" load balancer would pick.
    server_free_at = [0.0] * num_servers
    heapq.heapify(server_free_at)

    now = 0.0
    latencies = []
    for _ in range(num_requests):
        # Time between arrivals in a Poisson process is exponentially distributed.
        now += rng.expovariate(arrival_rate)
        service_time = rng.expovariate(service_rate)

        earliest_free = heapq.heappop(server_free_at)
        start = max(now, earliest_free)          # wait in queue if all busy
        finish = start + service_time
        heapq.heappush(server_free_at, finish)

        latencies.append(finish - now)           # what the user experiences

    latencies.sort()
    utilization = arrival_rate / (num_servers * service_rate)
    return {
        "servers": num_servers,
        "utilization": utilization,
        "avg_ms": 1000 * sum(latencies) / len(latencies),
        "p99_ms": 1000 * latencies[int(0.99 * len(latencies))],
    }


def amdahl_speedup(serial_fraction: float, n: int) -> float:
    """Max speedup from n workers when `serial_fraction` of work can't be parallelised."""
    return 1 / (serial_fraction + (1 - serial_fraction) / n)


if __name__ == "__main__":
    print("=" * 70)
    print("PART 1: Latency vs utilization on ONE server (service time = 10 ms)")
    print("=" * 70)
    # One server handles 100 req/s (10ms each). Increase load toward 100 req/s.
    for load in (50, 70, 80, 90, 95, 98):
        r = simulate_cluster(num_servers=1, arrival_rate=load, service_rate=100)
        print(f"  load={load:3d} req/s  utilization={r['utilization']:4.0%}  "
              f"avg={r['avg_ms']:7.1f} ms   p99={r['p99_ms']:8.1f} ms")
    print("  -> Notice latency is NOT linear: it explodes as utilization -> 100%.\n")

    print("=" * 70)
    print("PART 2: Horizontal scaling -- fixed load of 950 req/s, add servers")
    print("=" * 70)
    for n in (10, 11, 12, 15, 20):
        r = simulate_cluster(num_servers=n, arrival_rate=950, service_rate=100)
        print(f"  servers={n:2d}  utilization={r['utilization']:4.0%}  "
              f"avg={r['avg_ms']:6.1f} ms   p99={r['p99_ms']:7.1f} ms")
    print("  -> Going from 10 to 11 servers (+10% cost) cuts p99 dramatically.\n")

    print("=" * 70)
    print("PART 3: Amdahl's law -- the serial bottleneck caps your speedup")
    print("=" * 70)
    for s in (0.01, 0.05, 0.20):
        speedups = ", ".join(f"N={n}: {amdahl_speedup(s, n):5.1f}x" for n in (2, 10, 100, 1000))
        print(f"  serial={s:4.0%}  ->  {speedups}")
    print("  -> Find and remove the shared bottleneck; don't just add machines.")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "I'll keep the app tier stateless so we can scale it horizontally behind a
   load balancer; session state lives in Redis / a signed token."
* "I'd start with vertical scaling for the database -- it's simpler -- and move
   to read replicas and then sharding only when the numbers require it."
* "We should provision for ~60-70% peak utilization; queueing delay grows
   non-linearly as we approach 100%."
* Always identify the component that doesn't scale horizontally (usually the
  database or a hot key) -- that's where the interesting discussion is.
""")
