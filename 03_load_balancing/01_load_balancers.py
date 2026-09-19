"""
LESSON 03.01 -- LOAD BALANCERS, PROXIES, AND BALANCING ALGORITHMS
==================================================================

WHAT A LOAD BALANCER (LB) DOES
------------------------------
Sits in front of a pool of servers and spreads incoming requests across them.
    1. Distributes load -> horizontal scaling.
    2. Health-checks servers and stops sending traffic to dead ones -> availability.
    3. Gives clients ONE stable address while servers come and go (deploys,
       autoscaling) -> operability.
    4. Often: TLS termination, compression, caching, rate limiting.

PROXIES
-------
* FORWARD PROXY: sits in front of CLIENTS, talks to the internet on their
  behalf (corporate proxies, VPN-ish anonymity, content filtering).
* REVERSE PROXY: sits in front of SERVERS; clients think it IS the server
  (Nginx, HAProxy, Envoy). A load balancer is a reverse proxy with several
  backends. An API GATEWAY is a reverse proxy that also does auth, rate
  limiting, request routing to microservices, etc. (see module 09).

L4 vs L7 LOAD BALANCING
-----------------------
* L4 (transport layer): routes based on IP + port only; forwards TCP/UDP
  packets/connections without looking inside. Extremely fast, protocol
  agnostic, can't make content-based decisions. (AWS NLB, LVS)
* L7 (application layer): terminates the connection and parses HTTP. Can
  route by path (/api -> API servers, /static -> file servers), header,
  cookie; can retry failed requests, do TLS termination, sticky sessions via
  cookies, gRPC-aware balancing. More CPU per request. (AWS ALB, Nginx, Envoy)

ALGORITHMS
----------
STATIC (don't look at server state):
  * ROUND ROBIN: 1,2,3,1,2,3... Simple, fair if servers and requests are uniform.
  * WEIGHTED ROUND ROBIN: a server with weight 3 gets 3x the traffic
    (heterogeneous hardware, canary deployments at 5%).
  * IP HASH / consistent hashing on a key: same client (or same key) always
    goes to the same server -> session affinity, cache locality.
DYNAMIC (look at current state):
  * LEAST CONNECTIONS: send to the server with fewest in-flight requests.
    Great when request durations vary a lot (some requests take 10ms, some 5s).
  * LEAST RESPONSE TIME: favour the fastest-responding server.
  * POWER OF TWO RANDOM CHOICES: pick 2 servers at random, send to the less
    loaded one. Nearly as good as least-connections, O(1), and avoids the
    "herd" problem where many LBs all pick the same least-loaded server.
    (Used by Envoy, Nginx, and many large systems.)

HEALTH CHECKS
-------------
* ACTIVE: the LB pings /health every few seconds; N consecutive failures ->
  mark down; M successes -> mark up (hysteresis prevents flapping).
* PASSIVE: watch real traffic; too many errors/timeouts -> eject for a while
  ("outlier detection").
* Deep vs shallow health checks: a shallow check ("process is up") can miss a
  broken DB connection; a deep check that tests every dependency can take the
  WHOLE fleet out when one shared dependency blips. Usually: shallow checks
  for LB routing, deep checks for alerting.

STICKY SESSIONS
---------------
Route a user to the same server every time (cookie or IP hash). Needed if
servers hold session state in memory. Downsides: uneven load, and a server
dying loses its sessions. Preferred: stateless servers + shared session store.

AVOIDING THE LB AS A SINGLE POINT OF FAILURE
--------------------------------------------
Run LBs in pairs (active-passive with a floating/virtual IP via VRRP, or
active-active behind DNS / anycast). Cloud LBs are already redundant.

The simulation below sends requests with highly variable durations to a pool
of servers (one of them slower) under each algorithm, then kills a server to
show health checking.
"""

import itertools
import random
import zlib


class Server:
    def __init__(self, name, speed=1.0, weight=1):
        self.name = name
        self.speed = speed          # >1 = faster hardware
        self.weight = weight
        self.active = []            # finish times of in-flight requests
        self.healthy = True
        self.handled = 0

    def in_flight(self, now):
        self.active = [t for t in self.active if t > now]
        return len(self.active)

    def accept(self, now, work):
        # Requests queue behind each other if the server is busy (1 at a time for simplicity).
        start = max([now] + self.active)
        finish = start + work / self.speed
        self.active.append(finish)
        self.handled += 1
        return finish - now          # latency seen by the client


# ---------------------------------------------------------------------------
# Balancing strategies. Each is a function (servers, now, request) -> server.
# ---------------------------------------------------------------------------
def make_round_robin(servers):
    cycle = itertools.cycle(servers)

    def pick(pool, now, req):
        for _ in range(len(servers)):
            s = next(cycle)
            if s.healthy:
                return s
        raise RuntimeError("no healthy servers")
    return pick


def make_weighted_round_robin(servers):
    # Expand by weight: weight 3 -> appears 3 times in the rotation.
    # (Nginx uses a "smooth" variant that interleaves instead of bursting.)
    expanded = [s for s in servers for _ in range(s.weight)]
    cycle = itertools.cycle(expanded)

    def pick(pool, now, req):
        for _ in range(len(expanded)):
            s = next(cycle)
            if s.healthy:
                return s
        raise RuntimeError("no healthy servers")
    return pick


def least_connections(pool, now, req):
    return min((s for s in pool if s.healthy), key=lambda s: s.in_flight(now))


def make_power_of_two(rng):
    def pick(pool, now, req):
        healthy = [s for s in pool if s.healthy]
        a, b = rng.sample(healthy, 2) if len(healthy) > 1 else (healthy[0], healthy[0])
        return a if a.in_flight(now) <= b.in_flight(now) else b
    return pick


def ip_hash(pool, now, req):
    # Same client IP -> same server (while the pool is unchanged).
    # Problem: if the pool size changes, almost EVERY client is remapped.
    # Consistent hashing (next lesson) fixes that.
    healthy = [s for s in pool if s.healthy]
    return healthy[zlib.crc32(req["client_ip"].encode()) % len(healthy)]


def run(strategy_name, pick, pool, requests):
    latencies = []
    for req in requests:
        server = pick(pool, req["t"], req)
        latencies.append(server.accept(req["t"], req["work"]))
    latencies.sort()
    dist = " ".join(f"{s.name}={s.handled:4d}" for s in pool)
    print(f"  {strategy_name:22s} p50={latencies[len(latencies) // 2] * 1000:6.0f}ms "
          f"p99={latencies[int(len(latencies) * 0.99)] * 1000:7.0f}ms   {dist}")


def make_requests(rng, n=4000, rate=38.0):
    t, reqs = 0.0, []
    for _ in range(n):
        t += rng.expovariate(rate)
        # Heavy-tailed work: most requests are quick, a few are very slow.
        work = rng.choice([0.02] * 9 + [0.5])
        reqs.append({"t": t, "work": work, "client_ip": f"10.0.{rng.randint(0, 50)}.{rng.randint(0, 255)}"})
    return reqs


def fresh_pool():
    # s4 is old hardware: half speed. Weighted RR gives it less traffic.
    return [Server("s1", 1.0, 2), Server("s2", 1.0, 2), Server("s3", 1.0, 2), Server("s4", 0.5, 1)]


if __name__ == "__main__":
    rng = random.Random(5)
    requests = make_requests(rng)

    print("=" * 90)
    print("PART 1: Algorithms under variable request durations (s4 is a half-speed server)")
    print("=" * 90)
    for name, factory in (
        ("round robin", lambda p: make_round_robin(p)),
        ("weighted round robin", lambda p: make_weighted_round_robin(p)),
        ("ip hash", lambda p: ip_hash),
        ("least connections", lambda p: least_connections),
        ("power of two choices", lambda p: make_power_of_two(random.Random(1))),
    ):
        pool = fresh_pool()
        run(name, factory(pool), pool, requests)
    print("  -> Load-aware algorithms (least-conn, P2C) route AROUND busy/slow servers,")
    print("     cutting tail latency. Round robin blindly queues work on s4.")

    print()
    print("=" * 90)
    print("PART 2: Health checks -- s2 dies halfway through")
    print("=" * 90)
    pool = fresh_pool()
    pick = least_connections
    half = len(requests) // 2
    failed = 0
    for i, req in enumerate(requests):
        if i == half:
            pool[1].healthy = False   # the health checker notices and marks it DOWN
            print(f"  request #{i}: health check marks s2 DOWN; traffic shifts to s1/s3/s4")
        s = pick(pool, req["t"], req)
        s.accept(req["t"], req["work"])
    print("  final distribution:", " ".join(f"{s.name}={s.handled}" for s in pool),
          "-> no requests sent to s2 after it died")

    print()
    print("=" * 90)
    print("PART 3: Why plain hash(ip) % N is fragile")
    print("=" * 90)
    ips = [f"10.1.{i // 256}.{i % 256}" for i in range(10_000)]
    before = [zlib.crc32(ip.encode()) % 4 for ip in ips]
    after = [zlib.crc32(ip.encode()) % 5 for ip in ips]
    moved = sum(1 for a, b in zip(before, after) if a != b)
    print(f"  adding a 5th server remaps {moved / len(ips):.0%} of clients (ideal: 20%)")
    print("  -> every remapped client loses its session / cache locality. See consistent hashing next.")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "An L7 load balancer in front of stateless app servers, with active health
   checks; least-connections (or P2C) because request cost varies."
* LBs themselves are redundant (pair with a virtual IP, or managed cloud LB).
* For stateful routing (caches, WebSocket gateways, shards) use consistent hashing.
* L4 when you need raw throughput or non-HTTP protocols; L7 for smart routing.
""")
