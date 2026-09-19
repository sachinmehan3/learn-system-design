"""
LESSON 09.01 -- MONOLITH vs MICROSERVICES, API GATEWAY, SERVICE DISCOVERY
=========================================================================

MONOLITH
--------
One deployable application containing all features, usually one database.
  + Simple to develop, test, deploy, debug; in-process calls are fast and
    reliable; ACID transactions across features; easy refactoring.
  - As team and code grow: slow builds, risky deploys (everything ships
    together), one bug/memory leak can take down everything, can only scale
    the whole thing, tech stack locked in.
  "MODULAR MONOLITH": strong internal module boundaries, one deployable. The
  right starting point for most products (Shopify, Stack Overflow, Basecamp
  run large monoliths).

MICROSERVICES
-------------
Many small services, each owning ONE business capability AND ITS OWN DATA
("database per service"), communicating over the network (REST/gRPC/events).
  + Independent deploys and scaling per service; team autonomy (Conway's law:
    architecture mirrors org structure); fault isolation; polyglot tech.
  - DISTRIBUTED SYSTEM TAX: network latency and failures between every call,
    no cross-service transactions (sagas -- module 06), data consistency is
    eventual, distributed tracing needed to debug, service discovery, versioned
    APIs, much more infrastructure (CI/CD, containers, orchestration, mesh).
  Anti-pattern: the DISTRIBUTED MONOLITH -- services that must be deployed
  together or share a database. All the costs, none of the benefits.

How to split: along BUSINESS CAPABILITIES / DOMAIN-DRIVEN DESIGN "bounded
contexts" (orders, payments, catalog, users), not technical layers. Migrate
incrementally with the STRANGLER FIG pattern: route one feature at a time
from the monolith to a new service behind a proxy.

Interview stance: "Start with a modular monolith; extract services where
there's a clear scaling, team-ownership, or reliability reason."

API GATEWAY
-----------
A single entry point in front of the services:
  * Routing (/users/* -> user-service), protocol translation (REST -> gRPC)
  * Cross-cutting concerns in ONE place: authentication, rate limiting, TLS
    termination, request logging, CORS, caching, request/response shaping.
  * BFF (Backend-for-Frontend): a gateway per client type (web, mobile) that
    aggregates several service calls into one response.
  Risk: becomes a bottleneck / SPOF / dumping ground for business logic ->
  keep it thin, run it redundantly. (Kong, AWS API Gateway, Envoy, Apigee.)

SERVICE DISCOVERY
-----------------
Instances come and go (autoscaling, deploys, crashes) with changing IPs. How
does a caller find a healthy instance of "payment-service"?
  * SERVICE REGISTRY (Consul, etcd, ZooKeeper, Eureka): instances REGISTER on
    startup and send heartbeats (or have a TTL); instances missing heartbeats
    are removed.
  * CLIENT-SIDE discovery: the caller queries the registry and load-balances
    itself (Netflix Eureka + Ribbon).
  * SERVER-SIDE discovery: the caller hits a load balancer / DNS name, which
    consults the registry (Kubernetes Services + kube-proxy, AWS ALB).
  * SERVICE MESH (Istio, Linkerd): a sidecar proxy (Envoy) next to each
    service handles discovery, load balancing, retries, timeouts, circuit
    breaking, mTLS, and telemetry -- taking that code out of every service.

The demo: a registry with heartbeat TTLs, and an API gateway that
authenticates, rate-limits, discovers instances, and routes.
"""

import itertools
import random


class ServiceRegistry:
    def __init__(self, ttl=3):
        self.ttl = ttl
        self.instances = {}          # (service, address) -> last heartbeat time

    def register(self, service, address, now):
        self.instances[(service, address)] = now

    def heartbeat(self, service, address, now):
        self.instances[(service, address)] = now

    def healthy(self, service, now):
        return [addr for (svc, addr), last in self.instances.items()
                if svc == service and now - last <= self.ttl]


class APIGateway:
    ROUTES = {"/users": "user-service", "/orders": "order-service", "/catalog": "catalog-service"}

    def __init__(self, registry):
        self.registry = registry
        self.rr = {}
        self.request_counts = {}

    def handle(self, path, token, now):
        # 1) Authentication (in one place, not in every service)
        if token != "valid-token":
            return "401 Unauthorized"
        # 2) Rate limiting (very crude: 5 requests per token per time unit)
        key = (token, int(now))
        self.request_counts[key] = self.request_counts.get(key, 0) + 1
        if self.request_counts[key] > 5:
            return "429 Too Many Requests"
        # 3) Routing
        prefix = "/" + path.strip("/").split("/")[0]
        service = self.ROUTES.get(prefix)
        if not service:
            return "404 Not Found"
        # 4) Service discovery + load balancing over HEALTHY instances only
        instances = self.registry.healthy(service, now)
        if not instances:
            return f"503 {service} unavailable"
        cycle = self.rr.setdefault(service, itertools.count())
        target = instances[next(cycle) % len(instances)]
        return f"200 via {service}@{target}"


if __name__ == "__main__":
    reg = ServiceRegistry(ttl=3)
    for addr in ("10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"):
        reg.register("order-service", addr, now=0)
    reg.register("user-service", "10.0.1.1:8080", now=0)
    gw = APIGateway(reg)

    print("=" * 76)
    print("API gateway: auth -> rate limit -> route -> discover -> load balance")
    print("=" * 76)
    for path, token in (("/orders/42", "valid-token"), ("/orders/43", "valid-token"),
                        ("/users/7", "valid-token"), ("/orders/44", "stolen?"),
                        ("/payments/1", "valid-token")):
        print(f"  t=0 GET {path:12s} token={token:12s} -> {gw.handle(path, token, now=0.5)}")

    print("\n  Rate limit: 7 rapid requests from one client in the same second:")
    print("   ", [gw.handle("/orders/1", "valid-token", now=1.2)[:3] for _ in range(7)])

    print()
    print("=" * 76)
    print("Service discovery: instance 10.0.0.2 crashes (stops heartbeating)")
    print("=" * 76)
    for t in range(1, 8):
        for addr in ("10.0.0.1:8080", "10.0.0.3:8080"):
            reg.heartbeat("order-service", addr, now=t)
        reg.heartbeat("user-service", "10.0.1.1:8080", now=t)
        if t in (2, 5):
            print(f"  t={t}: healthy order-service instances = {reg.healthy('order-service', t)}")
    targets = {gw.handle("/orders/9", "valid-token", now=7 + i * 0.3).split("@")[-1] for i in range(4)}
    print(f"  requests at t=7 are routed only to: {sorted(targets)}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* Start with a (modular) monolith; split into services by bounded context
  when team size, independent scaling, or fault isolation justify it.
* Database per service; communicate via APIs and events; no shared tables.
* API gateway for auth, rate limiting, routing; keep business logic out of it.
* Service discovery via a registry with health checks (or Kubernetes
  Services); a service mesh handles retries/timeouts/mTLS/telemetry uniformly.
""")
