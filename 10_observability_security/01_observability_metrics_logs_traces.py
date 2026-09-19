"""
LESSON 10.01 -- OBSERVABILITY: METRICS, LOGS, TRACES, SLOs, AND ALERTING
========================================================================

You can't operate (or scale, or debug) what you can't see. In interviews,
mentioning monitoring in the wrap-up is expected; knowing HOW is a plus.

THE THREE PILLARS
-----------------
1) METRICS -- numbers over time, cheap to store and query.
   * Counters (requests_total), gauges (queue_depth, memory), HISTOGRAMS
     (request latency distribution -> percentiles).
   * Aggregated, low cardinality (don't put user_id in metric labels!).
   * Tools: Prometheus + Grafana, Datadog, CloudWatch.
   * What to measure:
       RED method (for services):  Rate, Errors, Duration.
       USE method (for resources): Utilization, Saturation, Errors.
       Google's FOUR GOLDEN SIGNALS: latency, traffic, errors, saturation.

2) LOGS -- timestamped records of discrete events.
   * STRUCTURED (JSON: {"level":"error","order_id":42,"trace_id":"abc"}) so
     they're searchable; include a request/trace ID for correlation.
   * High volume and cost -> sampling, retention tiers, log levels.
   * Tools: ELK/OpenSearch stack, Loki, Splunk, CloudWatch Logs.

3) DISTRIBUTED TRACES -- follow ONE request across many services.
   * A TRACE is a tree of SPANS (one per operation: "gateway", "auth",
     "db query"), each with start time, duration, parent span.
   * The trace ID is propagated in headers (W3C traceparent) through every
     hop, queue message, and async job.
   * Answers "WHY is this request slow?" -- which hop ate the time.
   * Usually SAMPLED (e.g. 1%, plus all errors / slow requests = tail sampling).
   * Tools: OpenTelemetry (the standard instrumentation), Jaeger, Zipkin,
     Tempo, Honeycomb, X-Ray.

SLOs AND ALERTING
-----------------
* Define SLIs/SLOs from the USER's perspective (see lesson 01.03): "99.9% of
  checkout requests succeed and complete in < 500 ms over 30 days."
* Alert on SYMPTOMS (SLO burn: users are hurting) rather than every cause
  (CPU 80% is not an emergency if users are fine). Page a human only for
  urgent, actionable problems.
* ERROR BUDGET BURN RATE: at 99.9%, the budget is 0.1% errors. A burn rate of
  14.4 (1.44% errors) would exhaust a 30-day budget in ~2 days -> page. A
  slow burn -> ticket. (Google SRE workbook multi-window burn-rate alerts.)

The demo: a mini metrics registry with a latency histogram (percentiles), a
toy distributed tracer with nested spans across "services", and an SLO
burn-rate check.
"""

import bisect
import contextlib
import json
import random
import time
import uuid


class Histogram:
    """Bucketed latency histogram like Prometheus: cheap to store, gives approximate percentiles."""

    BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, float("inf")]

    def __init__(self):
        self.counts = [0] * len(self.BUCKETS_MS)
        self.total = 0

    def observe(self, ms):
        self.counts[bisect.bisect_left(self.BUCKETS_MS, ms)] += 1
        self.total += 1

    def percentile(self, p):
        target, running = p / 100 * self.total, 0
        for upper, c in zip(self.BUCKETS_MS, self.counts):
            running += c
            if running >= target:
                return upper
        return float("inf")


class Tracer:
    def __init__(self):
        self.spans = []
        self.stack = []

    @contextlib.contextmanager
    def span(self, name, trace_id):
        parent = self.stack[-1] if self.stack else None
        span = {"trace_id": trace_id, "span_id": uuid.uuid4().hex[:6], "parent": parent and parent["span_id"],
                "name": name, "start": time.perf_counter(), "depth": len(self.stack)}
        self.stack.append(span)
        try:
            yield span
        finally:
            span["duration_ms"] = (time.perf_counter() - span["start"]) * 1000
            self.stack.pop()
            self.spans.append(span)

    def print_trace(self, trace_id):
        spans = sorted((s for s in self.spans if s["trace_id"] == trace_id), key=lambda s: s["start"])
        t0 = spans[0]["start"]
        for s in spans:
            offset = (s["start"] - t0) * 1000
            bar = " " * int(offset / 4) + "#" * max(1, int(s["duration_ms"] / 4))
            print(f"    {'  ' * s['depth'] + s['name']:28s} {s['duration_ms']:6.1f} ms  |{bar}")


def log(level, msg, **fields):
    """Structured logging: machine-parseable, correlated by trace_id."""
    print("    " + json.dumps({"level": level, "msg": msg, **fields}))


def handle_checkout(tracer, rng):
    trace_id = uuid.uuid4().hex[:8]                  # would come from the traceparent header
    with tracer.span("api-gateway /checkout", trace_id):
        with tracer.span("auth-service verify", trace_id):
            time.sleep(0.004)
        with tracer.span("cart-service get", trace_id):
            time.sleep(0.008)
            with tracer.span("redis GET cart:42", trace_id):
                time.sleep(0.002)
        with tracer.span("payment-service charge", trace_id):
            with tracer.span("postgres INSERT payment", trace_id):
                time.sleep(0.005)
            with tracer.span("stripe API call", trace_id):
                time.sleep(0.060 if rng.random() < 0.5 else 0.015)   # the slow external dependency
    return trace_id


if __name__ == "__main__":
    rng = random.Random(5)

    print("=" * 78)
    print("METRICS: latency histogram for 10,000 requests")
    print("=" * 78)
    h = Histogram()
    for _ in range(10_000):
        h.observe(rng.lognormvariate(3, 0.6) if rng.random() > 0.02 else rng.uniform(500, 2000))
    print(f"  bucket counts (<= ms): {dict(zip(h.BUCKETS_MS, h.counts))}")
    for p in (50, 90, 99, 99.9):
        print(f"  p{p:<4} <= {h.percentile(p)} ms")

    print()
    print("=" * 78)
    print("TRACES: where did the time go in one checkout request?")
    print("=" * 78)
    tracer = Tracer()
    tid = handle_checkout(tracer, rng)
    tracer.print_trace(tid)
    slowest = max((s for s in tracer.spans if s["trace_id"] == tid and s["depth"] > 1),
                  key=lambda s: s["duration_ms"])
    print(f"  -> slowest leaf span: {slowest['name']!r} -- the trace points straight at it.")

    print()
    print("=" * 78)
    print("LOGS: structured and correlated with the trace")
    print("=" * 78)
    log("info", "checkout started", trace_id=tid, user_id=42)
    log("warn", "payment provider slow", trace_id=tid, provider="stripe", latency_ms=round(slowest["duration_ms"]))

    print()
    print("=" * 78)
    print("SLO BURN RATE: SLO = 99.9% success over 30 days")
    print("=" * 78)
    budget = 0.001
    for window, error_rate in (("last 1h", 0.0005), ("last 1h", 0.004), ("last 5m", 0.02)):
        burn = error_rate / budget
        days_to_exhaust = 30 / burn if burn else float("inf")
        action = "PAGE on-call" if burn >= 14.4 else ("open a ticket" if burn >= 1 else "ok")
        print(f"  {window}: error rate {error_rate:.2%} -> burn rate {burn:5.1f}x "
              f"(budget gone in {days_to_exhaust:5.1f} days) -> {action}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "Each service exports RED metrics (rate, errors, duration histograms),
   emits structured logs with trace IDs, and is instrumented with
   OpenTelemetry; traces are sampled (all errors + slow requests)."
* Alert on user-facing SLO burn rate, not on every CPU spike.
* Dashboards per service: golden signals + dependency health + queue lag.
""")
