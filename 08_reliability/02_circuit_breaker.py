"""
LESSON 08.02 -- THE CIRCUIT BREAKER PATTERN
============================================

THE PROBLEM: CASCADING FAILURES
-------------------------------
Service A calls service B. B becomes slow or starts failing. A keeps calling:
  * Each call waits for a timeout -> A's threads/connections pile up waiting
    (Little's law!) -> A runs out of threads -> A fails its own callers ->
    the failure spreads up the call graph. One sick service takes down many.
  * Meanwhile the flood of calls (and retries) keeps B from recovering.

THE CIRCUIT BREAKER (Michael Nygard, "Release It!"; Netflix Hystrix,
resilience4j, Polly, Envoy outlier detection)
------------------------------------------------------------------------
A proxy around remote calls with three states, like an electrical breaker:

        CLOSED --(failure rate over threshold)--> OPEN
          ^                                         |
          |                                   (cool-down timer expires)
     (trial calls succeed)                          v
          +------------------------------------ HALF-OPEN
                        (a trial call fails -> back to OPEN)

  * CLOSED: calls pass through; failures are counted over a sliding window.
  * OPEN: calls FAIL FAST immediately (no network call) -> return an error or
    a FALLBACK (cached value, default, degraded response). Gives B breathing room.
  * HALF-OPEN: after a cool-down, let a few trial requests through. Success ->
    CLOSED; failure -> OPEN again.

Tune: failure-rate threshold (e.g. 50% over the last 20 calls), minimum
number of calls before judging, cool-down (e.g. 30 s), which errors count
(timeouts and 5xx yes; 4xx no -- those are the caller's fault).

Combine with: timeouts (always!), bounded retries with backoff, bulkheads,
and fallbacks (next lessons).
"""

import random


class CircuitOpenError(Exception):
    pass


class CircuitBreaker:
    def __init__(self, failure_threshold=0.5, window=10, min_calls=5, cooldown=5.0, half_open_trials=2):
        self.failure_threshold = failure_threshold
        self.window, self.min_calls = window, min_calls
        self.cooldown, self.half_open_trials = cooldown, half_open_trials
        self.state = "CLOSED"
        self.results = []            # sliding window of recent outcomes (True = success)
        self.opened_at = 0.0
        self.trial_successes = 0
        self.transitions = []

    def _to(self, state, now):
        self.transitions.append((round(now, 1), f"{self.state} -> {state}"))
        self.state = state

    def call(self, fn, now):
        if self.state == "OPEN":
            if now - self.opened_at >= self.cooldown:
                self._to("HALF_OPEN", now)
                self.trial_successes = 0
            else:
                raise CircuitOpenError("fail fast")
        try:
            result = fn()
        except Exception:
            self._record(False, now)
            raise
        self._record(True, now)
        return result

    def _record(self, ok, now):
        if self.state == "HALF_OPEN":
            if not ok:
                self.opened_at = now
                self._to("OPEN", now)
                return
            self.trial_successes += 1
            if self.trial_successes >= self.half_open_trials:
                self.results = []
                self._to("CLOSED", now)
            return
        self.results = (self.results + [ok])[-self.window:]
        failures = self.results.count(False)
        if len(self.results) >= self.min_calls and failures / len(self.results) >= self.failure_threshold:
            self.opened_at = now
            self._to("OPEN", now)


class FlakyService:
    """Healthy, then an outage from t=10 to t=25 (each failing call burns a 2s timeout)."""

    def __init__(self, rng):
        self.rng = rng
        self.now = 0.0
        self.calls_received = 0

    def request(self):
        self.calls_received += 1
        if 10 <= self.now < 25 or self.rng.random() < 0.02:
            raise TimeoutError("timed out after 2s")
        return "ok"


def run(use_breaker):
    rng = random.Random(1)
    svc = FlakyService(rng)
    cb = CircuitBreaker()
    stats = {"ok": 0, "failed": 0, "fast_failed": 0, "time_blocked": 0.0}
    t = 0.0
    while t < 40:
        svc.now = t
        try:
            if use_breaker:
                cb.call(svc.request, t)
            else:
                svc.request()
            stats["ok"] += 1
        except CircuitOpenError:
            stats["fast_failed"] += 1          # instant; serve a fallback instead
        except TimeoutError:
            stats["failed"] += 1
            stats["time_blocked"] += 2.0       # a thread was stuck waiting for the timeout
        t += 0.2                               # 5 requests/second
    return stats, svc.calls_received, cb.transitions


if __name__ == "__main__":
    print("=" * 78)
    print("Downstream outage from t=10s to t=25s; 5 requests/s; each timeout blocks a thread 2s")
    print("=" * 78)
    for use in (False, True):
        stats, received, transitions = run(use)
        label = "WITH circuit breaker" if use else "WITHOUT breaker"
        print(f"\n  {label}:")
        print(f"    successes={stats['ok']}, slow timeouts={stats['failed']}, "
              f"instant fail-fast={stats['fast_failed']}")
        print(f"    thread-seconds wasted waiting on timeouts: {stats['time_blocked']:.0f}")
        print(f"    calls that hit the sick service: {received}")
        if transitions:
            print(f"    state changes: {transitions}")

    print("""
WHAT YOU SAW
------------
* Without a breaker, every call during the outage waited for a 2s timeout:
  ~150 thread-seconds burned and the sick service was hammered the whole time.
* With a breaker, after a few failures the circuit OPENED and calls failed
  fast (serve a fallback); it periodically probed (HALF_OPEN) and CLOSED again
  once the service recovered.

INTERVIEW TALKING POINTS
------------------------
* "Every synchronous call to a dependency has a timeout, bounded retries, and
   a circuit breaker with a fallback (cached data / degraded response)."
* Circuit breakers stop cascading failures and give the dependency time to heal.
* In a service mesh (Envoy/Istio) this is configured, not coded.
""")
