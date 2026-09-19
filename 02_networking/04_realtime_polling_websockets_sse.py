"""
LESSON 02.04 -- REAL-TIME COMMUNICATION: POLLING, LONG POLLING, SSE, WEBSOCKETS
===============================================================================

THE PROBLEM
-----------
HTTP is client-initiated: the server can only speak when asked. But chat
messages, notifications, live scores, stock prices, and collaborative editing
need the SERVER to push updates as soon as they happen. Four techniques:

1) SHORT POLLING
   Client asks "anything new?" every N seconds.
     + Trivial; works everywhere; stateless servers.
     - Wasteful: most responses are "no". Latency up to N seconds.
       1M clients polling every 5 s = 200k req/s of mostly-empty responses!

2) LONG POLLING
   Client asks; server HOLDS the request open until there's data (or a
   timeout ~30 s), then responds; client immediately re-asks.
     + Near-instant delivery; works over plain HTTP everywhere.
     - One held connection per client; reconnect overhead after each message;
       message ordering/duplication across reconnects needs care.

3) SERVER-SENT EVENTS (SSE)
   One long-lived HTTP response that the server streams events down
   ("text/event-stream"). One-way: server -> client. Browser auto-reconnects
   and resumes via Last-Event-ID.
     + Simple, HTTP-native, works through most proxies.
     - Server-to-client only (client uses normal requests to send).
   Good for: notifications, live feeds, dashboards, LLM token streaming.

4) WEBSOCKETS
   Starts as an HTTP request with "Upgrade: websocket", then becomes a
   persistent, FULL-DUPLEX (both directions) TCP connection with tiny framing.
     + Lowest latency and overhead; bidirectional.
     - STATEFUL connections: load balancing, scaling, and deploys are harder.
       Each server holds many open sockets (a well-tuned server can hold
       100k-1M+). You need to know WHICH server holds a user's connection to
       deliver a message to them (see the chat case study: a "connection
       registry" / pub-sub between gateway servers).
   Good for: chat, multiplayer games, collaborative editing, trading.

(Also: WebRTC for peer-to-peer audio/video; mobile PUSH NOTIFICATIONS
(APNs/FCM) when the app isn't open.)

DECISION GUIDE
    Updates rare, latency not critical       -> short polling (simplest!)
    Server -> client only                    -> SSE
    Bidirectional, low latency, high volume  -> WebSockets
    Must work through ancient proxies        -> long polling (fallback)

The simulation below delivers the same 5 server events to one client using
each technique, counting requests made and delivery delay.
"""

import queue
import threading
import time

EVENT_TIMES = [0.30, 0.35, 1.10, 1.80, 2.50]   # when the server produces events (seconds)
SIM_DURATION = 3.0


class EventSource:
    """Server side: produces events at fixed times into a queue."""

    def __init__(self):
        self.q = queue.Queue()
        self.start = time.monotonic()

    def run(self):
        for i, t in enumerate(EVENT_TIMES):
            time.sleep(max(0, t - (time.monotonic() - self.start)))
            self.q.put((f"event{i}", time.monotonic()))

    def elapsed(self):
        return time.monotonic() - self.start


def drain(q):
    items = []
    while True:
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            return items


def short_polling(interval):
    src = EventSource()
    threading.Thread(target=src.run, daemon=True).start()
    requests, delays = 0, []
    while src.elapsed() < SIM_DURATION:
        time.sleep(interval)
        requests += 1                                   # one full HTTP request/response
        for _, produced_at in drain(src.q):
            delays.append(time.monotonic() - produced_at)
    return requests, delays


def long_polling(timeout=1.0):
    src = EventSource()
    threading.Thread(target=src.run, daemon=True).start()
    requests, delays = 0, []
    while src.elapsed() < SIM_DURATION:
        requests += 1
        try:
            # Server holds the request until an event arrives or the timeout passes.
            _, produced_at = src.q.get(timeout=timeout)
            delays.append(time.monotonic() - produced_at)
            for _, p in drain(src.q):                    # batch anything else pending
                delays.append(time.monotonic() - p)
        except queue.Empty:
            pass                                         # empty response; client re-asks
    return requests, delays


def push_stream():
    """SSE / WebSocket: one connection; server pushes each event immediately."""
    src = EventSource()
    threading.Thread(target=src.run, daemon=True).start()
    requests, delays = 1, []                             # a single connection
    while src.elapsed() < SIM_DURATION:
        try:
            _, produced_at = src.q.get(timeout=0.05)
            delays.append(time.monotonic() - produced_at)
        except queue.Empty:
            pass
    return requests, delays


def report(name, requests, delays):
    avg = 1000 * sum(delays) / len(delays) if delays else float("nan")
    worst = 1000 * max(delays) if delays else float("nan")
    print(f"  {name:28s} requests={requests:3d}  delivered={len(delays)}/5  "
          f"avg delay={avg:6.0f} ms  worst={worst:6.0f} ms")


if __name__ == "__main__":
    print("=" * 78)
    print(f"Delivering {len(EVENT_TIMES)} server events over {SIM_DURATION}s to ONE client")
    print("=" * 78)
    report("short polling every 0.1s", *short_polling(0.1))
    report("short polling every 1.0s", *short_polling(1.0))
    report("long polling (1s timeout)", *long_polling())
    report("SSE / WebSocket push", *push_stream())

    print("""
WHAT YOU SAW
------------
* Fast short polling: low delay, but ~30 requests for 5 events (mostly empty).
* Slow short polling: few requests, but up to ~1 s delay.
* Long polling: few requests AND low delay.
* Push (SSE/WebSocket): one connection, near-zero delay.
Now multiply "requests" by 10 million users to see why this matters.

INTERVIEW TALKING POINTS
------------------------
* Chat / collaborative apps -> WebSockets, with a gateway tier that holds
  connections and a pub/sub layer (Redis / Kafka) to route messages to
  whichever gateway holds the recipient's socket.
* Notifications / live feed updates -> SSE is often enough and simpler.
* Stateful connections complicate deploys: drain connections gracefully and
  have clients reconnect with backoff + resume from the last event ID.
""")
