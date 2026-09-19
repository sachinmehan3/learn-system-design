"""
LESSON 07.04 -- BACKPRESSURE, LOAD LEVELING, AND LOAD SHEDDING
===============================================================

THE PROBLEM
-----------
Producers can generate work faster than consumers can handle it (a traffic
spike, a slow downstream, a flash sale). Something has to give. Options:

1) BUFFER (queue): absorb the spike and drain it later -> LOAD LEVELING.
   Works when spikes are temporary and the AVERAGE rate < consumer capacity.
   An UNBOUNDED buffer under sustained overload just grows until memory runs
   out -- and every item waits longer and longer (latency -> infinity).

2) BACKPRESSURE: slow the producer down. A bounded queue that BLOCKS (or
   rejects) when full propagates "slow down" upstream. TCP flow control,
   reactive streams, Kafka consumers pulling at their own pace (pull-based
   systems have natural backpressure).

3) LOAD SHEDDING: deliberately drop/reject some work (HTTP 429/503) so the
   rest is served well. Better to serve 80% of users fast than 100% of users
   with timeouts. Shed the LEAST important work first (analytics before
   checkout); drop requests that have already waited too long (the client
   gave up anyway -- "deadline propagation").

4) SCALE CONSUMERS: autoscale on queue depth / consumer lag. Takes minutes,
   so you still need 1-3 for the transition.

KEY METRIC FOR QUEUES: consumer LAG (how far behind) and age of the oldest
message. Alert when they grow steadily.

The simulation: a consumer handles 100 msgs/s. Traffic is 50/s with a
10-second spike to 200/s, then a period of SUSTAINED overload at 150/s.
Compare an unbounded queue, a bounded queue that sheds load, and autoscaling.
"""


def traffic(t):
    if 10 <= t < 20:
        return 200           # short spike
    if 40 <= t < 70:
        return 150           # sustained overload
    return 50


def simulate(policy, duration=90, capacity_per_consumer=100, max_queue=500):
    queue = 0.0
    consumers = 1
    dropped = 0
    worst_wait = 0.0
    timeline = []
    for t in range(duration):
        arriving = traffic(t)
        if policy == "bounded+shed":
            space = max_queue - queue
            accepted = min(arriving, space)
            dropped += arriving - accepted
            queue += accepted
        else:
            queue += arriving
        if policy == "autoscale" and t % 5 == 0:          # autoscaler evaluates every 5 s
            if queue > 200:
                consumers += 1                             # scale out on queue depth
            elif consumers > 1 and arriving < (consumers - 1) * capacity_per_consumer * 0.7:
                consumers -= 1                             # scale in when capacity is idle
        queue = max(0.0, queue - consumers * capacity_per_consumer)
        wait = queue / (consumers * capacity_per_consumer)   # seconds a new message waits
        worst_wait = max(worst_wait, wait)
        if t % 10 == 9:
            timeline.append(f"t={t + 1:2d}s q={int(queue):5d} c={consumers}")
    return worst_wait, int(dropped), timeline


if __name__ == "__main__":
    print("=" * 88)
    print("Consumer capacity 100 msg/s. Traffic: 50/s, spike 200/s (t=10-20), overload 150/s (t=40-70)")
    print("=" * 88)
    for policy in ("unbounded", "bounded+shed", "autoscale"):
        worst, dropped, timeline = simulate(policy)
        print(f"\n  {policy}: worst queueing delay = {worst:5.1f}s, dropped = {dropped}")
        print("    " + " | ".join(timeline[:5]))
        print("    " + " | ".join(timeline[5:]))

    print("""
WHAT YOU SAW
------------
* unbounded: the short spike was absorbed and drained by t=40 (load leveling!),
  but during sustained overload the queue kept growing and messages waited ~15s
  (and it would grow forever if the overload never ended).
* bounded+shed: delay capped at a few seconds by rejecting the excess (clients
  get 429 and can retry later / degrade gracefully).
* autoscale: consumers were added when the queue grew and removed when idle --
  no drops, bounded delay. But it needs the buffer to survive until new
  capacity arrives (real autoscaling takes minutes, not seconds).

INTERVIEW TALKING POINTS
------------------------
* "A queue between the API and workers levels load spikes; workers autoscale
   on queue depth / consumer lag."
* Queues must be BOUNDED; under sustained overload, apply backpressure or shed
  load (429/503) rather than let latency grow without limit.
* Prioritise: shed low-value work first; drop requests past their deadline.
""")
