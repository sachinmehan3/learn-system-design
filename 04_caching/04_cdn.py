"""
LESSON 04.04 -- CONTENT DELIVERY NETWORKS (CDNs)
=================================================

WHAT A CDN IS
-------------
A globally distributed network of cache servers ("edge servers" grouped in
"Points of Presence", PoPs) that store copies of your content close to users.
Light in fiber takes ~150 ms for a round trip California <-> Europe; a PoP in
the user's city answers in ~10-20 ms.

WHAT IT GIVES YOU
  * LATENCY: content served from nearby.
  * OFFLOAD: your origin servers only see cache misses (often <5% of traffic).
  * BANDWIDTH COST: egress from the CDN is cheaper than from your origin.
  * RESILIENCE: absorbs traffic spikes and DDoS attacks; can serve stale
    content if the origin is down.
  * TLS termination and HTTP/2/3 near the user (fewer long round trips).

WHAT TO PUT ON IT
  * Static assets: images, video segments, JS/CSS, fonts, downloads. Always.
  * Cacheable API responses / whole pages (public, not per-user).
  * Increasingly: "edge compute" (Cloudflare Workers, Lambda@Edge) for
    auth checks, A/B routing, personalization at the edge.

PULL vs PUSH CDN
----------------
* PULL (origin pull): the edge fetches from your origin on the first miss,
  then caches per Cache-Control TTL. Zero upfront work; first user in each
  region pays the miss. Most common.
* PUSH: you upload content to the CDN ahead of time. Good for large, rarely
  changing files (software releases, video libraries) or predictable launches.

INVALIDATION / VERSIONING
-------------------------
* Purging a file from hundreds of PoPs is slow-ish and rate limited.
* Best practice: FINGERPRINTED URLs -- app.3f9a1c.js. The file name changes
  when the content changes, so you can cache "forever"
  (Cache-Control: max-age=31536000, immutable) and never purge.

ORIGIN SHIELD / TIERED CACHING
------------------------------
With 300 PoPs, a new file could cause 300 origin fetches. A mid-tier
"shield" cache sits between edges and origin so the origin sees ~1 fetch.

VIDEO STREAMING NOTE
--------------------
Videos are split into small segments (2-10 s) at multiple bitrates (HLS /
DASH). The player picks the bitrate per segment (adaptive bitrate). Each
segment is a small static file -> perfect for CDNs. This is how Netflix /
YouTube scale (Netflix even places its own caches inside ISPs: Open Connect).

The simulation: users in 4 regions request a skewed catalogue of files,
with and without a CDN (and with an origin shield).
"""

import itertools
import random

REGIONS = ["us-west", "us-east", "europe", "asia"]
ORIGIN_REGION = "us-east"

# Round-trip latency (ms) between regions. Same region ~ 10ms.
RTT = {
    ("us-west", "us-east"): 70, ("us-west", "europe"): 150, ("us-west", "asia"): 120,
    ("us-east", "europe"): 90, ("us-east", "asia"): 200, ("europe", "asia"): 180,
}


def rtt(a, b):
    if a == b:
        return 10
    return RTT.get((a, b)) or RTT[(b, a)]


class EdgeCache:
    def __init__(self, region, capacity):
        self.region, self.capacity = region, capacity
        self.store = {}

    def get(self, key):
        return key in self.store

    def put(self, key):
        if len(self.store) >= self.capacity:
            self.store.pop(next(iter(self.store)))   # FIFO for simplicity
        self.store[key] = True


def simulate(use_cdn, use_shield=False, n_requests=50_000, seed=8):
    rng = random.Random(seed)
    files = [f"/img/{i}.jpg" for i in range(5_000)]
    weights = [1 / (r ** 1.0) for r in range(1, len(files) + 1)]
    cum_weights = list(itertools.accumulate(weights))   # precompute once: much faster sampling
    edges = {r: EdgeCache(r, capacity=500) for r in REGIONS}
    shield = EdgeCache(ORIGIN_REGION, capacity=5_000)
    origin_hits, total_latency = 0, 0.0

    for _ in range(n_requests):
        region = rng.choice(REGIONS)
        f = rng.choices(files, cum_weights=cum_weights)[0]
        if not use_cdn:
            total_latency += rtt(region, ORIGIN_REGION)
            origin_hits += 1
            continue
        edge = edges[region]
        latency = rtt(region, region)                     # user -> nearby PoP
        if not edge.get(f):
            if use_shield:
                latency += rtt(region, ORIGIN_REGION)     # PoP -> shield (near origin)
                if not shield.get(f):
                    origin_hits += 1
                    latency += 5                          # shield -> origin (same DC)
                    shield.put(f)
            else:
                latency += rtt(region, ORIGIN_REGION)     # PoP -> origin
                origin_hits += 1
            edge.put(f)
        total_latency += latency
    return total_latency / n_requests, origin_hits


if __name__ == "__main__":
    print("=" * 72)
    print("50,000 image requests from 4 regions; origin server in us-east")
    print("=" * 72)
    for label, cdn, shield in (("no CDN", False, False), ("CDN (pull)", True, False),
                               ("CDN + origin shield", True, True)):
        avg, origin = simulate(cdn, shield)
        print(f"  {label:22s} avg latency={avg:6.1f} ms   origin requests={origin:6d} "
              f"({origin / 50_000:5.1%})")

    print("""
WHAT YOU SAW
------------
* The CDN roughly halved average latency and removed most origin traffic,
  even though each edge caches only 10% of the catalogue (popularity is skewed;
  real edges are far bigger and hit 95%+).
* The origin shield collapses the misses of ALL PoPs into ~one fetch per file,
  cutting origin load ~5x more, at the cost of a tiny bit of extra latency.

INTERVIEW TALKING POINTS
------------------------
* "Static content and media go to object storage (S3) fronted by a CDN."
* "Use fingerprinted asset URLs with long max-age so we never need purges."
* For video: chunked adaptive-bitrate segments served from the CDN.
* CDNs also absorb DDoS and traffic spikes -- part of the availability story.
""")
