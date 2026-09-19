"""
CASE STUDY 05 -- DESIGN A WEB CRAWLER (Googlebot, Common Crawl)
===============================================================

STEP 1 -- REQUIREMENTS
----------------------
Functional: start from seed URLs, download pages, extract links, repeat;
store page content for indexing. Respect robots.txt. Re-crawl pages
periodically (freshness).
Non-functional: SCALE (billions of pages), POLITENESS (never overload a
single website), ROBUSTNESS (malformed HTML, crawler traps, slow servers,
infinite calendars), EXTENSIBILITY (new content types), deduplication.

STEP 2 -- ESTIMATION
--------------------
  1B pages/month -> ~400 pages/s (peak ~800). Avg page 500 KB -> 200 MB/s
  download, ~500 TB/month raw (compress + dedupe). URL "seen" set of
  billions of entries -> Bloom filter or sharded KV store.

STEP 3 -- HIGH-LEVEL DESIGN
---------------------------
  seeds -> [URL FRONTIER] -> fetcher workers -> DNS resolver (cached!)
               ^                    |
               |                    v
               |              content parser -> content dedup (hash / SimHash)
               |                    |                 |
               |                    v                 v
               +---- URL filter <- link extractor   content store (S3 / Bigtable)
                     (normalise, robots.txt,               |
                      seen-URL Bloom filter)               v
                                                     indexer (search)

THE URL FRONTIER (the heart of the design)
------------------------------------------
Not a single FIFO queue. It must handle PRIORITY and POLITENESS:
  * FRONT QUEUES (priority): URLs are bucketed by importance (PageRank,
    update frequency, domain authority). Higher-priority queues are drained
    more often.
  * BACK QUEUES (politeness): ONE queue per HOST, each mapped to a single
    worker, with a minimum delay between requests to the same host (and
    obey robots.txt Crawl-delay). A heap of "host -> next allowed fetch
    time" decides which host is ready next.
  This mirrors the Mercator crawler design.

STEP 4 -- DEEP DIVES
--------------------
* DEDUP OF URLs: normalise (lowercase host, strip fragments, sort query
  params, remove tracking params), then check a Bloom filter (billions of
  URLs in a few GB; a false positive only means skipping one page).
* DEDUP OF CONTENT: ~30% of the web is duplicate/near-duplicate. Exact:
  hash of content. Near-duplicate: SimHash / MinHash fingerprints.
* CRAWLER TRAPS: infinite URL spaces (calendars, session IDs in URLs) ->
  max URL length, max depth per site, max pages per host, pattern detection.
* DNS: resolution is slow (tens of ms+) -> local DNS cache per worker.
* DISTRIBUTION: partition the frontier BY HOST (consistent hashing on host)
  so politeness is enforced locally by the one worker that owns the host.
* FRESHNESS: re-crawl schedule based on observed change rate (news sites
  hourly, static pages monthly); use If-Modified-Since / ETags.
* FAULT TOLERANCE: frontier persisted (Kafka / disk-backed queues), workers
  stateless, checkpoints.

The simulation crawls a synthetic web graph with per-host politeness
delays, URL normalisation, a seen-set, and content dedup.
"""

import hashlib
import heapq
import random
from collections import defaultdict, deque
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "ref", "sessionid"}


def normalise(url):
    """Canonical form so trivially different URLs are recognised as the same page."""
    parts = urlsplit(url)
    query = urlencode(sorted((k, v) for k, v in parse_qsl(parts.query) if k not in TRACKING_PARAMS))
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, query, ""))


class SyntheticWeb:
    """A fake internet: 30 hosts, pages linking to each other, some duplicate content."""

    def __init__(self, rng, hosts=30, pages_per_host=40):
        self.rng = rng
        self.hosts = [f"site{i}.com" for i in range(hosts)]
        self.pages = {}
        for h in self.hosts:
            for p in range(pages_per_host):
                self.pages[f"https://{h}/page{p}"] = None
        urls = list(self.pages)
        for u in urls:
            links = rng.sample(urls, 6)
            # Messy real-world links: tracking params, fragments, case, trailing slashes.
            host1 = urlsplit(links[1]).netloc
            messy = [links[0] + "?utm_source=twitter",
                     links[1].replace(host1, host1.upper()) + "/",        # SITE3.COM/page7/
                     links[2] + "#section-2"] + links[3:]
            body = f"content of {u}" if rng.random() > 0.2 else "COPYRIGHT BOILERPLATE MIRROR PAGE"
            self.pages[u] = (body, messy)
        self.fetch_log = []

    def fetch(self, url, now):
        self.fetch_log.append((now, urlsplit(url).netloc))
        page = self.pages.get(url)
        return page if page else ("404", [])


class Frontier:
    """Per-host back queues + a heap of when each host may next be fetched (politeness)."""

    def __init__(self, politeness_delay):
        self.delay = politeness_delay
        self.host_queues = defaultdict(deque)
        self.ready_heap = []                       # (next_allowed_time, host)
        self.scheduled = set()
        self.last_fetch = {}                       # host -> time of our last request to it

    def add(self, url, now):
        host = urlsplit(url).netloc
        self.host_queues[host].append(url)
        if host not in self.scheduled:
            # Even a host with an empty queue must rest if we hit it recently.
            allowed = max(now, self.last_fetch.get(host, -self.delay) + self.delay)
            heapq.heappush(self.ready_heap, (allowed, host))
            self.scheduled.add(host)

    def next_url(self, now):
        if not self.ready_heap or self.ready_heap[0][0] > now:
            return None                            # nothing polite to fetch right now
        _, host = heapq.heappop(self.ready_heap)
        url = self.host_queues[host].popleft()
        self.last_fetch[host] = now
        if self.host_queues[host]:
            heapq.heappush(self.ready_heap, (now + self.delay, host))   # host must rest
        else:
            self.scheduled.discard(host)
        return url


def crawl(web, seeds, max_pages=600, politeness_delay=1.0, workers=8):
    frontier = Frontier(politeness_delay)
    seen_urls, seen_content = set(), set()
    stats = {"fetched": 0, "dup_urls_skipped": 0, "dup_content": 0}
    for s in seeds:
        seen_urls.add(normalise(s))
        frontier.add(normalise(s), 0)
    now = 0.0
    while stats["fetched"] < max_pages and (frontier.ready_heap):
        for _ in range(workers):                    # each tick, every worker may fetch one URL
            url = frontier.next_url(now)
            if url is None:
                break
            body, links = web.fetch(url, now)
            stats["fetched"] += 1
            digest = hashlib.sha1(body.encode()).hexdigest()
            if digest in seen_content:
                stats["dup_content"] += 1           # store once; don't re-index mirrors
            seen_content.add(digest)
            for link in links:
                n = normalise(link)
                if n in seen_urls:
                    stats["dup_urls_skipped"] += 1
                    continue
                seen_urls.add(n)
                frontier.add(n, now)
        now += 0.1                                  # 100 ms per tick
    return stats, now


if __name__ == "__main__":
    rng = random.Random(8)
    web = SyntheticWeb(rng)
    print("=" * 78)
    print("URL normalisation")
    print("=" * 78)
    for u in ("HTTPS://Site1.com/page3/?utm_source=x&b=2&a=1#top", "https://site1.com/page3?a=1&b=2"):
        print(f"  {u:52s} -> {normalise(u)}")

    print()
    print("=" * 78)
    print("Crawling a synthetic web of 30 hosts x 40 pages with 8 workers")
    print("=" * 78)
    stats, elapsed = crawl(web, ["https://site0.com/page0", "https://site7.com/page3"])
    print(f"  fetched {stats['fetched']} pages in {elapsed:.1f}s of simulated time")
    print(f"  duplicate URLs skipped (after normalisation): {stats['dup_urls_skipped']}")
    print(f"  duplicate content detected: {stats['dup_content']} pages")

    per_host = defaultdict(list)
    for t, host in web.fetch_log:
        per_host[host].append(t)
    min_gap = min(b - a for times in per_host.values() for a, b in zip(times, times[1:]))
    print(f"  smallest gap between two requests to the SAME host: {min_gap:.1f}s "
          f"(politeness delay = 1.0s)")
    print(f"  hosts crawled in parallel: {len(per_host)}")

    print("""
FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How do you avoid hammering one site? (per-host queues + delay; host-based
  partitioning so one worker owns each host; robots.txt Crawl-delay)
* How do you store billions of seen URLs? (Bloom filter / sharded KV)
* How do you detect near-duplicate pages? (SimHash/MinHash)
* How do you prioritise? (front queues by PageRank/freshness)
* Crawler traps? (depth/URL-length/pages-per-host limits)
""")
