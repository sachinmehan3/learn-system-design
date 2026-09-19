"""
CASE STUDY 04 -- DESIGN SEARCH AUTOCOMPLETE / TYPEAHEAD
=======================================================

STEP 1 -- REQUIREMENTS
----------------------
Functional: as the user types a prefix, return the top 5-10 most popular
completions. (Optionally personalised, multi-language, trending.)
Non-functional: VERY low latency -- results must feel instant (< 100 ms
end-to-end, so the service gets ~10-20 ms); very high QPS (every keystroke
is a request); results can be slightly stale (updated hourly/daily is fine);
highly available.

STEP 2 -- ESTIMATION
--------------------
  10M DAU x 10 searches x ~5 keystroke requests = 500M requests/day ->
  ~6k QPS avg, ~20k peak. Mostly READS; the data changes slowly.
  Distinct queries: maybe 100M -> the trie must be sharded or pruned.

STEP 3 -- HIGH-LEVEL DESIGN: TWO SEPARATE PATHS
-----------------------------------------------
  QUERY PATH (fast, read-only):
     client (debounce ~100ms, local cache) -> CDN / edge cache (popular
     prefixes!) -> LB -> Autocomplete service -> in-memory TRIE with
     precomputed top-k per node -> response

  DATA PATH (offline / near-real-time):
     search logs -> Kafka -> aggregation (Spark/Flink: counts per query per
     time window, with decay so old queries fade) -> trie BUILDER -> new
     immutable trie snapshot -> published to autocomplete servers (swap
     atomically, blue/green)

THE TRIE (prefix tree) AND THE KEY OPTIMISATION
-----------------------------------------------
Naive: walk to the prefix node, then DFS the whole subtree, collect all
completions, sort by count, take top k. For a short prefix like "s", the
subtree has millions of entries -> far too slow.
OPTIMISATION: store the TOP-K completions AT EVERY NODE, precomputed at
build time. A query is then: walk len(prefix) nodes, return the stored list.
O(prefix length) per query. Costs extra memory (k entries per node) and
rebuild time -- a classic space-for-time trade-off, fine because updates are
batched offline.

STEP 4 -- DEEP DIVES
--------------------
* SHARDING: by first character(s) of the prefix -> uneven ("s" >> "x");
  better: split ranges by observed traffic ("a-ab", "ac-an", ...), via a
  shard map. Or replicate the whole (pruned) trie on every server if it fits
  in RAM -- simplest, and top-k-per-node tries prune easily.
* CACHING: popular prefixes are extremely repetitive -> cache responses at the
  CDN/edge with short TTLs; browser caches too. Most traffic never reaches
  the service.
* FRESHNESS / TRENDING: a small real-time layer (count-min sketch over the
  last hour, lesson 06.06) merged with the daily trie for breaking news.
* FILTERING: remove offensive/illegal suggestions at build time + a
  blocklist checked at query time (to react immediately).
* CLIENT: debounce keystrokes, cancel stale in-flight requests, cache
  results per prefix (typing "sys" after "sy" can filter locally).
"""

import heapq
import random
import time

TOP_K = 5


class TrieNode:
    __slots__ = ("children", "count", "top")

    def __init__(self):
        self.children = {}
        self.count = 0          # >0 if a complete query ends here
        self.top = []           # precomputed [(count, query)] top-k for this prefix


class AutocompleteTrie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, query, count):
        node = self.root
        for ch in query:
            node = node.children.setdefault(ch, TrieNode())
        node.count += count

    def build_top_k(self):
        """Post-order pass: each node's top-k = best of its own count + children's top-k."""
        def visit(node, prefix):
            candidates = [(node.count, prefix)] if node.count else []
            for ch, child in node.children.items():
                visit(child, prefix + ch)
                candidates.extend(child.top)
            node.top = heapq.nlargest(TOP_K, candidates)
        visit(self.root, "")

    def suggest_fast(self, prefix):
        node = self.root
        for ch in prefix:
            node = node.children.get(ch)
            if node is None:
                return []
        return [q for _, q in node.top]           # O(len(prefix)) -- just read the list

    def suggest_naive(self, prefix):
        node = self.root
        for ch in prefix:
            node = node.children.get(ch)
            if node is None:
                return []
        results, stack = [], [(node, prefix)]
        while stack:                              # DFS the entire subtree
            n, p = stack.pop()
            if n.count:
                results.append((n.count, p))
            stack.extend((c, p + ch) for ch, c in n.children.items())
        return [q for _, q in heapq.nlargest(TOP_K, results)]


def synthetic_query_log(rng, n_distinct=200_000):
    syllables = ["sys", "tem", "de", "sign", "in", "ter", "view", "da", "ta", "base", "ca", "che",
                 "red", "is", "kaf", "ka", "sca", "le", "net", "work", "py", "thon", "go", "java"]
    queries = {}
    for rank in range(1, n_distinct + 1):
        q = "".join(rng.choice(syllables) for _ in range(rng.randint(2, 4)))
        queries[q] = queries.get(q, 0) + int(1_000_000 / rank ** 0.9)   # Zipf-ish popularity
    queries.update({"system design": 2_000_000, "system design interview": 1_500_000,
                    "systemd": 300_000, "system of a down": 900_000})
    return queries


if __name__ == "__main__":
    rng = random.Random(6)
    log = synthetic_query_log(rng)

    t = time.perf_counter()
    trie = AutocompleteTrie()
    for q, c in log.items():
        trie.insert(q, c)
    trie.build_top_k()
    print("=" * 78)
    print(f"Built trie from {len(log):,} distinct queries (+ top-{TOP_K} per node) "
          f"in {time.perf_counter() - t:.1f}s  <- offline job")
    print("=" * 78)

    for prefix in ("s", "sys", "syst", "system d", "kaf", "zzz"):
        print(f"  {prefix!r:12s} -> {trie.suggest_fast(prefix)}")

    print()
    print("=" * 78)
    print("Query latency: precomputed top-k vs naive subtree scan")
    print("=" * 78)
    for prefix in ("s", "sys", "systemde"):
        t = time.perf_counter()
        for _ in range(200):
            fast = trie.suggest_fast(prefix)
        fast_us = (time.perf_counter() - t) / 200 * 1e6
        t = time.perf_counter()
        for _ in range(3):
            naive = trie.suggest_naive(prefix)
        naive_us = (time.perf_counter() - t) / 3 * 1e6
        print(f"  prefix {prefix!r:11s}: fast {fast_us:8.1f} us   naive {naive_us:12,.0f} us   "
              f"same answer: {fast == naive}")

    print("""
FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How do updates reach the servers? (offline rebuild of an immutable trie
  snapshot, then atomic swap; plus a small real-time trending layer)
* How do you shard? (by prefix ranges balanced by traffic, or full replicas)
* How do you keep latency low at 20k QPS? (edge caching of popular prefixes,
  client debouncing, in-memory data, precomputed top-k)
* Personalisation? (blend global top-k with the user's own history at query time)
""")
