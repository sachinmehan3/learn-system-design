"""
CASE STUDY 02 -- DESIGN A NEWS FEED / TIMELINE (Twitter, Instagram, Facebook)
=============================================================================
The key question: when user X opens the app, how do we quickly build the
list of recent posts from everyone X follows?

STEP 1 -- REQUIREMENTS
----------------------
Functional: post content; follow users; see a home feed of posts from people
you follow, newest (or best-ranked) first, paginated.
Non-functional: feed load p99 < 200 ms; very read-heavy; high availability;
eventual consistency is FINE (a post appearing 5 s late is OK); follower
counts are extremely skewed (most users have <1000 followers; celebrities
have 100M+).

STEP 2 -- ESTIMATION
--------------------
  200M DAU; each posts ~2/day -> ~5k posts/s; each opens the feed ~10x/day
  -> ~25k feed reads/s (peak ~75k). Average ~200 followees per user.

API
  POST /v1/posts {text, media_ids}
  GET  /v1/feed?cursor=<last_post_id>&limit=20
  POST /v1/users/{id}/follow

DATA
  posts(post_id [Snowflake, time-sortable], author_id, text, media_urls, created_at)
      -> sharded by author_id (or post_id)
  follows(follower_id, followee_id) -> both directions indexed ("who do I
      follow", "who follows me")
  feed cache: user_id -> list of post_ids (Redis list / sorted set, capped at ~800)
  Media in object storage (S3) + CDN; only URLs in the DB.

STEP 3 -- HIGH-LEVEL DESIGN AND THE CORE TRADE-OFF
--------------------------------------------------
FAN-OUT ON READ (pull model)
  At feed-read time: fetch recent posts of every followee, merge, sort.
  + Writes are cheap (store one post). + No wasted work for inactive users.
  - Reads are expensive: ~200 queries (scatter-gather across shards) per
    feed load, x 25k/s. Slow and heavy.

FAN-OUT ON WRITE (push model)
  At post time: push the post_id into the feed cache of EVERY follower
  (async, via a queue + fan-out workers).
  + Reads are one cache lookup: super fast.
  - Write amplification: 1 post x avg 200 followers = 200 cache writes;
    a celebrity with 100M followers = 100M writes per post (minutes of lag,
    huge cost), and wasted work for followers who never log in.

HYBRID (what Twitter/Instagram actually do)
  * Normal users: fan-out on write.
  * Celebrities (followers > threshold): NOT fanned out. At read time, merge
    the precomputed feed with recent posts pulled from the few celebrities
    the user follows.
  * Skip fan-out to inactive users; build their feed on demand when they return.

  post --> Post service --> posts DB
                  |
                  +--> Kafka "new_post" --> fan-out workers --> follower feed caches (Redis)
  feed read --> Feed service --> read feed cache + pull celebrity posts --> merge/rank
            --> hydrate post_ids with post/user data (caches) --> response

STEP 4 -- DEEP DIVES
--------------------
* RANKING: chronological is simple; ranked feeds score candidates with an ML
  model (engagement likelihood, recency, affinity) -- candidate generation
  (the fan-out result) then a ranking stage.
* PAGINATION: cursor = last seen post_id (time-sortable Snowflake IDs make
  "WHERE post_id < cursor" natural).
* HYDRATION: feeds store only IDs; post bodies and author profiles come from
  their own caches (multi-get), so edits/deletes show up everywhere.
* DELETES / UNFOLLOWS: filter at read time (check post still exists /
  author still followed) and clean up lazily.
* MEDIA: upload directly to S3 via pre-signed URL; CDN for delivery.
* CACHE SIZING: keep only the latest ~800 IDs per active user in Redis.

The simulation compares write cost, read cost and celebrity lag for the
three strategies on a synthetic social graph with a power-law follower distribution.
"""

import heapq
import random
from collections import defaultdict, deque

FEED_SIZE = 50
CELEBRITY_THRESHOLD = 1000


class SocialNetwork:
    def __init__(self, n_users, rng):
        self.followers = defaultdict(set)    # author -> set of followers
        self.following = defaultdict(set)    # user -> set of authors they follow
        # Power-law popularity: a few users are followed by very many.
        weights = [1 / (r ** 1.2) for r in range(1, n_users + 1)]
        for user in range(n_users):
            for author in set(rng.choices(range(n_users), weights=weights, k=30)):
                if author != user:
                    self.followers[author].add(user)
                    self.following[user].add(author)
        self.posts_by_author = defaultdict(list)   # author -> [(post_id, author)], newest last
        self.next_post_id = 0

    def new_post(self, author):
        self.next_post_id += 1
        self.posts_by_author[author].append(self.next_post_id)
        return self.next_post_id


class PullFeed:
    name = "fan-out on READ (pull)"

    def __init__(self, net):
        self.net, self.write_ops, self.read_ops = net, 0, 0

    def on_post(self, author, post_id):
        self.write_ops += 1                                  # just store the post

    def read_feed(self, user):
        followees = self.net.following[user]
        self.read_ops += len(followees)                      # one query per followee
        recent = (self.net.posts_by_author[a][-FEED_SIZE:] for a in followees)
        return heapq.nlargest(FEED_SIZE, (p for posts in recent for p in posts))


class PushFeed:
    name = "fan-out on WRITE (push)"

    def __init__(self, net):
        self.net, self.write_ops, self.read_ops = net, 0, 0
        self.feeds = defaultdict(lambda: deque(maxlen=FEED_SIZE))

    def on_post(self, author, post_id):
        self.write_ops += 1
        for follower in self.net.followers[author]:          # write into every follower's feed
            self.feeds[follower].appendleft(post_id)
            self.write_ops += 1

    def read_feed(self, user):
        self.read_ops += 1                                   # one cache read
        return list(self.feeds[user])


class HybridFeed(PushFeed):
    def __init__(self, net, threshold=CELEBRITY_THRESHOLD):
        super().__init__(net)
        self.threshold = threshold
        self.name = f"HYBRID (celeb >= {threshold:,})"

    def is_celebrity(self, author):
        return len(self.net.followers[author]) >= self.threshold

    def on_post(self, author, post_id):
        self.write_ops += 1
        if self.is_celebrity(author):
            return                                           # celebrities are pulled at read time
        for follower in self.net.followers[author]:
            self.feeds[follower].appendleft(post_id)
            self.write_ops += 1

    def read_feed(self, user):
        self.read_ops += 1
        celebs = [a for a in self.net.following[user] if self.is_celebrity(a)]
        self.read_ops += len(celebs)
        pulled = [p for a in celebs for p in self.net.posts_by_author[a][-FEED_SIZE:]]
        return heapq.nlargest(FEED_SIZE, list(self.feeds[user]) + pulled)


if __name__ == "__main__":
    rng = random.Random(3)
    net = SocialNetwork(20_000, rng)
    counts = sorted((len(f) for f in net.followers.values()), reverse=True)
    celebs = sum(1 for c in counts if c >= CELEBRITY_THRESHOLD)
    print("=" * 84)
    print(f"Synthetic network: 20,000 users; top follower counts {counts[:5]}; median {counts[len(counts) // 2]}")
    print(f"{celebs} 'celebrities' (>= {CELEBRITY_THRESHOLD} followers)")
    print("=" * 84)

    strategies = [PullFeed(net), PushFeed(net),
                  HybridFeed(net, 1_000), HybridFeed(net, 5_000), HybridFeed(net, 10_000)]
    authors = rng.choices(range(20_000), k=5_000)            # 5,000 posts...
    authors += list(range(5))                                # ...including posts by the top celebrities
    for a in authors:
        pid = net.new_post(a)
        for s in strategies:
            s.on_post(a, pid)
    readers = rng.choices(range(20_000), k=20_000)           # 20,000 feed loads (read-heavy)
    for s in strategies:
        for u in readers:
            s.read_feed(u)

    print(f"  {'strategy':30s} {'write ops':>12s} {'read ops':>12s} {'ops per feed load':>18s}")
    for s in strategies:
        print(f"  {s.name:30s} {s.write_ops:12,d} {s.read_ops:12,d} {s.read_ops / len(readers):18.1f}")

    sample = readers[0]
    same = PullFeed(net).read_feed(sample) == strategies[2].read_feed(sample)
    print(f"\n  sanity check: hybrid feed == pull feed for user {sample}: {same}")
    top = max(net.followers, key=lambda a: len(net.followers[a]))
    print(f"  one post by the top celebrity (user {top}) costs push {len(net.followers[top]):,} feed writes; "
          "hybrid: 1.")

    print("""
WHAT YOU SAW
------------
* Pull: cheap writes, but every feed load queries dozens of authors.
* Push: 1 cache read per feed load, but huge write amplification driven by
  celebrities.
* Hybrid: in between, and the CELEBRITY THRESHOLD is the dial. A low threshold
  pulls many authors at read time (cheaper writes, costlier reads); a high one
  pushes more (cheap reads, but the biggest accounts' posts are never fanned
  out). Pick it from a cost model of followers x post rate -- the industry answer.

FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How do you pick the celebrity threshold? (cost model: followers x post rate)
* What about inactive users? (don't fan out to them; rebuild on login)
* How is the feed ranked? (candidate generation + ML ranking service)
* How to handle deletes/edits/blocks? (store IDs only; filter at hydration)
* What if the fan-out queue lags? (feed is eventually consistent; show the
  author their own post immediately -- read-your-writes)
""")
