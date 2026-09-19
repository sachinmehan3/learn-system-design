"""
LESSON 05.01 -- SQL vs NoSQL, DATA MODELS, NORMALIZATION vs DENORMALIZATION
============================================================================

"Which database would you use and why?" comes up in EVERY design interview.
The answer is driven by: data shape, access patterns, consistency needs,
and scale. Know the families:

RELATIONAL (SQL): PostgreSQL, MySQL, SQL Server, Oracle; NewSQL: Spanner,
CockroachDB, TiDB, Aurora
  * Tables with fixed schemas, rows, foreign keys; SQL with JOINs.
  * ACID transactions across rows/tables (see next lesson).
  * Flexible querying: you can ask questions you didn't plan for.
  * Scaling writes beyond one node needs sharding (hard) -- or NewSQL.
  Use when: data is relational (users, orders, payments), you need
  transactions and integrity constraints, or ad-hoc queries. THE DEFAULT.

KEY-VALUE: Redis, DynamoDB, Riak, etcd
  * get(key) / put(key, value). Extremely fast and easy to partition.
  * No queries by value (without secondary indexes).
  Use when: caching, sessions, shopping carts, feature flags, lookups by ID.

DOCUMENT: MongoDB, Couchbase, Firestore (DynamoDB also fits)
  * JSON-like documents; flexible schema; nested data stored together.
  * Great "locality": one read fetches an entire aggregate (a user profile
    with its addresses and preferences).
  * Joins are weak or absent; many-to-many relationships are awkward.
  Use when: self-contained aggregates, evolving schemas, content/catalogues.

WIDE-COLUMN: Cassandra, HBase, ScyllaDB, Bigtable
  * Rows identified by a PARTITION KEY; within a partition, rows sorted by a
    CLUSTERING KEY. You design tables around QUERIES (one table per query).
  * Massive write throughput (LSM-trees, next lessons), linear horizontal
    scaling, multi-datacenter replication, tunable consistency.
  Use when: time-series, messaging, activity feeds, IoT, event logs --
  huge write volume with known access patterns.
  e.g. messages table: PRIMARY KEY ((chat_id), message_time)
       -> "get the latest 50 messages in chat X" is one sequential read.

GRAPH: Neo4j, Amazon Neptune
  * Nodes and edges; traversals ("friends of friends who like X") are cheap.
  Use when: social graphs, recommendations, fraud rings, knowledge graphs.

SEARCH ENGINES: Elasticsearch, OpenSearch, Solr
  * Inverted index: word -> list of documents. Full-text search, relevance
    ranking, fuzzy matching, faceting. Usually a SECONDARY store fed from the
    primary DB (via CDC or a queue), not the source of truth.

TIME-SERIES: InfluxDB, TimescaleDB, Prometheus
  * Optimised for append-only timestamped metrics; downsampling; retention.

OBJECT / BLOB STORAGE: Amazon S3, GCS, Azure Blob
  * Files (images, videos, backups, logs) of any size; cheap; 11 nines
    durability; accessed by key over HTTP. Store the FILE in S3 and its
    METADATA (owner, size, URL) in a database. Never put large blobs in a DB.

NORMALIZATION vs DENORMALIZATION
--------------------------------
* NORMALIZED: each fact stored once; related data referenced by ID and
  JOINed at read time. No update anomalies; writes are simple; reads may need
  expensive joins (especially across shards, where joins become impossible).
* DENORMALIZED: duplicate data so reads need no joins (store author_name on
  every post). Fast reads; but writes must update every copy, and copies can
  drift. At scale, read-heavy systems denormalize deliberately (or maintain
  precomputed views: materialized views, caches, search indexes, feeds).

INTERVIEW RECIPE
----------------
"Users, orders and payments are relational and need transactions -> Postgres.
 Chat messages are huge write volume, accessed by (chat_id, time) -> Cassandra.
 Media files -> S3 + CDN. Search -> Elasticsearch fed by CDC.
 Hot lookups/sessions -> Redis."
This is POLYGLOT PERSISTENCE: the right store for each access pattern.

The demo uses sqlite3 (a real SQL engine in the Python stdlib) for the
relational model and plain dicts for the document model.
"""

import json
import sqlite3
import time


def relational_demo():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE users  (id INTEGER PRIMARY KEY, name TEXT NOT NULL, city TEXT);
        CREATE TABLE posts  (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL REFERENCES users(id),
                             body TEXT, created_at INTEGER);
        CREATE TABLE follows(follower_id INTEGER, followee_id INTEGER,
                             PRIMARY KEY (follower_id, followee_id));
    """)
    db.executemany("INSERT INTO users VALUES (?,?,?)",
                   [(1, "Ada", "London"), (2, "Linus", "Portland"), (3, "Grace", "NYC")])
    db.executemany("INSERT INTO posts VALUES (?,?,?,?)",
                   [(1, 1, "Engines can do more than numbers", 100),
                    (2, 2, "Talk is cheap. Show me the code.", 200),
                    (3, 3, "It's easier to ask forgiveness", 300),
                    (4, 1, "Notes on the analytical engine", 400)])
    db.executemany("INSERT INTO follows VALUES (?,?)", [(3, 1), (3, 2), (2, 1)])

    print("  Normalized: each user's name stored ONCE; posts reference author_id.")
    print("  Query: Grace's home feed = posts by people Grace follows (a JOIN):")
    rows = db.execute("""
        SELECT u.name, p.body FROM follows f
        JOIN posts p ON p.author_id = f.followee_id
        JOIN users u ON u.id = p.author_id
        WHERE f.follower_id = 3 ORDER BY p.created_at DESC
    """).fetchall()
    for name, body in rows:
        print(f"    {name:6s}: {body}")

    db.execute("UPDATE users SET name = 'Ada Lovelace' WHERE id = 1")
    print("  Rename Ada -> ONE row updated; every post reflects it automatically:")
    print("   ", db.execute("SELECT u.name, p.body FROM posts p JOIN users u ON u.id=p.author_id "
                           "WHERE p.id=4").fetchone())

    print("  Ad-hoc question nobody planned for (the power of SQL):")
    print("   ", db.execute("SELECT city, COUNT(*) FROM users u JOIN posts p ON p.author_id=u.id "
                           "GROUP BY city ORDER BY 2 DESC").fetchall())
    return db


def document_demo():
    # One self-contained document per user: everything for the profile page in ONE read.
    users = {
        "1": {"name": "Ada", "city": "London",
              "posts": [{"id": 1, "body": "Engines can do more than numbers"},
                        {"id": 4, "body": "Notes on the analytical engine"}],
              "settings": {"theme": "dark", "notifications": {"email": True}}},
    }
    print("  Document model: the whole profile page is one lookup, no joins:")
    print("   ", json.dumps(users["1"])[:95] + "...")

    # Denormalized feed: author name copied into every feed item.
    feeds = {"3": [{"author": "Ada", "body": "Notes on the analytical engine"},
                   {"author": "Linus", "body": "Talk is cheap. Show me the code."},
                   {"author": "Ada", "body": "Engines can do more than numbers"}]}
    print("  Denormalized feed for Grace (precomputed, author names copied in):")
    t = time.perf_counter()
    feed = feeds["3"]
    print(f"    read in {(time.perf_counter() - t) * 1e6:.1f} us, {len(feed)} items, no joins")
    # ...but renaming Ada means rewriting every copy in every follower's feed:
    copies = sum(1 for f in feeds.values() for item in f if item["author"] == "Ada")
    print(f"  Rename Ada -> must update {copies} copies in this ONE feed; "
          "with 10M followers, 10M+ writes (or accept stale names).")


if __name__ == "__main__":
    print("=" * 76)
    print("RELATIONAL (normalized, SQL, joins)")
    print("=" * 76)
    relational_demo()
    print()
    print("=" * 76)
    print("DOCUMENT / DENORMALIZED")
    print("=" * 76)
    document_demo()

    print("""
DECISION CHEAT SHEET
--------------------
  Need transactions / constraints / ad-hoc queries     -> SQL (Postgres/MySQL)
  Simple lookups by key, extreme speed                 -> Key-value (Redis/DynamoDB)
  Self-contained aggregates, flexible schema           -> Document (MongoDB)
  Massive writes, known queries, multi-DC              -> Wide-column (Cassandra)
  Relationship traversal                               -> Graph (Neo4j)
  Full-text search                                     -> Elasticsearch (secondary)
  Files / media                                        -> Object storage (S3) + CDN

INTERVIEW TALKING POINTS
------------------------
* Justify by ACCESS PATTERN, not hype: "messages are always read by
  (chat_id, recent time), so a wide-column store partitioned by chat_id with
  messages clustered by time makes that a single sequential read."
* SQL scales further than people think (read replicas, partitioning, big boxes).
* Denormalize for read-heavy paths, accept write amplification and staleness.
""")
