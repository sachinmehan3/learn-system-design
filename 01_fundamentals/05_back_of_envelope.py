"""
LESSON 01.05 -- BACK-OF-THE-ENVELOPE ESTIMATION
===============================================

WHY ESTIMATE?
-------------
Estimates tell you which architecture you need. 100 writes/sec fits on one
Postgres box. 1,000,000 writes/sec needs a sharded, write-optimised store.
Interviewers want to see you reason quantitatively, not get exact numbers.

RULES OF THUMB
--------------
* 1 day ~ 86,400 s ~ 10^5 s   (round aggressively -- say "about 100k seconds")
* 1 million requests/day ~ 12 req/s     (1M / 86,400)
* 1 billion requests/day ~ 12,000 req/s
* Peak QPS ~ 2-3x average (up to 10x for spiky events)

POWERS OF TWO / TEN
    2^10 ~ 10^3  = 1 thousand   = 1 KB
    2^20 ~ 10^6  = 1 million    = 1 MB
    2^30 ~ 10^9  = 1 billion    = 1 GB
    2^40 ~ 10^12 = 1 trillion   = 1 TB
    2^50 ~ 10^15                = 1 PB

TYPICAL SIZES
    char (ASCII) 1 B, UUID 16 B, int64 / timestamp 8 B,
    a tweet-sized text row ~ 300 B - 1 KB with metadata,
    a compressed photo ~ 200 KB - 2 MB, 1 minute of HD video ~ 50-100 MB.

WHAT ONE MACHINE CAN DO (very rough, modern hardware)
    * Web/app server: 1k - 10k+ simple req/s depending on work per request
    * Relational DB (single node): ~1k-10k writes/s, 10k-50k simple reads/s
    * Redis/Memcached: ~100k+ ops/s per node
    * Kafka broker: hundreds of MB/s
    * RAM per server: 64-512 GB is ordinary; SSD: several TB

THE STANDARD ESTIMATION FLOW
----------------------------
    1. Users:        DAU (daily active users)
    2. Traffic:      actions per user per day -> write QPS, read QPS, peak QPS
    3. Storage:      bytes per item x items per day x retention (x replication)
    4. Bandwidth:    QPS x bytes per request
    5. Cache memory: ~20% of daily read data (80/20 rule: 20% of items get 80% of reads)
    6. Servers:      peak QPS / QPS per server (+ headroom)

The code below is a reusable estimator. The worked example is a
Twitter-like service.
"""

from dataclasses import dataclass

SECONDS_PER_DAY = 86_400


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB", "EB"):
        if abs(n) < 1000:
            return f"{n:,.1f} {unit}"
        n /= 1000
    return f"{n:,.1f} ZB"


def human_count(n: float) -> str:
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if n >= div:
            return f"{n / div:,.1f}{unit}"
    return f"{n:,.0f}"


@dataclass
class Workload:
    name: str
    dau: int                        # daily active users
    writes_per_user_per_day: float  # e.g. posts created
    reads_per_user_per_day: float   # e.g. items viewed
    bytes_per_write: int            # size of one stored item
    bytes_per_read: int             # response size
    retention_years: float
    replication_factor: int = 3
    peak_factor: float = 3.0
    qps_per_server: int = 2_000

    def report(self):
        write_qps = self.dau * self.writes_per_user_per_day / SECONDS_PER_DAY
        read_qps = self.dau * self.reads_per_user_per_day / SECONDS_PER_DAY
        peak_read_qps = read_qps * self.peak_factor
        peak_write_qps = write_qps * self.peak_factor

        daily_new_data = self.dau * self.writes_per_user_per_day * self.bytes_per_write
        total_storage = daily_new_data * 365 * self.retention_years * self.replication_factor

        egress_per_sec = read_qps * self.bytes_per_read
        ingress_per_sec = write_qps * self.bytes_per_write

        # 80/20 rule: cache the 20% hottest part of one day's read traffic.
        daily_read_bytes = self.dau * self.reads_per_user_per_day * self.bytes_per_read
        cache_memory = 0.2 * daily_read_bytes

        servers = (peak_read_qps + peak_write_qps) / self.qps_per_server

        print(f"\n### {self.name}")
        print(f"  Read:write ratio       : {self.reads_per_user_per_day / self.writes_per_user_per_day:,.0f}:1")
        print(f"  Write QPS  (avg/peak)  : {write_qps:,.0f} / {peak_write_qps:,.0f}")
        print(f"  Read  QPS  (avg/peak)  : {read_qps:,.0f} / {peak_read_qps:,.0f}")
        print(f"  New data per day       : {human_bytes(daily_new_data)}")
        print(f"  Storage ({self.retention_years}y, x{self.replication_factor} repl.) : {human_bytes(total_storage)}")
        print(f"  Ingress / egress       : {human_bytes(ingress_per_sec)}/s / {human_bytes(egress_per_sec)}/s")
        print(f"  Cache (20% of daily reads): {human_bytes(cache_memory)}")
        print(f"  App servers at peak    : ~{servers:,.0f} (at {self.qps_per_server:,} QPS each, before headroom)")


if __name__ == "__main__":
    print("=" * 70)
    print("WORKED EXAMPLES")
    print("=" * 70)

    # Twitter-like: 200M DAU, each posts 2 tweets and reads 100 tweets a day.
    Workload(
        name="Twitter-like (text only)",
        dau=200_000_000,
        writes_per_user_per_day=2,
        reads_per_user_per_day=100,
        bytes_per_write=1_000,      # 280 chars + ids, timestamps, metadata
        bytes_per_read=1_000,
        retention_years=5,
    ).report()

    # URL shortener: 100M new URLs/month, 100:1 read:write.
    # 100M/month ~ 3.3M/day. Model it as 3.3M "users" doing one write each.
    Workload(
        name="URL shortener",
        dau=3_300_000,
        writes_per_user_per_day=1,
        reads_per_user_per_day=100,
        bytes_per_write=500,
        bytes_per_read=500,
        retention_years=10,
    ).report()

    # Photo sharing: 10M DAU, 0.1 uploads/day each, view 50 photos/day.
    Workload(
        name="Instagram-like (photos)",
        dau=10_000_000,
        writes_per_user_per_day=0.1,
        reads_per_user_per_day=50,
        bytes_per_write=2_000_000,  # 2 MB photo
        bytes_per_read=200_000,     # served as compressed thumbnails/feeds
        retention_years=10,
    ).report()

    print("""
HOW TO READ THESE NUMBERS
-------------------------
* Twitter-like: ~230k read QPS avg -> definitely caching + many app servers;
  ~4.6k write QPS is manageable but storage grows ~1 PB over 5 years
  (with replication) -> sharded storage.
* URL shortener: ~40 writes/s -- a single DB could handle writes! Reads at
  ~4k/s avg are easily served from cache. Don't over-engineer.
* Photos: egress bandwidth (~1 GB/s+) is the dominant cost -> CDN + object
  storage (S3), not a database.

ID-SPACE MATH (URL shortener): how long must the short code be?
""")
    for length in (6, 7, 8):
        combos = 62 ** length
        print(f"  base62, {length} chars = {combos:,.0f} codes "
              f"({human_count(combos)}) -> lasts {combos / (100e6 * 12):,.0f} years at 100M/month")

    print("""
INTERVIEW TALKING POINTS
------------------------
* State assumptions out loud, round aggressively, and sanity-check the result.
* Draw conclusions: "~40 writes/s means a single primary DB is fine; the real
  challenge is the 4k reads/s, so let's put a cache in front."
* Identify the dominant resource: QPS? storage? bandwidth? memory? That's what
  your design must optimise.
""")
