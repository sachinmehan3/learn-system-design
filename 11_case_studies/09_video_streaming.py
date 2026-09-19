"""
CASE STUDY 09 -- DESIGN A VIDEO PLATFORM (YouTube / Netflix)
============================================================

STEP 1 -- REQUIREMENTS
----------------------
Functional: upload videos; watch videos smoothly on any device and network;
(search, comments, recommendations -- usually out of scope).
Non-functional: playback must start fast and not buffer; massive read
bandwidth; uploads can be processed asynchronously (minutes is fine);
highly available; cost-efficient storage and bandwidth.

STEP 2 -- ESTIMATION
--------------------
  Uploads: 500 hours of video per minute (YouTube scale) -> after
  transcoding into ~6 renditions, petabytes per day of new storage.
  Views: billions per day; egress bandwidth is THE dominant cost -> CDN.

STEP 3 -- HIGH-LEVEL DESIGN
---------------------------
UPLOAD PATH (write, asynchronous):
  client --(pre-signed URL, resumable multipart upload)--> object storage "raw"
     --> event "VideoUploaded" --> queue --> TRANSCODING PIPELINE (a DAG):
            split into segments (chunks of a few seconds) -> transcode chunks
            IN PARALLEL on many workers into multiple resolutions/bitrates
            (240p .. 4K) and codecs (H.264, VP9, AV1) -> package as HLS/DASH
            (segments + manifest) -> thumbnails, captions, content moderation
     --> "transcoded" bucket --> push popular content to CDN --> metadata DB
         status = READY --> notify uploader

WATCH PATH (read, synchronous):
  client --> API: GET /videos/{id} -> metadata + manifest URL (CDN)
  player downloads the MANIFEST (list of renditions and segment URLs) and
  then fetches segments from the nearest CDN edge, choosing the bitrate per
  segment: ADAPTIVE BITRATE STREAMING (ABR).

STEP 4 -- DEEP DIVES
--------------------
* WHY SEGMENTS: small files (2-10 s) are cacheable by any HTTP CDN, allow
  switching quality mid-stream, allow parallel transcoding, and let playback
  start after the first segment instead of the whole file.
* ADAPTIVE BITRATE: the player measures download throughput and buffer level;
  if throughput drops, it picks a lower bitrate for the next segment instead
  of stalling. Better blurry than buffering.
* PARALLEL TRANSCODING: a 2-hour movie split into 1,000 chunks transcoded on
  1,000 workers finishes in minutes. Workers pull tasks from a queue; failed
  chunks are retried (idempotent: output path is deterministic).
* CDN STRATEGY: popular videos pushed to edges proactively; long tail pulled
  on demand from origin/shield; Netflix Open Connect places caches INSIDE ISPs
  and pre-fills them overnight with predicted popular titles.
* STORAGE TIERS: hot (recent/popular) vs cold (rarely watched) storage classes;
  most views concentrate on a small fraction of videos.
* METADATA: video info in a sharded SQL/NoSQL store + cache; view counts via
  async counters (Kafka -> aggregation), not a DB write per view.
* RESUMABLE UPLOADS: large files uploaded in parts; retry only failed parts.

The simulation: (1) parallel chunked transcoding vs serial; (2) a player on
a fluctuating network with fixed-bitrate vs adaptive-bitrate playback.
"""

import concurrent.futures as cf
import random
import time

RENDITIONS_KBPS = [400, 1000, 2500, 5000, 8000]      # 240p .. 4K
SEGMENT_SECONDS = 4


def transcode_chunk(chunk_id):
    time.sleep(0.02)                                 # pretend this is CPU-heavy work
    return {r: f"s3://videos/v42/{r}k/seg{chunk_id:04d}.ts" for r in RENDITIONS_KBPS}


def transcode(n_chunks, workers):
    start = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        outputs = list(pool.map(transcode_chunk, range(n_chunks)))
    manifest = {r: [o[r] for o in outputs] for r in RENDITIONS_KBPS}
    return time.perf_counter() - start, manifest


def play(policy, bandwidth_trace):
    """
    Simulate a player: each 4-second segment must download before the buffer runs out.
    Returns (rebuffering seconds, average bitrate).
    """
    buffer_s, rebuffer_s, bitrates = 0.0, 0.0, []
    for kbps_available in bandwidth_trace:
        if policy == "fixed 5000k":
            choice = 5000
        else:
            # Simple ABR: pick the highest rendition below 80% of measured throughput;
            # be more conservative when the buffer is low.
            safety = 0.8 if buffer_s > 8 else 0.5
            choice = max([r for r in RENDITIONS_KBPS if r <= kbps_available * safety] or [RENDITIONS_KBPS[0]])
        download_time = SEGMENT_SECONDS * choice / kbps_available
        if download_time > buffer_s:
            rebuffer_s += download_time - buffer_s   # video frozen, spinner shown
            buffer_s = 0
        else:
            buffer_s -= download_time
        buffer_s += SEGMENT_SECONDS
        bitrates.append(choice)
    return rebuffer_s, sum(bitrates) / len(bitrates)


if __name__ == "__main__":
    print("=" * 78)
    print("UPLOAD: transcoding a video split into 200 chunks x 5 renditions")
    print("=" * 78)
    for workers in (1, 10, 50):
        elapsed, manifest = transcode(200, workers)
        print(f"  {workers:3d} worker(s): {elapsed:5.2f}s")
    print(f"  manifest: {len(manifest)} renditions x {len(manifest[400])} segments, e.g. {manifest[2500][0]}")

    print()
    print("=" * 78)
    print("WATCH: 10 minutes of playback on a phone moving between Wi-Fi and a weak 4G signal")
    print("=" * 78)
    rng = random.Random(7)
    trace = []
    for i in range(150):                             # 150 segments x 4 s = 10 min
        good = (i // 30) % 2 == 0                    # alternates every 2 minutes
        trace.append(max(300, rng.gauss(9000 if good else 1800, 700)))
    for policy in ("fixed 5000k", "adaptive bitrate"):
        rebuffer, avg = play(policy, trace)
        print(f"  {policy:17s}: rebuffering {rebuffer:6.1f}s, average quality {avg:6.0f} kbps")

    print("""
WHAT YOU SAW
------------
* Chunked, parallel transcoding scales almost linearly with workers.
* A fixed high bitrate freezes for minutes on the weak network; ABR drops
  quality briefly and keeps playing -- then climbs back up on Wi-Fi.

FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How do you make uploads reliable for 10 GB files? (resumable multipart
  uploads directly to object storage via pre-signed URLs)
* How do you reduce CDN cost? (better codecs like AV1, cache-fill off-peak,
  ISP-embedded caches, tiered storage for the long tail)
* How do you count views at scale? (async events, aggregated counters,
  approximate is fine)
* Live streaming? (same segment idea with a few seconds of latency: RTMP/SRT
  ingest -> real-time transcoding -> LL-HLS; or WebRTC for sub-second)
""")
