# Numbers Every System Designer Should Know

All values are rough orders of magnitude. The point is to reason about
*which* resource dominates, not to be precise.

## Latency ladder

| Operation | Time | Relative |
|---|---|---|
| L1 cache reference | 1 ns | |
| Main memory reference | 100 ns | |
| Compress 1 KB (fast codec) | 2 µs | |
| Read 1 MB sequentially from RAM | 3 µs | |
| Redis GET (same datacenter, incl. network) | ~0.2–0.5 ms | |
| Round trip within a datacenter | 0.5 ms | |
| Random SSD read | ~100 µs | |
| Read 1 MB sequentially from SSD | ~1 ms | |
| Simple indexed DB query | 1–10 ms | |
| HDD seek | 10 ms | |
| Read 1 MB sequentially from HDD | 20 ms | |
| Round trip US ↔ Europe | ~80–150 ms | |
| TCP + TLS handshake across an ocean | 300–600 ms | |

**Takeaways:** memory ≫ network ≫ disk; sequential ≫ random; distance costs
~1 ms per 100 km of fiber round trip.

## Time conversions

* 1 day ≈ 86,400 s ≈ **10⁵ s**
* 1 month ≈ 2.6 M s; 1 year ≈ 31.5 M s
* **1 M requests/day ≈ 12 req/s**; 1 B/day ≈ 12k req/s
* Peak ≈ 2–3× average (10× for flash events)

## Powers of two

| Power | Exact | Approx | Name |
|---|---|---|---|
| 2¹⁰ | 1,024 | 10³ | KB |
| 2²⁰ | 1,048,576 | 10⁶ | MB |
| 2³⁰ | | 10⁹ | GB |
| 2⁴⁰ | | 10¹² | TB |
| 2⁵⁰ | | 10¹⁵ | PB |

## Typical sizes

* ASCII char 1 B · int32 4 B · int64/timestamp 8 B · UUID 16 B
* Tweet-sized row with metadata: ~0.5–1 KB
* Web page: ~100 KB–2 MB · Photo: 200 KB–5 MB · 1 min of HD video: 50–100 MB

## Single-machine capacity (very rough, modern hardware)

| Component | Throughput |
|---|---|
| Stateless web/app server | 1k–10k+ req/s |
| PostgreSQL/MySQL (single primary) | ~5k–20k writes/s, 10k–100k simple reads/s |
| Redis / Memcached node | 100k–1M ops/s |
| Kafka broker | 100s of MB/s; ~1M msgs/s per cluster is routine |
| WebSocket gateway | 100k–1M concurrent connections |
| Server RAM | 64 GB–1 TB+ |
| NIC | 10–100 Gbit/s |

## Availability

| SLA | Downtime / year | Downtime / month |
|---|---|---|
| 99% | 3.65 days | 7.3 h |
| 99.9% | 8.8 h | 43.8 min |
| 99.99% | 52.6 min | 4.4 min |
| 99.999% | 5.3 min | 26 s |

Series: multiply availabilities. Parallel: `1 − ∏(1 − Aᵢ)`.

## Handy formulas

* **Little's law:** concurrency = throughput × latency
* **Queueing (M/M/1):** latency ≈ service_time / (1 − utilization)
* **Quorum:** R + W > N for overlapping reads/writes
* **Consensus:** 2f + 1 nodes tolerate f failures
* **Bloom filter:** ~10 bits per item → ~1% false positives
* **Base62 codes:** 62⁶ ≈ 57 B, 62⁷ ≈ 3.5 T
* **Snowflake ID:** 41-bit ms timestamp | 10-bit machine | 12-bit sequence
