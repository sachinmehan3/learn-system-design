# Learn System Design — From Basics to Interview-Ready

A hands-on, **heavily commented** course in system design. Every lesson is a
single, self-contained Python file (standard library only — no `pip install`)
that:

1. **Explains** the concept in long-form comments (what, why, trade-offs,
   what interviewers look for).
2. **Implements** a small but real version of it, so you can see how it works.
3. **Demonstrates** it when you run the file.

```bash
python 03_load_balancing/02_consistent_hashing.py   # run one lesson
python run_all.py                                   # run every lesson's demo
python run_all.py caching                           # run lessons whose path matches "caching"
```

> Read the file top-to-bottom first, *then* run it. The comments are the
> lesson; the code is the proof.

---

## The Roadmap

Work through the modules in order. Each builds on the vocabulary of the last.

| # | Module | What you'll learn |
|---|--------|-------------------|
| 00 | [`00_interview_framework.md`](00_interview_framework.md) | **Start here.** The 4-step method for answering any design question. |
| 01 | `01_fundamentals/` | Scalability, latency vs throughput, availability math ("nines"), CAP & PACELC, back-of-the-envelope estimation, consistency models |
| 02 | `02_networking/` | OSI/TCP/UDP, DNS, HTTP/1.1→2→3, REST vs gRPC vs GraphQL, polling vs long-polling vs WebSockets vs SSE, proxies |
| 03 | `03_load_balancing/` | L4 vs L7, round-robin / weighted / least-connections / IP-hash, health checks, **consistent hashing** |
| 04 | `04_caching/` | LRU & LFU, cache-aside / write-through / write-back / write-around, TTLs, stampede & penetration & avalanche, CDNs |
| 05 | `05_databases/` | SQL vs NoSQL, ACID, isolation levels & anomalies, **indexes (B-tree vs LSM-tree)**, replication, **sharding**, denormalization |
| 06 | `06_distributed_systems/` | Quorums, vector clocks, **Raft consensus**, 2PC vs Sagas, gossip, Bloom filters, Merkle trees, distributed locks, unique ID generation |
| 07 | `07_messaging/` | Queues vs pub/sub vs logs (Kafka), delivery semantics, idempotency, backpressure, dead-letter queues |
| 08 | `08_reliability/` | **Rate limiting** (5 algorithms), circuit breakers, retries + exponential backoff + jitter, bulkheads, timeouts, graceful degradation |
| 09 | `09_architecture/` | Monolith vs microservices, API gateway, service discovery, event sourcing, CQRS, the outbox pattern |
| 10 | `10_observability_security/` | Metrics / logs / traces, SLIs/SLOs, percentiles; AuthN vs AuthZ, JWT, OAuth2, hashing passwords |
| 11 | `11_case_studies/` | End-to-end designs: URL shortener, news feed, chat, typeahead, web crawler, distributed KV store, notification system, ride sharing / proximity |

## Cheat sheets

* [`cheatsheets/numbers_to_know.md`](cheatsheets/numbers_to_know.md) — latency numbers, powers of two, QPS rules of thumb.
* [`cheatsheets/tradeoffs.md`](cheatsheets/tradeoffs.md) — "X vs Y" decisions you'll be asked to justify.
* [`cheatsheets/glossary.md`](cheatsheets/glossary.md) — every term used in this project, one line each.

## How to study with this

1. **Read** a lesson file fully. Every lesson ends with an *"Interview
   talking points"* section — say those out loud.
2. **Run** it and match the output to the explanation.
3. **Break it.** Change a parameter (cache size, replica count, failure rate)
   and predict the output before running again. This is where learning sticks.
4. After module 10, attempt each case study **on paper first** using the
   framework in `00_interview_framework.md`, *then* read the solution file.

## Other helpful repos

This project teaches by running code. These repos complement it with
diagrams, reading lists, and more case studies.

### Interview prep and core concepts

| Repo | Why it's useful |
|---|---|
| [donnemartin/system-design-primer](https://github.com/donnemartin/system-design-primer) | The most popular system design guide, with worked interview questions and Anki flashcards. |
| [ByteByteGoHq/system-design-101](https://github.com/ByteByteGoHq/system-design-101) | Complex systems explained with clear visuals. Great for quick review. |
| [karanpratapsingh/system-design](https://github.com/karanpratapsingh/system-design) | A full course in one long read, from basics through case studies. |
| [ashishps1/awesome-system-design-resources](https://github.com/ashishps1/awesome-system-design-resources) | Free articles and videos organised by concept, plus practice problems. |
| [checkcheckzz/system-design-interview](https://github.com/checkcheckzz/system-design-interview) | Links to how real companies designed their systems, plus interview questions. |
| [InterviewReady/system-design-resources](https://github.com/InterviewReady/system-design-resources) | Curated engineering blog posts and papers behind real systems. |

### Going deeper into scalability and distributed systems

| Repo | Why it's useful |
|---|---|
| [binhnguyennus/awesome-scalability](https://github.com/binhnguyennus/awesome-scalability) | Scalability, reliability and performance patterns, drawn from real companies' engineering blogs. |
| [madd86/awesome-system-design](https://github.com/madd86/awesome-system-design) | Curated distributed systems resources: papers, talks and books. |
| [theanalyst/awesome-distributed-systems](https://github.com/theanalyst/awesome-distributed-systems) | Reading list of foundational distributed systems papers and courses. |
| [aphyr/distsys-class](https://github.com/aphyr/distsys-class) | Lecture notes by the author of Jepsen; excellent on failure modes and consistency. |
| [pingcap/talent-plan](https://github.com/pingcap/talent-plan) | Hands-on courses where you build a distributed database (Raft, transactions). |

### Beyond backend (not covered in this project)

| Repo | Why it's useful |
|---|---|
| [greatfrontend/awesome-front-end-system-design](https://github.com/greatfrontend/awesome-front-end-system-design) | Frontend system design resources for interviews. |
| [open-guides/og-aws](https://github.com/open-guides/og-aws) | A practical guide to Amazon Web Services, for hands-on cloud knowledge. |
