# The System Design Interview Framework

A system design interview is **not** a test of whether you know "the right
answer". There is no right answer. It's a test of whether you can:

* take an ambiguous problem and make it concrete,
* make reasonable decisions **and say why**,
* notice trade-offs and bottlenecks before the interviewer points them out,
* communicate like a senior engineer in a design review.

Use this 4-step structure for every question (roughly 45 minutes):

```
 ┌──────────────────────┬─────────┐
 │ 1. Requirements      │ ~5 min  │  What are we building? For how many people?
 │ 2. Estimation + API  │ ~5 min  │  How big? What's the interface?
 │ 3. High-level design │ ~15 min │  Boxes and arrows, data model, end-to-end flow
 │ 4. Deep dives        │ ~15 min │  Bottlenecks, scaling, failure modes, trade-offs
 │ (wrap-up)            │ ~5 min  │  Summarise, mention what you'd do next
 └──────────────────────┴─────────┘
```

---

## Step 1 — Clarify requirements (never skip this)

Jumping straight into drawing boxes is the #1 way candidates fail. Ask
questions, then **write down** the answers.

### Functional requirements — *what* the system does
"Users can shorten a URL." "Users can follow others and see a feed."
Pick **3–5 core features** and explicitly say what's *out of scope*
("I'll leave out analytics and custom domains unless you'd like them").

### Non-functional requirements — *how well* it does it
These drive almost every architectural decision:

| Property | Question to ask |
|---|---|
| Scale | How many daily active users (DAU)? Requests per second? Data size? |
| Read/write ratio | Is this read-heavy (feeds, URL redirects) or write-heavy (logging, metrics)? |
| Latency | What's acceptable? (e.g. p99 < 200 ms for a redirect) |
| Availability | Is downtime catastrophic (payments) or tolerable (analytics)? |
| Consistency | Must every read see the latest write? Or is "eventually" OK? |
| Durability | Can we ever lose data? |

> A key sentence to say: **"Given this is read-heavy and can tolerate
> eventual consistency, I'll prioritise availability and low latency."**
> That one sentence shows you know CAP/PACELC and that requirements drive design.

## Step 2 — Back-of-the-envelope estimation + API

See `01_fundamentals/05_back_of_envelope.py`. Estimate:

* **QPS** (average and peak ≈ 2–10× average)
* **Storage** over 5–10 years
* **Bandwidth** (ingress/egress)
* **Memory** for caching (the 80/20 rule: cache the hottest 20%)

The point isn't precision — it's knowing whether you need 1 server or 1,000,
1 database or a sharded cluster.

Then define the **API** (the contract between client and system):

```
POST /v1/urls          { long_url, custom_alias? }  -> { short_url }
GET  /{short_code}     -> 301/302 redirect
```

And a rough **data model** (tables/collections, key fields, access pattern).

## Step 3 — High-level design

Draw the end-to-end path of a request. Nearly every design starts as:

```
Client → DNS → CDN → Load Balancer → API servers (stateless) → Cache → Database
                                           │
                                           └→ Message Queue → Workers
```

Walk through **one write** and **one read** end to end. Keep it simple first;
you'll add complexity in step 4 only where the numbers demand it.

## Step 4 — Deep dives

The interviewer usually steers here. Common deep-dive topics, and where they
live in this project:

| Topic | Lesson |
|---|---|
| "The DB is the bottleneck" | `05_databases/` — replication, sharding, indexes |
| "Reads are too slow" | `04_caching/` |
| "One server gets all the traffic" | `03_load_balancing/`, hot-key handling in `04_caching/` |
| "What if a node dies?" | `06_distributed_systems/`, `08_reliability/` |
| "How do you avoid duplicates?" | `07_messaging/03_idempotency.py` |
| "How do you generate unique IDs?" | `06_distributed_systems/09_unique_id_generation.py` |
| "How do you prevent abuse?" | `08_reliability/01_rate_limiting.py` |

For each deep dive: **state the problem → propose 2 options → pick one with a
reason → mention its downside.** That structure *is* the signal interviewers
are grading.

## Wrap-up

Summarise the design in 30 seconds, list the known weak points, and say what
you'd add with more time (monitoring, multi-region, analytics pipeline...).

---

## Phrases that signal seniority

* "The trade-off here is…"
* "This becomes a bottleneck at roughly N QPS because…"
* "It depends on X; if X then A, otherwise B."
* "The failure mode I'm worried about is… and we mitigate it with…"
* "Let's start simple and scale only the part the numbers say we need to."

## Common mistakes

* Designing for Google scale when the requirement is 1,000 users.
* Naming technologies ("use Kafka!") without explaining the property you need
  ("we need a durable, replayable, ordered log").
* Ignoring failure: every box you draw can die. What happens then?
* Silence. Think out loud — the interviewer grades your reasoning, not your
  final diagram.
