# Trade-offs You Must Be Able to Justify

Interviewers don't want the "right" answer; they want **"I chose X over Y
because of requirement Z, and I accept downside W."**

| Decision | Choose the first when... | Choose the second when... | Lesson |
|---|---|---|---|
| **Vertical vs horizontal scaling** | early stage, simplicity, stateful DB | beyond one machine, need redundancy | 01.01 |
| **Consistency vs availability** (CP vs AP) | money, inventory, locks, uniqueness | feeds, likes, carts, catalogues | 01.04 |
| **Latency vs consistency** (PACELC "E") | strong reads required | stale-by-ms reads acceptable | 01.04 |
| **Strong vs eventual consistency** | correctness > latency | scale & availability > freshness | 01.06 |
| **TCP vs UDP** | correctness (APIs, DBs) | real-time media, games, DNS | 02.01 |
| **REST vs gRPC vs GraphQL** | REST: public APIs, HTTP caching | gRPC: internal service calls, streaming; GraphQL: flexible client-driven fetching | 02.03 |
| **Polling vs WebSockets vs SSE** | rare updates, simplicity | bidirectional, low latency | 02.04 |
| **L4 vs L7 load balancer** | raw throughput, non-HTTP | path/header routing, TLS, retries | 03.01 |
| **Round robin vs least-connections** | uniform requests | variable request cost | 03.01 |
| **hash mod N vs consistent hashing** | fixed cluster size | nodes join/leave | 03.02 |
| **Cache-aside vs write-through vs write-back** | cache-aside: general default | write-through: read-after-write freshness; write-back: write-heavy, loss-tolerant | 04.02 |
| **LRU vs LFU** | recency predicts reuse | stable long-term popularity, scan resistance | 04.01 |
| **SQL vs NoSQL** | relations, transactions, ad-hoc queries | massive scale, known access patterns, flexible schema | 05.01 |
| **Normalize vs denormalize** | write-heavy, integrity | read-heavy, avoid joins | 05.01 |
| **B-tree vs LSM-tree** | read-heavy, transactional | write-heavy (logs, messages, metrics) | 05.03 |
| **Sync vs async replication** | zero data loss | low write latency | 05.04 |
| **Single-leader vs multi-leader vs leaderless** | single-leader: simplicity, no conflicts | multi-leader: multi-region writes; leaderless: max availability, tunable quorums | 05.04 |
| **Range vs hash sharding** | range scans | even distribution | 05.05 |
| **2PC vs Saga** | atomic isolation, one DB system | across microservices | 06.04 |
| **LWW vs vector clocks vs CRDTs** | LWW: simplicity (accepts lost writes) | vector clocks: detect conflicts; CRDTs: auto-merge | 06.02 |
| **Redis lock vs ZooKeeper/etcd lock** | efficiency (duplicate work OK) | correctness (+ fencing tokens) | 06.08 |
| **UUID vs Snowflake** | no coordination, no ordering needed | 64-bit, time-sortable | 06.09 |
| **Queue vs pub/sub vs log** | queue: one consumer per job | pub/sub: fan-out to many; log (Kafka): replay, ordering, high volume | 07.01 |
| **At-most-once vs at-least-once** | loss OK (metrics) | no loss (+ idempotency) | 07.02 |
| **Token bucket vs sliding window** | allow bursts | strict limits | 08.01 |
| **Monolith vs microservices** | small team, early product | many teams, independent scaling | 09.01 |
| **Fan-out on write vs on read** | read-heavy, normal users | celebrities, inactive users | 11.02 |
| **Session vs JWT** | instant revocation | stateless verification at scale | 10.02 |

## How to present a trade-off (script)

1. "The two options here are **A** and **B**."
2. "A gives us ___ but costs ___. B gives us ___ but costs ___."
3. "Given our requirement of ___, I'll pick **A**."
4. "The risk is ___, which we mitigate with ___."
