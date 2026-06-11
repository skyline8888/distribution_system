# Replication in Distributed Systems

> **Replication** is the mechanism of copying and maintaining data across multiple nodes to achieve durability, availability, and read scalability. It is one of the most fundamental building blocks of distributed systems.

---

## Table of Contents

1. [Context & Motivation](#1-context--motivation)
2. [Architecture: Replication Patterns](#2-architecture-replication-patterns)
   - [2.1 Leader-Based Replication](#21-leader-based-replication)
   - [2.2 Multi-Leader Replication](#22-multi-leader-replication)
   - [2.3 Leaderless Replication](#23-leaderless-replication)
   - [2.4 Synchronous vs Asynchronous Replication](#24-synchronous-vs-asynchronous-replication)
3. [Core Technical Innovations](#3-core-technical-innovations)
   - [3.1 Conflict Resolution Strategies](#31-conflict-resolution-strategies)
   - [3.2 Replication Lag: Causes & Mitigation](#32-replication-lag-causes--mitigation)
   - [3.3 Read-After-Write Consistency](#33-read-after-write-consistency)
   - [3.4 Quorum Reads & Writes](#34-quorum-reads--writes)
4. [Trade-offs (CAP / PACELC)](#4-trade-offs-cap--pacelc)
5. [Influence & Legacy](#5-influence--legacy)
6. [Comparison Matrix](#6-comparison-matrix)
7. [References](#7-references)

---

## 1. Context & Motivation

### What Problem Does Replication Solve?

Replication addresses three fundamental challenges that no single node can solve alone:

1. **Durability** — If a single node crashes, data is lost. Replicating to N nodes means the system survives up to N-1 failures.
2. **Availability** — When one replica is unreachable, others can serve requests.
3. **Read Scalability** — A single node has a finite throughput. Multiple replicas can distribute read load horizontally.

### Predecessor Limitations

- **Single-node databases** (early MySQL, PostgreSQL) offered zero fault tolerance — disk failure meant data loss.
- **Shared-disk clustering** (early Oracle RAC) required expensive shared storage and complex lock management, limiting geographic distribution.
- **Master-slave with cold standby** had minutes to hours of recovery time (RTO), unacceptable for modern services.

### Hardware/Network Trends Enabling Replication

- **Commodity hardware** made it cheaper to add nodes than to buy bigger disks.
- **High-speed WAN links** (10Gbps+) made cross-datacenter replication practical.
- **SSDs & NVMe** reduced local I/O bottlenecks, shifting the bottleneck to network replication lag.

---

## 2. Architecture: Replication Patterns

### 2.1 Leader-Based Replication (Single-Writer)

**Core idea:** One node (the *leader*, also called *primary* or *master*) accepts all writes. Other nodes (*followers*, *secondaries*, *slaves*) receive a replication log and apply it locally. Reads can go to any follower.

```
                  ┌─────────────────────────────────────────────────┐
                  │              Leader-Based Replication            │
                  └─────────────────────────────────────────────────┘

                          ┌──────────┐
                          │  Client  │
                          │ (Write)  │
                          └────┬─────┘
                               │
                          ┌────▼─────┐
                          │          │ ◄── Single Writer
                          │  LEADER  │     (accepts all writes)
                          │          │
                          └────┬─────┘
                               │ Replication Log
                ┌──────────────┼──────────────┐
                │              │              │
           ┌────▼─────┐  ┌────▼─────┐  ┌────▼─────┐
           │          │  │          │  │          │
           │FOLLOWER-1│  │FOLLOWER-2│  │FOLLOWER-N│
           │  (Read)  │  │  (Read)  │  │  (Read)  │
           │          │  │          │  │          │
           └──────────┘  └──────────┘  └──────────┘
                ▲              ▲              ▲
           ┌────┴────┐    ┌───┴─────┐    ┌───┴─────┐
           │  Client │    │ Client  │    │ Client  │
           │ (Read)  │    │ (Read)  │    │ (Read)  │
           └─────────┘    └─────────┘    └─────────┘
```

#### How It Works

1. Client sends write to leader.
2. Leader appends write to its **replication log** (WAL / binlog / CDC stream).
3. Followers tail the log and replay operations locally.
4. Reads can be served by any replica.

#### Representative Systems

| System | Replication Mechanism | Key Details |
|--------|----------------------|-------------|
| MySQL | Binary Log (binlog) + semi-sync | Statement, row, or mixed format |
| PostgreSQL | WAL streaming (physical) / logical decoding | Hot standby, cascading replication |
| MongoDB | Oplog (capped collection) | Replica Set with elected primary |
| Redis | RDB snapshot + AOF sub-stream | Replica receives commands asynchronously |
| Kafka | ISR (In-Sync Replicas) | Log-based with leader election |

#### Failover Flow

```
  1. Leader fails ─► Followers detect (timeout / heartbeat loss)
  2. New election ─► Followers vote for a new leader
  3. Promotion    ─► Selected follower becomes the new leader
  4. Reconfig     ─ Clients re-route writes to new leader
```

MySQL uses **semi-synchronous replication** (MySQL 5.5+): the leader waits for at least one follower to acknowledge receipt before confirming to the client. This narrows the window of data loss during failover but adds latency.

#### Advantages

- **Simple consistency model** — one writer eliminates write-write conflicts.
- **Ordering** — the leader's log imposes a total order on writes.
- **Easy reasoning** — developers understand "single source of truth."

#### Limitations

- **Single point of bottleneck** — all writes funnel through one node.
- **Failover gap** — asynchronous replication can lose committed writes.
- **Geographic latency** — a distant leader adds RTT for every write.

---

### 2.2 Multi-Leader Replication (Multi-Writer)

**Core idea:** Multiple nodes accept writes concurrently. Each leader replicates its writes to the others. Common in multi-datacenter and collaboration scenarios.

```
                  ┌─────────────────────────────────────────────────┐
                  │            Multi-Leader Replication              │
                  └─────────────────────────────────────────────────┘

     ┌───────────────┐                  ┌───────────────┐
     │   DC-A (Region│                  │   DC-B (Region│
     │   1)          │                  │   2)          │
     │               │                  │               │
     │  ┌─────────┐  │                  │  ┌─────────┐  │
     │  │ Leader-A│◄─┼─── bidirectional ─►│ Leader-B │  │
     │  │         │  │    replication    │         │  │
     │  └────┬────┘  │                  │  └────┬────┘  │
     │       │       │                  │       │       │
     │  ┌────▼────┐  │                  │  ┌────▼────┐  │
     │  │Follower │  │                  │  │Follower │  │
     │  │  -A1    │  │                  │  │  -B1    │  │
     │  └─────────┘  │                  │  └─────────┘  │
     │               │                  │               │
     └───────┬───────┘                  └───────┬───────┘
             │                                  │
        ┌────▼────┐                        ┌────▼────┐
        │ Client-A│                        │ Client-B│
        │ (local  │                        │ (local  │
        │  write) │                        │  write) │
        └─────────┘                        └─────────┘
```

#### Representative Systems

| System | Approach | Key Details |
|--------|----------|-------------|
| CockroachDB | Raft per range, multi-active | Each range has its own Raft group; leases allow local reads |
| YugabyteDB | Raft + Hybrid Logical Clocks | PostgreSQL-compatible, multi-leader within same cluster |
| MySQL Group Replication | Paxos-based multi-primary | All members can accept writes; conflict detection via certification |
| PostgreSQL BDR | Logical decoding + async | Bi-directional async replication via logical slots |
| CouchDB | MVCC + revision trees | Append-only, revision-based conflict detection |

#### Why Use Multi-Leader?

- **Low-latency writes in multiple regions** — each region writes locally.
- **Offline operation** — devices or edge nodes can accept writes without connectivity, syncing later.
- **Collaborative applications** — Google Docs-style concurrent editing.

#### The Conflict Problem

When two leaders accept writes to the same key simultaneously, a **write-write conflict** occurs. The system must resolve it (see [Conflict Resolution](#31-conflict-resolution-strategies)).

```
  Time ─►

  Leader-A:  W(x=10) ───► W(x=20) ───► W(x=30)
               │            │            │
               └────► ◄─────┘            │
                  CONFLICT!              │
                  Both wrote x           │
                                         │
  Leader-B:  W(x=15) ───► W(x=25) ───► W(x=35)
```

#### Advantages

- **Write scalability** — multiple leaders share write load.
- **Fault tolerance** — if one leader fails, others continue.
- **Geographic distribution** — local writes in each region.

#### Limitations

- **Conflict complexity** — concurrent writes require resolution.
- **Consistency challenges** — no single total order across all writes.
- **Operational complexity** — harder to reason about, debug, and monitor.

---

### 2.3 Leaderless Replication

**Core idea:** Any replica can accept both reads and writes. There is no designated leader. The system relies on **quorum reads/writes** and **anti-entropy** to maintain consistency.

```
                  ┌─────────────────────────────────────────────────┐
                  │             Leaderless Replication               │
                  │         (Dynamo / Cassandra / Riak)              │
                  └─────────────────────────────────────────────────┘

              Client can write to ANY replica
                          │
              ┌───────────┼───────────┐
              │           │           │
         ┌────▼────┐ ┌────▼────┐ ┌────▼────┐
         │ Node A  │ │ Node B  │ │ Node C  │
         │ (R/W)   │ │ (R/W)   │ │ (R/W)   │
         └────┬────┘ └────┬────┘ └────┬────┘
              │           │           │
              └─────┬─────┘     ┌─────┘
                    │           │
               ┌────▼────┐ ┌────▼────┐
               │ Node D  │ │ Node E  │
               │ (R/W)   │ │ (R/W)   │
               └─────────┘ └─────────┘

        Each write goes to N replicas (W nodes).
        Each read queries N replicas (R nodes).
        Consistency: R + W > N  →  at least one
        overlap between read and write sets.
```

#### How It Works

1. **Write path:** Client sends a write. A coordinator node routes it to N replicas (configurable W of them must acknowledge).
2. **Read path:** Client reads from R replicas. If replicas disagree, the system resolves conflicts and returns the winning value.
3. **Repair:** Mechanisms like **read repair** and **anti-entropy** (Merkle trees) ensure eventual convergence.

#### Representative Systems

| System | Key Mechanism | Details |
|--------|--------------|---------|
| Amazon Dynamo | Vector Clocks + hinted handoff | Original design; evolved into DynamoDB |
| Cassandra | Tunable consistency + timestamps | Dynamo + Bigtable hybrid |
| Riak | Vector Clocks + CRDTs | Strong emphasis on conflict-free data types |
| Voldemort | Version vectors + push/pull | LinkedIn's key-value store |
| DynamoDB | Internal replication | Managed service; hides complexity |

#### Quorum Principle

```
  N = total replicas
  W = write quorum (nodes that must ack write)
  R = read quorum (nodes queried for read)

  Strong consistency requires:  R + W > N

  Example (N=3):
    W=1, R=3  →  fast writes, consistent reads  (AP-leaning)
    W=2, R=2  →  balanced quorum                  (middle ground)
    W=3, R=1  →  consistent writes, fast reads    (CP-leaning)
```

#### Read Repair & Anti-Entropy

```
  Read Repair (on read path):
  ┌────────┐   Read K   ┌────┐
  │Client  ├───────────►│ A  │ ──► returns (v=5, ts=100)
  └────────┘            └────┘
                        ┌────┐
                  ─────►│ B  │ ──► returns (v=3, ts=80)  ← STALE!
                        └────┘
                        ┌────┐
                  ─────►│ C  │ ──► returns (v=5, ts=100)
                        └────┘

  Coordinator detects B is stale, sends async write
  to update B: "set K=5, ts=100"

  Anti-Entropy (background):
  ┌───────────┐         ┌───────────┐
  │  Node A   │ ◄─────► │  Node B   │
  │ Merkle Tree│         │Merkle Tree│
  │  hash(0-9)│         │hash(0-9)  │
  │  hash(0-4)│ ── diff ─►hash(0-4) │  ← mismatch!
  │  hash(5-9)│ ── same ─►hash(5-9) │  ← skip, no diff
  └───────────┘         └───────────┘

  Only divergent ranges are synced.
  O(log N) comparison vs O(N) full scan.
```

#### Advantages

- **High write availability** — any node accepts writes; no single bottleneck.
- **Graceful degradation** — continues operating even with multiple node failures.
- **Tunable consistency** — W and R can be adjusted per operation.

#### Limitations

- **Eventual consistency** — reads may return stale data.
- **Conflict resolution burden** — application or system must handle concurrent writes.
- **Storage overhead** — multiple versions (via vector clocks) accumulate until compaction.

---

### 2.4 Synchronous vs Asynchronous Replication

This dimension is orthogonal to the leader/multi-leader/leaderless classification. Any pattern can be synchronous or asynchronous.

```
  ┌──────────────────────────────────────────────────────────────┐
  │         Synchronous vs Asynchronous Replication              │
  └──────────────────────────────────────────────────────────────┘

  SYNCHRONOUS:

  Client ──W──► Leader ───────────────────────► Client receives ACK
                       │                          only after ALL
                  ┌────┴────┐                   followers ack
                  │         │
             ┌────▼───┐ ┌──▼────┐
             │Follower│ │Follower│
             │  ack ✓ │ │ ack ✓ │
             └────────┘ └───────┘

  Latency: 1 RTT to leader + 1 RTT to slowest follower
  Guarantee: No data loss on leader failure
  Risk: If any follower is down, writes block

  ──────────────────────────────────────────────────────────────

  ASYNCHRONOUS:

  Client ──W──► Leader ──► Client receives ACK
                       │     immediately after
                       │     local commit
                  ┌────┴────┐
             ┌────▼───┐ ┌──▼────┐
             │Follower│ │Follower│  ← replication happens later
             │  ...   │ │  ...   │

  Latency: 1 RTT to leader only
  Guarantee: None (committed writes may be lost on leader failure)
  Risk: Replication lag causes stale reads and potential data loss
```

#### Semi-Synchronous (Middle Ground)

```
  Client ──W──► Leader ──► ACK after ≥1 follower acks

  ┌──────────────────────────────────────────────┐
  │ MySQL semi-sync:                               │
  │ - Leader waits for at least 1 replica ACK     │
  │ - If no replica responds within timeout,      │
  │   falls back to async                          │
  │ - Balances durability with availability        │
  └──────────────────────────────────────────────┘
```

#### Comparison

| Dimension | Synchronous | Asynchronous | Semi-Synchronous |
|-----------|------------|--------------|-------------------|
| **Write latency** | High (2× RTT) | Low (1× RTT) | Medium (1× RTT + ack) |
| **Data loss risk** | None | Possible on leader fail | Minimal (1 replica) |
| **Availability** | Blocked if follower down | Always available | Graceful fallback |
| **Use case** | Financial systems, RPO=0 | Web apps, analytics | Production databases |
| **CAP leaning** | CP | AP | Between |

---

## 3. Core Technical Innovations

### 3.1 Conflict Resolution Strategies

When concurrent writes hit the same data, systems must decide which value wins.

#### Last-Write-Wins (LWW)

The simplest approach: each write carries a timestamp; the highest timestamp wins.

```
  Write A:  x = 10,  ts = 1000
  Write B:  x = 20,  ts = 1005  ← WINS

  Problem: Clock skew can cause incorrect ordering.
  If Node B's clock is 5s ahead, its "later" write
  may actually have happened first.
```

**Used by:** Cassandra (default), DynamoDB, Redis (with active-active).

**Flaw:** Concurrent writes within the same clock tick lose one silently. No conflict is detected — a value is silently overwritten.

#### Vector Clocks

Each replica maintains a counter. Every write increments the local counter and carries the full vector. Causal relationships are preserved.

```
  Node A writes:  {A:1, B:0, C:0}  →  v1
  Node B writes:  {A:0, B:1, C:0}  →  v2

  Compare v1 and v2:
    v1 has A:1, v2 has A:0  →  v1 has info v2 lacks
    v2 has B:1, v1 has B:0  →  v2 has info v1 lacks
    Result: CONCURRENT (siblings) — must merge

  ┌─────────────────────────────────────────┐
  │ Sibling resolution (read-time):         │
  │  - Application logic merges both values  │
  │  - User picks one (shopping cart union)  │
  │  - CRDT merges automatically             │
  └─────────────────────────────────────────┘
```

**Used by:** Amazon Dynamo (original), Riak.

**Trade-off:** Vector size grows with the number of nodes. Causal ordering is accurate, but siblings require application-level resolution.

#### Version Vectors (Causal Context)

Similar to vector clocks but track per-client or per-key versions instead of per-node. More compact in practice.

```
  Key "cart:123":
    Write from Client-X:  {X:5}
    Write from Client-Y:  {Y:3}

  Merge on read:  {X:5, Y:3}  →  union of both carts
```

#### CRDTs (Conflict-Free Replicated Data Types)

Mathematically guaranteed to converge without coordination. Operations commute: applying them in any order yields the same result.

```
  Two common CRDT types:

  G-Counter (Grow-only Counter):
    Node A: +3  →  {A:3, B:0}
    Node B: +5  →  {A:0, B:5}
    Merge:         {A:3, B:5}  →  value = 8

  PN-Counter (Positive-Negative Counter):
    Increment and decrement tracked separately.
    Value = P-counter - N-counter

  OR-Set (Observed-Remove Set):
    Add("apple", id=1) from Node A
    Add("banana", id=2) from Node B
    Remove("apple", id=1) from Node B
    Result: {"banana"}  — unique IDs prevent
            accidental removal of concurrent adds
```

**Used by:** Riak (CRDT data types), Redis Enterprise (Active-Active with CRDTs), AntidoteDB.

**Trade-off:** CRDTs require additional metadata (tombstones, unique IDs) and only support specific data types. Not a general-purpose solution for arbitrary application state.

#### Full Comparison

| Strategy | Complexity | Data Loss Risk | Ordering | Systems |
|----------|-----------|----------------|----------|---------|
| LWW | Lowest | High (silent overwrite) | Wall-clock only | Cassandra, DynamoDB |
| Vector Clocks | Medium | None (siblings preserved) | Causal | Dynamo, Riak |
| Version Vectors | Medium | None | Causal per-key | CouchDB, Riak |
| CRDTs | High | None (mathematically proven) | Commutative ops | Riak, AntidoteDB |
| Custom merge | Application-dependent | Varies | Application-defined | Any system |

---

### 3.2 Replication Lag: Causes & Mitigation

Replication lag is the delay between a write being committed on one node and appearing on its replicas.

#### Causes

```
  ┌────────────────────────────────────────────────────────────┐
  │                    Replication Lag                         │
  └────────────────────────────────────────────────────────────┘

  Source of Lag                     Typical Impact
  ─────────────────────────────────────────────────────────
  1. Network RTT                    1ms (LAN) to 200ms+ (WAN)
  2. Leader disk I/O (WAL flush)    1-10ms (SSD), 10-50ms (HDD)
  3. Follower replay speed          Depends on workload complexity
  4. Follower disk I/O (apply)      Same as #2
  5. Lock contention on leader      Queued writes pile up
  6. Large transactions             Single big tx blocks pipeline
  7. Garbage collection pauses      10ms-seconds (JVM, Go)
  8. Backpressure from slow follower│
     (semi-sync or sync mode)       │

  Total Lag = Σ(all above)
```

#### Cascading Failure Scenario

```
  ┌─────────────────────────────────────────────────────────────┐
  │ Lag-Induced Read Staleness                                  │
  │                                                             │
  │  T1: Client writes to Leader   →  x = "v2"                 │
  │  T2: Client reads from Follower →  x = "v1" (STALE!)       │
  │                                                             │
  │  User sees: "My update didn't go through!"                  │
  │                                                             │
  │  T1        T1+50ms     T1+100ms    T1+200ms                │
  │  │           │            │            │                    │
  │  ▼           ▼            ▼            ▼                    │
  │  Leader:   [v2 committed]─────────────────►                │
  │  Follower:            ───────────► [v2 applied]             │
  │                                        ▲                    │
  │                              150ms replication lag          │
  └─────────────────────────────────────────────────────────────┘
```

#### Mitigation Strategies

| Strategy | How It Works | Trade-off |
|----------|-------------|-----------|
| **Read your writes** | Route read-after-write to the leader | Adds leader load |
| **Monotonic reads** | Pin user's session to one replica | Reduced load balancing |
| **Cross-region pinning** | Pin user to a region's leader | Higher latency for distant users |
| **Synchronous replication** | Wait for follower ack before confirming | Higher write latency |
| **Lag monitoring + circuit breaker** | If lag > threshold, read from leader | Operational complexity |
| **Parallel apply** | Apply replication log in parallel on follower | Complex (cross-tx ordering) |

---

### 3.3 Read-After-Write Consistency

A practical consistency model: **after a client writes, its subsequent reads must see that write.** This is weaker than strong consistency but sufficient for most user-facing applications.

```
  ┌──────────────────────────────────────────────────────────┐
  │             Read-After-Write Consistency                 │
  │                                                          │
  │  Scenario: User updates profile photo                   │
  │                                                          │
  │  ┌──────┐   Write: new_photo.jpg   ┌────────┐           │
  │  │Client┼─────────────────────────►│Leader  │           │
  │  │      │                          │        │           │
  │  │      │◄──────── ACK ────────────┤        │           │
  │  │      │                          └────────┘           │
  │  │      │                                                │
  │  │      │   Read: profile_photo    ┌────────┐           │
  │  │      ├─────────────────────────►│Follower│ ❌ STALE! │
  │  │      │◄── old_photo.jpg ───────┤        │            │
  │  │      │                          └────────┘           │
  │  └──────┘                                                │
  │                                                          │
  │  Fix: Route this client's read to the leader             │
  │  OR wait until follower catches up                       │
  └──────────────────────────────────────────────────────────┘
```

#### Implementation Patterns

**Pattern 1: Leader routing on recent writes**

```
  if (client.lastWriteTime > follower.syncTime) {
    routeToLeader();
  } else {
    routeToFollower();
  }
```

**Pattern 2: Session affinity**

After a write, pin the session to the node (or region) that accepted it for a configurable window.

**Pattern 3: Token-based freshness**

The leader returns a replication token (LSN, commit timestamp). The client includes it in subsequent reads. The replica waits until it has applied up to that token.

```
  Leader returns:  lsn = 45678

  Client reads from Follower with:  min_lsn = 45678

  Follower logic:
    if (follower.applied_lsn < 45678) {
      wait_until(follower.applied_lsn >= 45678);
    }
    return result;
```

---

### 3.4 Quorum Reads & Writes

In leaderless systems (Dynamo-style), consistency is controlled by quorum parameters.

```
  N = replication factor (total copies)
  W = write quorum (min acks for write success)
  R = read quorum (min responses for read)

  ┌─────────────────────────────────────────────────────────┐
  │                  Quorum Intersection                      │
  │                                                         │
  │  R + W > N  ⟹  read and write sets MUST overlap        │
  │                                                         │
  │  ┌───┬───┬───┬───┬───┐   N = 5                        │
  │  │ A │ B │ C │ D │ E │                                 │
  │  └───┴───┴───┴───┴───┘                                 │
  │                                                         │
  │  Write (W=3):  [A, B, C]  ← acknowledged               │
  │  Read  (R=3):  [C, D, E]  ← queried                    │
  │                                                         │
  │  Intersection: [C]  →  at least one node has the       │
  │  latest write. Latest-wins or merge resolves.           │
  └─────────────────────────────────────────────────────────┘
```

#### Sloppy Quorum

When nodes are down, Dynamo uses "hinted handoff": writes are routed to the next available nodes. This sacrifices the quorum guarantee for availability.

```
  Normal:   Write to [A, B, C]  →  W=3 satisfied
  Degraded: A is down → Write to [B, C, D]  →  W=3 satisfied
            But read quorum [A, B, C] may miss the value

  Hinted Handoff: D holds the write temporarily,
  forwards to A when A recovers.
```

---

## 4. Trade-offs (CAP / PACELC)

### CAP Theorem and Replication

The CAP theorem states that in the presence of a **network partition (P)**, a system must choose between **Consistency (C)** and **Availability (A)**. Replication strategy determines this choice.

```
  ┌─────────────────────────────────────────────────────────────┐
  │                    CAP Triangle                             │
  │                                                             │
  │                       Consistency                           │
  │                          (C)                                │
  │                         /   \                               │
  │                        /     \                              │
  │                  CP   /       \   CA (theoretical)          │
  │                    /           \                            │
  │                   /             \                           │
  │              ────/───────────────\────                      │
  │             /                     \                         │
  │            /    Availability      \                        │
  │           /         (A)            \                       │
  │          /                          \                      │
  │    Partition Tolerance (P) ── The baseline assumption       │
  │                                                             │
  │    ┌──────────────────────────────────────────────────┐    │
  │    │ Replication Pattern    │ CAP Choice │ Rationale   │    │
  │    ├──────────────────────────────────────────────────┤    │
  │    │ Sync leader-based      │ CP         │ Leader must │    │
  │    │                        │            │ confirm all │    │
  │    │                        │            │ followers   │    │
  │    ├────────────────────────┼────────────┼─────────────┤    │
  │    │ Async leader-based     │ AP (soft)  │ Writes avail│    │
  │    │                        │            │ but stale   │    │
  │    ├────────────────────────┼────────────┼─────────────┤    │
  │    │ Multi-leader           │ AP         │ All leaders │    │
  │    │                        │            │ stay up,    │    │
  │    │                        │            │ conflicts   │    │
  │    │                        │            │ resolved    │    │
  │    │                        │            │ later       │    │
  │    ├────────────────────────┼────────────┼─────────────┤    │
  │    │ Leaderless (W+R>N)     │ AP / CP    │ Tunable via │    │
  │    │                        │            │ W, R params │    │
  │    └──────────────────────────────────────────────────┘    │
  └─────────────────────────────────────────────────────────────┘
```

### PACELC Extension

PACELC extends CAP: **if there is no Partition (P), the system chooses between Latency (L) and Consistency (E)**.

```
  ┌───────────────────────────────────────────────────────────────┐
  │                    PACELC Matrix                               │
  │                                                               │
  │  Condition         │ Choice          │ Systems                │
  │ ───────────────────┼─────────────────┼────────────────────── │
  │  Partition (P)     │ A (Availability)│ Cassandra, Dynamo,     │
  │                    │                 │ Riak, CouchDB          │
  │                    ├─────────────────┼────────────────────── │
  │                    │ C (Consistency) │ Spanner, CockroachDB,  │
  │                    │                 │ etcd, ZooKeeper        │
  │ ───────────────────┼─────────────────┼────────────────────── │
  │  No Partition      │ L (Latency)     │ Cassandra, DynamoDB,   │
  │  (EL choice)       │                 │ MongoDB (default)      │
  │                    ├─────────────────┼────────────────────── │
  │                    │ E (Consistency) │ Spanner, CockroachDB,  │
  │                    │                 │ TiDB, FoundationDB     │
  │ ───────────────────┼─────────────────┼────────────────────── │
  │                                                               │
  │  Full PACELC Notation:                                        │
  │    Cassandra:  PA/EL  (Partition→Availability, Else→Latency)  │
  │    Spanner:    PC/EC  (Partition→Consistency, Else→Consist.)  │
  │    CockroachDB:PC/EC  (Raft consensus everywhere)             │
  │    MongoDB:    PA/EL  (default), PA/EC (with concern:majority)│
  │    MySQL sync: PC/EC  (but degrades to PA/EL on partition)    │
  └───────────────────────────────────────────────────────────────┘
```

### Replication Choice Determines CAP Position

```
  ┌──────────────────────────────────────────────────────────────┐
  │          How Replication Choice Maps to CAP                   │
  │                                                              │
  │  You Choose:                  │ System Becomes:              │
  │  ─────────────────────────────┼────────────────────────────  │
  │  Synchronous replication      │ CP — writes block if follower│
  │  (all replicas must confirm)  │ is unreachable               │
  │                               │                              │
  │  Asynchronous replication     │ AP — writes succeed even if  │
  │  (fire-and-forget)            │ followers are lagging/down   │
  │                               │ but reads may be stale       │
  │                               │                              │
  │  Semi-synchronous             │ Between CP and AP — at least │
  │  (≥1 replica must confirm)    │ one copy survives, but may   │
  │                               │ lose one follower's worth    │
  │                               │                              │
  │  Multi-leader                 │ AP — all leaders available,  │
  │  (concurrent writes)          │ conflicts resolved later     │
  │                               │                              │
  │  Leaderless + quorum (W+R>N)  │ Configurable — tune W,R to   │
  │                               │ slide along the C-A axis     │
  │                               │                              │
  │  Leaderless + sloppy quorum   │ AP — prioritizes availability│
  │  (hinted handoff)             │ over strict consistency      │
  └──────────────────────────────────────────────────────────────┘
```

### Cost vs Performance

| Approach | Storage Overhead | Network Overhead | Operational Cost |
|----------|-----------------|-------------------|------------------|
| Single-leader async | 2-3× | Low (one replication stream) | Low |
| Single-leader sync | 2-3× | Medium (blocking acks) | Medium |
| Multi-leader | 2-5× | High (N×N replication mesh) | High (conflict debugging) |
| Leaderless (N=3) | 3× | Medium (gossip + anti-entropy) | Medium-High (tuning) |
| Leaderless (N=5+) | 5×+ | High (more replicas, more gossip) | High |

---

## 5. Influence & Legacy

### Technology Genealogy

```
  ┌──────────────────────────────────────────────────────────────┐
  │              Replication Technology Genealogy                │
  │                                                              │
  │  1980s                                                       │
  │  ┌──────────────┐                                            │
  │  │ DB2/QRep     │  Early commercial replication              │
  │  │ (IBM)        │                                            │
  │  └──────┬───────┘                                            │
  │         │                                                     │
  │  1990s  ▼                                                     │
  │  ┌──────────────┐   ┌──────────────┐                         │
  │  │ MySQL        │   │ PostgreSQL   │                         │
  │  │ Master-Slave │   │ Warm Standby │                         │
  │  │ (binlog)     │   │ (WAL)        │                         │
  │  └──────┬───────┘   └──────┬───────┘                         │
  │         │                  │                                  │
  │  2000s  ▼                  ▼                                  │
  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐      │
  │  │ MySQL        │   │ PG Streaming │   │ MongoDB      │      │
  │  │ Semi-Sync    │   │ Replication  │   │ Oplog/RS     │      │
  │  └──────┬───────┘   └──────────────┘   └──────┬───────┘      │
  │         │                                     │               │
  │         │              ┌──────────────┐       │               │
  │         └─────────────►│Amazon Dynamo │◄──────┘               │
  │                        │(2007)        │                        │
  │                        │ Vector Clock │                        │
  │                        │ Sloppy Quorum│                        │
  │                        └──────┬───────┘                        │
  │               ┌───────────────┼───────────────┐                │
  │  2010s        ▼               ▼               ▼                │
  │         ┌──────────┐  ┌────────────┐  ┌────────────┐          │
  │         │Cassandra │  │ Riak       │  │ DynamoDB   │          │
  │         │(LWW +    │  │(VC + CRDT) │  │(managed,   │          │
  │         │ tunable) │  │            │  │ internal)  │          │
  │         └──────────┘  └────────────┘  └────────────┘          │
  │                                                              │
  │         ┌──────────────┐   ┌──────────────┐                  │
  │         │ Google Spanner│   │CockroachDB   │                  │
  │         │(TrueTime +   │   │(Raft + HLC)  │                  │
  │         │ Paxos)        │   │ Multi-active  │                  │
  │         └──────────────┘   └──────┬───────┘                  │
  │                                   │                           │
  │                          ┌────────▼────────┐                 │
  │                          │   YugabyteDB    │                 │
  │                          │ (Raft + PG API) │                 │
  │                          └─────────────────┘                 │
  │                                                              │
  │  2020s: Convergence                                          │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
  │  │ Cloud-native │  │ CRDT-based   │  │ AI/ML work-  │       │
  │  │ multi-region │  │ Active-Active│  │ loads demand │       │
  │  │ databases    │  │ databases    │  │ new patterns │       │
  │  └──────────────┘  └──────────────┘  └──────────────┘       │
  └──────────────────────────────────────────────────────────────┘
```

### Design Patterns That Became Standards

1. **Replication Log** — Every modern database uses some form of append-only log (WAL, binlog, oplog, Raft log). The log is the universal interface for replication.
2. **Leader Election** — Raft/Paxos-based leader election replaced manual failover scripts.
3. **Quorum-based Reads/Writes** — The R+W>N formula is the foundation of tunable consistency.
4. **Vector Clocks** — Causal ordering without a central clock; still used in distributed coordination.
5. **Hinted Handoff** — Accept writes on behalf of unavailable nodes; reconcile later.
6. **Anti-Entropy via Merkle Trees** — Efficient divergence detection for large datasets.

### Current Relevance

| Scenario | Recommended Pattern | Why |
|----------|-------------------|-----|
| Financial transactions | Synchronous leader-based or Raft (Spanner-style) | Zero data loss |
| Global web app | Multi-leader or async leader with read-your-writes | Low latency writes |
| IoT/edge data ingestion | Leaderless (Cassandra-style) | High write throughput, graceful degradation |
| Collaborative editing | Multi-leader + CRDTs | Conflict-free merging |
| Analytics/BI | Async leader-based | Staleness acceptable, cost-effective |

---

## 6. Comparison Matrix

```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                         Replication Pattern Comparison                                 │
├──────────────────┬──────────────────┬──────────────────┬───────────────────────────────┤
│ Dimension        │ Leader-Based     │ Multi-Leader     │ Leaderless                    │
├──────────────────┼──────────────────┼──────────────────┼───────────────────────────────┤
│ Writers          │ 1 (leader)       │ N (any leader)   │ Any node                      │
│ Read scalability │ High (followers) │ High             │ High                          │
│ Write scalability│ Low (bottleneck) │ Medium-High      │ High                          │
│ Consistency      │ Strong (sync)    │ Eventual         │ Eventual / tunable            │
│                  │ or eventual (as) │ or causal        │                               │
│ Conflict handling │ Not needed      │ Required         │ Required (vector clocks/CRDTs)│
│ Failover         │ Election needed  │ No election      │ No election                   │
│ Latency (write)  │ 1 RTT (async)    │ 1 RTT local      │ 1 RTT (coordinator)           │
│                  │ 2 RTT (sync)     │ + replication    │ + quorum wait                 │
│ Data loss risk   │ None (sync)      │ None (conflicts  │ Possible (sloppy quorum)      │
│                  │ Possible (async) │ resolved later)  │                               │
│ Operational      │ Simple           │ Complex          │ Complex (tuning)              │
│ complexity       │                  │                  │                               │
│ CAP default      │ CP (sync) /      │ AP               │ AP                            │
│                  │ AP (async)       │                  │                               │
│ PACELC           │ PC/EC (sync) /   │ PA/EL            │ PA/EL                         │
│                  │ PA/EL (async)    │                  │                               │
│ Examples         │ MySQL, PG,       │ CockroachDB,     │ Dynamo, Cassandra,            │
│                  │ MongoDB (RS)     │ YugabyteDB       │ Riak                          │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. References

1. **Designing Data-Intensive Applications** — Martin Kleppmann (2017), Chapters 5-6. The definitive practical guide to replication patterns.
2. **Dynamo: Amazon's Highly Available Key-value Store** — DeCandia et al., SOSP 2007. Introduced leaderless replication, vector clocks, sloppy quorum, hinted handoff.
3. **Spanner: Google's Globally-Distributed Database** — Corbett et al., OSDI 2012. Synchronous replication with TrueTime, external consistency.
4. **CockroachDB: The Resilient Geo-Distributed SQL Database** — Cockroach Labs. Multi-active Raft-based replication.
5. **Cassandra: A Decentralized Structured Storage System** — Lakshman & Malik, 2009. Dynamo + Bigtable hybrid.
6. **A comprehensive study of Convergent and Commutative Replicated Data Types** — Shapiro et al., 2011. CRDT foundations.
7. **MySQL 5.7 Reference Manual: Replication** — Oracle. Semi-sync replication, group replication.
8. **PostgreSQL Documentation: High Availability** — PostgreSQL Global Development Group. Streaming replication, logical replication.
9. **PACELC Theorem** — Daniel Abadi, 2012. Extends CAP with latency/consistency trade-off in non-partitioned state.
10. **Harmony: Replication in the Presence of Partitions** — Yale University. Analysis of replication strategies under partition.

---

*Last updated: 2026-06-11 | Part of the Architect Skill references/concepts/*
