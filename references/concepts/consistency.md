# Consistency Models in Distributed Systems

> **Category:** Core Concepts | **Output Format:** 6 (Core Concepts)  
> **References:** Herlihy & Wing (1990) Linearizability, Lamport (1979) Sequential Consistency, Terry et al. (1995) Bayou, Gilbert & Lynch (2002) CAP, Abadi (2012) PACELC, Bailis et al. (2013) Causal+ Consistency, Shapiro et al. (2011) CRDTs

---

## Overview

In a single-machine system, memory is coherent by definition — there is one copy, one order of operations, and every read sees the latest write. In a distributed system, data is replicated across multiple nodes for availability, durability, and performance. The central question becomes:

> *When a client reads a value, which version will it see, and in what order do concurrent operations appear to execute?*

A **consistency model** is a formal contract between the storage system and the client, defining the guarantees the system provides about the visibility and ordering of operations across replicas. There is no single "correct" model — each represents a different point in the design space trading off consistency, latency, availability, and implementation complexity.

---

## 1. Consistency Hierarchy (Strength Ordering)

```
┌─────────────────────────────────────────────────────────┐
│              Consistency Models — Strength Ladder        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │ STRONGEST                                       │   │
│  │                                                 │   │
│  │  Linearizability (Strong / Atomic)              │   │
│  │  └─ Real-time single-copy semantics             │   │
│  │                                                 │   │
│  │  Sequential Consistency                         │   │
│  │  └─ Program order preserved, global total order │   │
│  │                                                 │   │
│  │  Causal Consistency                             │   │
│  │  └─ Causally-related operations ordered         │   │
│  │     ┌─ Causal+ (Causal + session guarantees)    │   │
│  │                                                 │   │
│  │  Session Consistency                            │   │
│  │  └─ Per-session: RYW + MR + MW + SW             │   │
│  │     ├─ Read-Your-Writes (RYW)                   │   │
│  │     ├─ Monotonic Reads (MR)                     │   │
│  │     ├─ Monotonic Writes (MW)                    │   │
│  │     └─ Session Writes (SW)                      │   │
│  │                                                 │   │
│  │  Eventual Consistency                           │   │
│  │  └─ Converge if no new writes                   │   │
│  │     ┌─ Strong Eventual Consistency (CRDTs)      │   │
│  │                                                 │   │
│  │  WEAKEST                                        │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ──────────────────────── stronger ─────────────────── │
│           │                                             │
│           │     Each level below relaxes guarantees     │
│           │     of the levels above                     │
│           ▼                                             │
│  ──────────────────────── weaker ────────────────────  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Key insight:** Moving down the ladder means each read may see *older* values, but the system can tolerate more failures and achieve lower latency. Moving up means the client gets a simpler mental model (appears as single copy), but the system must coordinate more, increasing latency and reducing availability under partitions.

---

## 2. Consistency Models — Detailed Analysis

### 2.1 Linearizability (Strong / Atomic Consistency)

#### Definition

Linearizability, introduced by Herlihy and Wing (1990), is the strongest consistency model for shared objects. A history of operations is linearizable if:

1. **Real-time ordering:** If operation A completes before operation B starts, then A must appear before B in the total order.
2. **Single-copy semantics:** The result is equivalent to some sequential execution that respects real-time ordering and each operation's specification.

In practice: *every read returns the value of the most recent completed write*.

#### Intuitive Explanation

Imagine a single global register. Every write atomically updates it. Every read sees whatever the register currently holds. Even if there are 100 replicas, the system behaves *as if* there is only one copy — clients cannot observe replication lag or stale data.

#### Formal Model

```
Timeline (real time →):

  W1: ──write(x=1)─────●
  W2:         ──write(x=2)─────●
  R1:                     ──read→ 2    ✅ linearizable
  R1':                    ──read→ 1    ❌ impossible: W1 ended before

  A linearization point ● exists for each operation,
  and the induced total order respects real-time ordering.
```

#### Properties

| Property | Guarantee |
|----------|-----------|
| Safety   | No stale reads; latest write always visible |
| Ordering | Real-time partial order extended to total order |
| Intuition | "It just works" — like a single non-distributed variable |

#### Implementation Requirements

- **Consensus required.** Paxos, Raft, or multi-Paxos variants.
- **Synchronous replication.** Writes must be durably committed to a quorum before returning.
- **Read coordination.** Reads may need to go through consensus (e.g., Raft read index) or use a lease-based leader to confirm freshness.
- **Clock assumptions.** Does NOT require synchronized clocks — ordering is derived from message causality and consensus, not wall clocks.

#### Systems Using Linearizability

- **Google Spanner** — TrueTime + Paxos for external consistency (superset of linearizability).
- **etcd** — Raft consensus; default reads are linearizable via `QuorumRead`.
- **ZooKeeper** — Zab protocol; sequential consistency by default, linearizable with sync barrier.
- **CockroachDB** — Raft-based; serializable and linearizable isolation.
- **TiKV** — Raft consensus; snapshot isolation + linearizable reads via `checkLeader` or follower read with lease.

#### Cost

- **Latency:** O(number of network RTTs for consensus round-trip). Typically 2 RTTs for write (prepare + commit).
- **Availability:** Under network partition, linearizable systems must sacrifice availability (CAP: CP systems).
- **Throughput:** Limited by consensus leader bottleneck; multi-Paxos optimizations help but don't eliminate the constraint.

---

### 2.2 Sequential Consistency

#### Definition

Introduced by Lamport (1979), sequential consistency requires:

1. **Program order preservation:** Operations from each individual process appear in the order specified by that process's program.
2. **Global total order:** There exists a single total order of all operations that is consistent with all processes' program orders.
3. **No real-time constraint:** Unlike linearizability, the global order does NOT need to respect real-time ordering of operations.

#### Key Difference from Linearizability

```
Linearizability:    Total order MUST respect real-time ordering
Sequential Cons.:   Total order MUST respect program order,
                    but MAY reorder across processes even if
                    one operation completed before another started

Example:
  P1: W(x=1)  ─────●
  P2:          W(x=2) ─────●
  P3: R(x) ──→ 2    ← This is allowed under sequential consistency
                     if P3's read happens to interleave before W1's
                     linearization point, even though W1 "finished"
                     earlier in real time.

  Under linearizability: R(x) must return 2 only if W2's
  linearization point is after W1's, and R(x) is after W2's.
```

#### Practical Significance

- Sequential consistency is easier to implement than linearizability because it doesn't require real-time tracking.
- However, it is still expensive: a global total order still requires coordination.
- Most real-world systems implement either linearizability (stronger, clearer semantics) or relax further to causal/eventual. Sequential consistency is primarily of theoretical and multiprocessor architecture significance.

#### Systems

- **Java Memory Model** (pre-JMM relaxation): aimed for sequential consistency as default.
- **ZooKeeper**: provides sequential consistency as its base guarantee.

---

### 2.3 Causal Consistency

#### Definition

Causal consistency, formalized by Ahamad et al. (1995), guarantees that:

> If operation A **causally precedes** operation B (in the sense of Lamport's happened-before relation), then all processes must observe A before B.

Operations that are **concurrent** (no causal relationship) may be observed in different orders by different processes.

#### Causal Ordering Rules (Happened-Before, `→`)

```
Lamport's happened-before relation (→):

1. Program order:   If a and b are operations in the same process,
                    and a occurs before b, then a → b.

2. Message passing: If a is a send and b is the corresponding receive,
                    then a → b.

3. Write-to-read:   If a writes value v and b reads v, then a → b.

4. Transitivity:    If a → b and b → c, then a → c.

Operations a, b are CONCURRENT (a || b) iff neither a → b nor b → a.
```

#### Intuitive Example

```
Scenario: Social media post and comments

  Alice:  W(post="Hello!") ──────────┐
                                     │ (causal dependency)
  Bob:    R(post="Hello!") → W(reply="Hi Alice!")
                                     │
  Carol:  R(post="Hello!") → R(reply="Hi Alice!")  ✅ OK
           (Carol sees both, in causal order)

  Dave:   R(reply="Hi Alice!") → R(post="Hello!")  ✅ allowed
           (if reply and post are concurrent from Dave's view,
            but causally reply → post means Dave must see post first)

  Eve:    R(reply="Hi Alice!") without seeing post  ❌ VIOLATION
           (reply causally depends on post; Eve broke causal order)
```

#### Why Causal Consistency Is Powerful

- **It's the strongest consistency model achievable without sacrificing availability** during network partitions (proved by Mahajan et al., 2011).
- It captures the "natural" ordering that humans expect — if you reply to my post, everyone should see the post before the reply.
- It allows concurrent operations to diverge without coordination, enabling high availability.

#### Implementing Causal Ordering

**Version Vectors (Vector Clocks):**

```
Each replica maintains a vector clock VC = [v₁, v₂, ..., vₙ]
where vᵢ is the number of operations from replica i that this
replica has observed.

Write at replica i:
  VC[i] += 1
  Attach VC to the write

Receive write with VC' at replica j:
  For all k: VC[j][k] = max(VC[j][k], VC'[k])
  VC[j][j] += 1  (for the local apply)

Causal ordering comparison:
  VC ≤ VC'  iff  ∀k: VC[k] ≤ VC'[k]
  VC < VC'  iff  VC ≤ VC' AND VC ≠ VC'
  VC || VC' iff  neither VC < VC' nor VC' < VC  (concurrent)

A write with VC_a must be delivered before VC_b if VC_a < VC_b.
```

**Delivery Guarantees:** To enforce causal consistency, the system must ensure **causal delivery** — a message is not delivered to the application until all its causal predecessors have been delivered. This can be implemented with vector clocks and a delivery buffer at each replica.

#### Systems Using Causal Consistency

- **AntidoteDB** — CRDT-based, causal consistency by default.
- **COPS / Eiger** (Lloyd et al., 2011) — causally consistent key-value store with explicit dependency tracking.
- **Orleans** — Microsoft's actor framework, provides causal consistency within grain activation.
- **CockroachDB** — causal consistency via MVCC timestamps for non-serializable transactions.
- **YugabyteDB** — hybrid logical clocks for causal ordering.

#### Causal+ Consistency

Bailis et al. (2013) introduced **Causal+**, which combines causal consistency with session guarantees (see Section 2.5):

- Causal ordering across all clients
- Read-your-writes within a session
- Monotonic reads within a session
- Monotonic writes within a session

This is the practical "sweet spot" for many applications — it provides intuitive ordering for causally related data while remaining available under partitions.

---

### 2.4 Eventual Consistency

#### Definition

Eventual consistency is the weakest widely-used consistency model:

> If no new updates are made to a given data item, eventually all reads to that item will return the same value.

There are **no guarantees** about:
- **When** convergence happens (could be milliseconds or hours)
- **What order** intermediate reads return (could be any version)
- **Which version** wins when concurrent writes conflict

#### Formal Model

```
Let W be a sequence of write operations to key k.
If W is finite (no more writes after some time t), then:

  ∃t' ≥ t : ∀r read(k) at time ≥ t', r returns the same value v

Nothing is guaranteed for reads before t'.
```

#### The Conflict Resolution Problem

Eventual consistency raises a critical question: *when two replicas concurrently write different values, which one wins?*

**Resolution strategies:**

| Strategy | Mechanism | Example |
|----------|-----------|---------|
| Last-Writer-Wins (LWW) | Use timestamps; latest wins | Dynamo, Cassandra |
| Vector Clocks | Detect conflicts; application resolves | Dynamo (original) |
| Multi-value | Return all concurrent values | Riak, Dynamo |
| CRDTs | Mathematically guaranteed merge | AntidoteDB, Redis GROW-ONLY SET |
| Custom merge | Application-specific resolution | CouchDB, Git (conceptually) |

**LWW pitfalls:** Clock skew can cause newer writes to appear older, silently losing data. Most production systems using LWW add clock synchronization (NTP, or Spanner's TrueTime) to minimize skew.

#### Strong Eventual Consistency (SEC)

A refinement of eventual consistency:

> If two replicas have received the same set of updates, they are guaranteed to be in the same state — without any coordination.

CRDTs (Conflict-free Replicated Data Types) achieve SEC:

```
CRDT Properties:
1. Commutative:  apply(a, apply(b, state)) = apply(b, apply(a, state))
2. Associative:  apply(a, apply(b, apply(c, state))) = ...
3. Idempotent:   apply(a, apply(a, state)) = apply(a, state)

These properties ensure that regardless of delivery order,
all replicas converge to the same state.
```

**CRDT Types:**

| Type | Operation | Merge Strategy |
|------|-----------|----------------|
| G-Counter | Increment only | `max(local, remote)` per replica |
| PN-Counter | Increment + Decrement | G-Counter for P + G-Counter for N |
| G-Set | Add only | Union |
| 2P-Set | Add + Remove | Added set ∪ Removed set; element in set iff in Added and not in Removed |
| LWW-Register | Set value | Last-write-wins with timestamp |
| OR-Set | Add + Remove | Tags per element; remove removes matching tags |
| LWW-Map / OR-Map | Key-value operations | Per-key CRDT composition |

#### Systems Using Eventual Consistency

- **Amazon Dynamo** (2007) — pioneered eventual consistency with vector clocks and tunable consistency (N, R, W, quorum).
- **Apache Cassandra** — tunable consistency per query; default is ONE (eventual).
- **Amazon DynamoDB** — configurable: eventual (default) or strong (consistent read).
- **Riak** — vector clocks, sibling resolution, eventual by default.
- **CouchDB** — MVCC + eventual consistency via replication.
- **DNS** — the original eventually consistent system (TTL-based propagation).

#### When Eventual Consistency Is Appropriate

| Good Fit | Poor Fit |
|----------|----------|
| Social media feeds | Financial transactions |
| Shopping cart (add items) | Inventory decrement |
| Likes / counters | Account balance |
| User profile updates | Seat reservation |
| Activity logs / metrics | Leader election |

---

### 2.5 Session Consistency Guarantees

Session guarantees are per-client (per-session) promises that relax global consistency while preserving an intuitive experience for individual users. They are typically **composed** into a "session consistency" package.

#### 2.5.1 Read-Your-Writes (RYW)

> After a client writes value v to key k, any subsequent read of k by the same client returns v (or a later value).

```
Session S:
  W(k=x) → R(k) must return ≥ x

  Without RYW:
    W(k=5) ───► [propagating to other replicas]
    R(k) ─────► returns 3  ← stale! user sees their write disappear

  With RYW:
    W(k=5) ───► [route reads to replica that has this write,
                  or wait for replication]
    R(k) ─────► returns 5  ✅
```

**Implementation approaches:**
- **Sticky reads:** Route all reads from a session to the same replica (the one that received the writes).
- **Read-after-write wait:** Block reads until the write has propagated.
- **Version tracking:** Client tracks its last-write version; server ensures reads return at least that version.

#### 2.5.2 Monotonic Reads (MR)

> If a client reads value v for key k, any subsequent read of k by the same client returns v or a newer value — never an older one.

```
Session S:
  R₁(k) → returns 5
  R₂(k) → must return ≥ 5

  Without MR:
    R₁(k) ──► 5  (from replica A, has latest)
    R₂(k) ──► 3  (from replica B, lagging behind)  ← confusing!

  With MR:
    R₁(k) ──► 5  (from replica A)
    R₂(k) ──► ≥ 5 (route to replica A, or wait until B catches up)
```

**Implementation:** Pin a session to a specific replica, or track the version a session has seen and ensure subsequent reads are from a replica at least as up-to-date.

#### 2.5.3 Monotonic Writes (MW)

> If a client writes value v₁ to key k, and later writes v₂ to k, the system ensures v₁ is propagated/committed before v₂.

```
Session S:
  W₁(k=v₁) → W₂(k=v₂)
  System must ensure: any replica that sees v₂ also sees v₁

  Without MW:
    W₁(k=5) ──► replica B (slow path)
    W₂(k=7) ──► replica A (fast path)
    Replica A shows k=7 but not k=5  ← order violation

  With MW:
    W₁(k=5) ──► serialize through same path as W₂,
                  or ensure dependency propagation
    W₂(k=7) ──► waits for W₁ propagation
```

#### 2.5.4 Session Writes / Writes Follow Reads (SW)

> If a client reads value v and then writes to the same key, the write is guaranteed to be applied on top of (or after) the version that produced v.

This prevents **lost update** anomalies in session contexts.

#### Session Consistency — Combined

Most production systems offer "session consistency" as a bundle of RYW + MR + MW + SW:

```
Session Consistency = RYW ∩ MR ∩ MW ∩ SW

For a single client session:
  ✅ My writes are visible to my reads
  ✅ I never see data go backwards
  ✅ My writes are applied in order
  ✅ My writes build on what I read
```

This is often sufficient for user-facing applications (social networks, content management, shopping carts) while avoiding the coordination cost of full linearizability.

---

## 3. PACELC Theorem

### 3.1 CAP Theorem (Recap)

Gilbert and Lynch (2002) formally proved Brewer's CAP conjecture:

> In a distributed system, when a **network partition** occurs, you must choose between **Consistency** and **Availability**.

```
       ┌─────────────┐
       │     CAP     │
       │   Triangle  │
       │             │
       │   C ─── A   │
       │    \   /    │
       │     \ /     │
       │      P      │
       │             │
       │ Pick 2 of 3 │
       │ (during P)  │
       └─────────────┘

  C (Consistency):  Every read returns the latest write
  A (Availability): Every request receives a response
  P (Partition):    Network partition exists
```

**The limitation:** CAP only tells you what to do *during a partition*. It says nothing about the normal (no-partition) case, which is the system's steady state 99.9%+ of the time.

### 3.2 PACELC Extension

Daniel Abadi (2012) extended CAP to cover both cases:

> **P**artition: choose between **A**vailability and **C**onsistency  
> **E**lse (no partition): choose between **L**atency and **C**onsistency

```
  PACELC Decision Tree:

         Is there a network partition?
         /                           \
       YES                            NO
        |                              |
   Choose A or C                Choose L or C
   (same as CAP)                (the EL part)

  ┌──────────────────────────────────────────────────┐
  │                  PACELC Classifications           │
  ├──────────────┬───────────────────────────────────┤
  │  PA/EL       │ Partition→Availability,            │
  │              │ Else→Latency                       │
  │              │ (e.g., Dynamo, Cassandra, CouchDB) │
  ├──────────────┼───────────────────────────────────┤
  │  PA/EC       │ Partition→Availability,            │
  │              │ Else→Consistency                   │
  │              │ (e.g., MongoDB with replicas)      │
  ├──────────────┼───────────────────────────────────┤
  │  PC/EL       │ Partition→Consistency,             │
  │              │ Else→Latency                       │
  │              │ (e.g., HBase, some Spanner configs)│
  ├──────────────┼───────────────────────────────────┤
  │  PC/EC       │ Partition→Consistency,             │
  │              │ Else→Consistency                   │
  │              │ (e.g., Bigtable, traditional RDBMS,│
  │              │  etcd, ZooKeeper)                  │
  └──────────────┴───────────────────────────────────┘
```

### 3.3 PACELC Analysis of Key Systems

| System | Partition → | Else → | PACELC | Notes |
|--------|------------|--------|--------|-------|
| **DynamoDB** | Availability | Latency | PA/EL | Eventual consistency default; optional consistent reads |
| **Cassandra** | Availability | Latency | PA/EL | Tunable consistency; default ONE = latency-optimized |
| **MongoDB** | Availability | Consistency | PA/EC | Primary-secondary; reads from primary are consistent |
| **HBase** | Consistency | Latency | PC/EL | CP system; partitioned regions become unavailable |
| **Spanner** | Consistency | Consistency | PC/EC | TrueTime + Paxos; strong consistency always |
| **etcd** | Consistency | Consistency | PC/EC | Raft; linearizable reads/writes |
| **ZooKeeper** | Consistency | Consistency | PC/EC | Zab; sequential consistency |
| **CockroachDB** | Consistency | Consistency | PC/EC | Raft; serializable by default |
| **Dynamo (original)** | Availability | Latency | PA/EL | Vector clocks, eventual consistency |
| **Riak** | Availability | Latency | PA/EL | Configurable; default eventual |
| **TiDB** | Consistency | Consistency | PC/EC | Raft on TiKV; snapshot isolation |
| **OceanBase** | Consistency | Consistency | PC/EC | Multi-Paxos; strong consistency |

### 3.4 The Latency-Consistency Trade-Off (EL)

Even without partitions, there is a fundamental tension:

```
  Want strong consistency?
  → Coordinate reads and writes
  → Extra network round trips
  → Higher latency

  Want low latency?
  → Serve reads from nearest replica
  → May serve stale data
  → Weaker consistency

  This is the EL trade-off — the "everyday" choice that
  systems make 99.9% of the time.
```

**Concrete example — read latency comparison:**

```
  Linearizable read (etcd/Raft):
    Client → Leader → Quorum acknowledgment → Client
    Latency: ~2 RTTs + processing

  Eventual read (Cassandra LOCAL_ONE):
    Client → Nearest replica → Client
    Latency: ~1 RTT + processing

  Difference: ~1 RTT per read
  At 1ms RTT: 2ms vs 1ms (small)
  At 100ms RTT (cross-region): 200ms vs 100ms (significant)
```

---

## 4. Implementation Mechanisms

### 4.1 Quorum Reads and Writes (Dynamo-style)

The Dynamo paper (2007) introduced tunable consistency through the **(N, R, W)** parameterization:

```
  N = number of replicas
  R = number of replicas that must acknowledge a read
  W = number of replicas that must acknowledge a write

  Quorum condition:  R + W > N

  If R + W > N:
    → Read and write quorums MUST overlap
    → At least one replica has seen the latest write
    → Read is guaranteed to see the latest write (strong consistency)

  Common configurations:

    N=3, R=1, W=1  → R+W=2 ≤ 3  → EVENTUAL (fast, no guarantee)
    N=3, R=2, W=2  → R+W=4 > 3  → STRONG (quorum)
    N=3, R=3, W=1  → R+W=4 > 3  → STRONG (write fast, read thorough)
    N=3, R=1, W=3  → R+W=4 > 3  → STRONG (write thorough, read fast)
    N=3, R=2, W=1  → R+W=3 ≤ 3  → WEAK (overlap not guaranteed)

  Sloppy quorum: When primary replicas are unavailable,
  Dynamo routes to "hinted handoff" nodes. This relaxes
  the quorum guarantee for availability.
```

**Why quorum works:**

```
  Replicas: [R1, R2, R3]

  Write W=2: R1 ✓, R2 ✓  (value v written to 2 of 3)

  Read R=2:  Must query 2 replicas.
    {R1, R2}: both have v → return v  ✅
    {R1, R3}: R1 has v, R3 may be old → read repair returns v  ✅
    {R2, R3}: R2 has v, R3 may be old → read repair returns v  ✅

  Every read quorum overlaps with every write quorum
  in at least one replica that has the latest value.
```

**Read Repair & Anti-Entropy:**
- **Read repair:** During a read, if replicas return different values, the coordinator returns the latest and asynchronously updates the stale replicas.
- **Anti-entropy:** Background process that periodically reconciles all replicas using Merkle trees (efficient difference detection).

### 4.2 Version Vectors

Version vectors (vector clocks) track causal relationships between replica updates:

```
  2-Replica Version Vector:

  Replica A: [a=3, b=1]   — A has applied 3 of its own writes, 1 from B
  Replica B: [a=2, b=2]   — B has applied 2 from A, 2 of its own

  Comparison:
    A vs B:  A[a]=3 > B[a]=2  AND  A[b]=1 < B[b]=2
    → CONCURRENT (neither dominates)

  3-Replica Version Vector:

  R1: [3, 1, 0]    R2: [2, 3, 0]    R3: [3, 2, 1]

  R3 dominates R1?  R3[0]=3≥R1[0]=3, R3[1]=2≥R1[1]=1, R3[2]=1≥R1[2]=0
    → YES (R3 ≥ R1)

  R3 dominates R2?  R3[0]=3≥R2[0]=2, R3[1]=2<R2[1]=3
    → NO (concurrent with R2)
```

**Version vector limitations:**
- **Storage cost:** O(N) per data item where N = number of replicas. For large clusters, this becomes expensive.
- **Partial knowledge:** Without a central authority, version vectors can only track what a specific replica knows, not the global state.
- **Dotted version vectors:** An optimization that tracks per-replica, per-key counters instead of full vectors, reducing overhead.

### 4.3 Hybrid Logical Clocks (HLC)

Hybrid Logical Clocks combine wall-clock time with logical counters to provide a globally consistent ordering that approximates real time:

```
  HLC = (physical_time, logical_counter, node_id)

  HLC ordering:
    (t₁, c₁, n₁) < (t₂, c₂, n₂)  iff
      t₁ < t₂  OR  (t₁ = t₂ AND c₁ < c₂)  OR
      (t₁ = t₂ AND c₁ = c₂ AND n₁ < n₂)

  Update rules:
    On local event:   ts = max(wall_clock, ts) + 1 (logical increment)
    On message recv:  ts = max(ts, sender_ts) + 1

  Properties:
    - Monotonically increasing
    - Respects causality (if a → b, then HLC(a) < HLC(b))
    - Close to wall-clock time (bounded skew from physical time)
```

**Systems using HLC or similar:**
- **CockroachDB** — HLC for transaction timestamps.
- **YugabyteDB** — HybridTime (physical + logical).
- **TiDB** — Oracle-like timestamp oracle (TSO) for ordering.

### 4.4 CRDTs (Conflict-free Replicated Data Types)

CRDTs are data structures designed to converge automatically under concurrent updates:

```
  CRDT Design Principles:

  1. State-based CRDTs (CvRDTs):
     - Each replica has a state
     - Merge function is commutative, associative, idempotent
     - join(lattice): state forms a semi-lattice

  2. Operation-based CRDTs (CmRDTs):
     - Operations are commutative
     - Reliable causal delivery required
     - Apply(op, state) produces same result regardless of order

  Example: G-Counter (Grow-only Counter)

    State: vector [c₁, c₂, ..., cₙ] where cᵢ is replica i's count
    Increment at replica i: cᵢ += 1
    Merge:  result[k] = max(left[k], right[k]) for all k
    Value:  sum(c₁, c₂, ..., cₙ)

    Proof of convergence:
      max() is commutative, associative, idempotent
      → All replicas with same set of increments converge to same sum
```

**CRDT compositions:**
- **Maps:** OR-Map (observe-remove map) combines OR-Sets with per-key CRDTs.
- **Registers:** LWW-Register uses timestamps for single-value types.
- **Text:** RGA (Replicated Growable Array) for collaborative text editing.

---

## 5. Trade-Offs: Consistency vs. Latency vs. Availability

### 5.1 The Fundamental Trade-Off Triangle

```
                   ┌──────────────┐
                   │              │
            Strong │              │ Strong
            Consist│              │ Availab.
                   │              │
                   │              │
        ┌──────────┴──────────────┴──────────┐
        │                                    │
        │         Low Latency                │
        │                                    │
        └────────────────────────────────────┘

  You can have any 2, but not all 3:
    Strong Consistency + Strong Availability → Higher Latency (coordination)
    Strong Consistency + Low Latency → Reduced Availability (need quorum up)
    Strong Availability + Low Latency → Weaker Consistency (eventual)
```

### 5.2 Quantitative Latency Comparison

```
  Consistency Model     | Write Latency    | Read Latency     | Coordination
  ──────────────────────┼──────────────────┼──────────────────┼─────────────
  Linearizability       | 2 RTT (Paxos)    | 1-2 RTT          | Full consensus
  Sequential            | 1-2 RTT          | 1 RTT            | Total ordering
  Causal                | 1 RTT + metadata | 1 RTT            | Causal delivery
  Session (RYW+MR)      | 1 RTT            | 1 RTT (sticky)   | Session routing
  Eventual              | 1 RTT            | 1 RTT            | None
  Strong Eventual       | 1 RTT            | 1 RTT            | CRDT merge

  RTT assumptions:
    Same data center:     ~0.5-2 ms
    Cross-region (US):    ~60-80 ms
    Cross-region (global): ~150-300 ms
```

### 5.3 Consistency Tuning in Practice

Modern systems expose **per-operation consistency levels**, allowing application developers to choose the right trade-off for each access pattern:

**Cassandra / DynamoDB-style levels:**

```
  Consistency Level      | R + W > N? | Use Case
  ───────────────────────┼────────────┼──────────────────────────
  ONE                    | No         | Fast reads, eventual
  QUORUM                 | Yes        | Strong consistency
  ALL                    | Yes        | Maximum safety (slow)
  LOCAL_QUORUM           | Yes (DC)   | Strong within data center
  EACH_QUORUM            | Yes (each) | Cross-DC strong consistency
  SERIAL (LWT)           | Paxos      | Compare-and-set operations
```

### 5.4 Decision Framework

```
  Application Requirement Analysis:

  1. Can stale reads cause correctness bugs?
     YES → Need at least causal consistency, possibly linearizable
     NO  → Eventual or session consistency may suffice

  2. Can lost writes cause correctness bugs?
     YES → Need quorum writes (W > N/2) or serializable transactions
     NO  → LWW or eventual may work

  3. Is user-facing latency critical (< 100ms)?
     YES → Avoid full linearizability across regions
     NO  → Strong consistency is viable

  4. Does the application span multiple geographic regions?
     YES → PACELC: choose between EL during normal ops
     NO  → Single-region strong consistency is practical

  5. What happens during a network partition?
     Must be available → PA systems (Dynamo-style)
     Must be consistent → PC systems (Spanner, etcd)
```

---

## 6. Summary: Consistency Model Comparison Matrix

| Model | Ordering Guarantee | Stale Reads Possible? | Partition Behavior | Coordination Cost | Best For |
|-------|-------------------|----------------------|-------------------|-------------------|----------|
| **Linearizability** | Real-time total order | Never | Sacrifice A | Highest (consensus) | Financial systems, config stores, leader election |
| **Sequential** | Program-order total order | Yes (across processes) | Sacrifice A | High (total ordering) | Multiprocessor memory, some KV stores |
| **Causal** | Causal partial order | Yes (for concurrent ops) | Maintain A | Medium (vector clocks) | Social apps, collaborative editing, comments |
| **Causal+** | Causal + session | No (within session) | Maintain A | Medium | Most user-facing apps |
| **Session (RYW+MR)** | Per-session ordering | No (within session) | Maintain A | Low (session routing) | User profiles, shopping carts |
| **Eventual** | None | Yes, frequently | Maintain A | None | Metrics, feeds, caches, DNS |
| **Strong Eventual** | Convergent state | Yes (during divergence) | Maintain A | Low (CRDT merge) | Collaborative docs, counters, sets |

---

## 7. Key Takeaways

1. **Consistency is a spectrum, not a binary.** Systems can offer different consistency levels for different operations, data items, or sessions.

2. **Causal consistency is the strongest model that doesn't sacrifice availability.** This is a proven theorem (Mahajan et al., 2011) — anything stronger requires coordination that fails under partitions.

3. **PACELC is more useful than CAP for design.** The EL trade-off (latency vs. consistency) governs the system's behavior during its normal operating state, which is the vast majority of the time.

4. **Tunable consistency is the industry standard.** Rather than picking one model globally, modern systems (Cassandra, DynamoDB, Cosmos DB) allow per-query or per-object consistency levels.

5. **CRDTs solve the conflict problem mathematically.** When eventual consistency is chosen, CRDTs provide a principled way to ensure convergence without application-level merge logic.

6. **Session consistency is the practical sweet spot.** For most user-facing applications, RYW + MR + MW provides an intuitive experience at minimal coordination cost.

7. **The implementation determines the cost.** Linearizability via Raft costs 2 RTTs; via TrueTime-assisted commits, it can be 1 RTT. Causal consistency via vector clocks has O(N) metadata; via dependency clocks, it's smaller.

---

## 8. References

1. Herlihy, M. & Wing, J. (1990). *Linearizability: A Correctness Condition for Concurrent Objects*. ACM TOPLAS.
2. Lamport, L. (1979). *How to Make a Multiprocessor Computer That Correctly Executes Multiprocess Programs*. IEEE Transactions on Computers.
3. Gilbert, S. & Lynch, N. (2002). *Brewer's Conjecture and the Feasibility of Consistent, Available, Partition-Tolerant Web Services*. ACM SIGACT News.
4. Abadi, D. (2012). *A Critical Analysis of CAP Theorem*.
5. Terry, D. et al. (1995). *Managing Update Conflicts in Bayou, a Weakly Connected Replicated Storage System*. SOSP.
6. Vogels, W. (2009). *Eventually Consistent*. ACM Queue.
7. Bailis, P. et al. (2013). *Coordination-Avoiding Database Systems*. VLDB. (Causal+ consistency)
8. Lloyd, W. et al. (2011). *Don't Settle for Eventual: Scalable Causal Consistency for Wide-Area Storage with COPS*. SOSP.
9. Mahajan, P. et al. (2011). *Consistency Tradeoffs in Modern Distributed Database System Design*. IEEE Computer.
10. DeCandia, G. et al. (2007). *Dynamo: Amazon's Highly Available Key-value Store*. SOSP.
11. Shapiro, M. et al. (2011). *Conflict-free Replicated Data Types (CRDTs)*. INRIA Research Report.
12. Corbett, J. et al. (2013). *Spanner: Google's Globally-Distributed Database*. ACM TOCS.
13. Ahamad, M. et al. (1995). *Causal Memory: Definitions, Implementations, and Programming*. Distributed Computing.
14. Kulkarni, N. et al. (2014). *Calvin: Fast Distributed Transactions for Partitioned Database Systems*. SIGMOD.

---

*Last updated: 2026-06-11*  
*Part of the Architect Skill reference collection — Core Concepts*
