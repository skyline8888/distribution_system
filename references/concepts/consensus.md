# Consensus Algorithms — 共识算法详解

## 概述

共识（Consensus）是分布式系统的核心基石：**在不可靠网络和部分节点故障的前提下，让多个节点就某个值或操作顺序达成一致**。没有共识，就没有强一致性，没有线性可读写，没有可靠复制。

本文按照 5 维分析框架，深度拆解 Paxos → Raft → ZAB → Multi-Raft 的演进脉络。

---

## 1. Context & Motivation（背景与动机）

### 1.1 要解决的根本问题

FLP 不可能定理（Fischer, Lynch, Paterson, 1985）证明：在异步网络中，哪怕只有一个进程可能崩溃，**确定性地解决共识是不可能的**。这给共识算法设定了理论天花板——所有实用算法都必须做出妥协（超时、随机化、或引入部分同步假设）。

Leslie Lamport 进一步定义了共识必须满足的性质：

| 性质 | 定义 |
|------|------|
| **Agreement（一致性）** | 所有正确节点必须就同一个值达成一致 |
| **Validity（有效性）** | 被选定的值必须是某个节点提出的值 |
| **Termination（终止性）** | 每个正确节点最终都会做出决定 |

### 1.2 前代系统的局限

在共识算法出现之前，系统多采用 **主从复制（Primary-Backup）** 或 **两阶段提交（2PC）**：

- **2PC**：阻塞协议，协调者宕机后参与者永久阻塞
- **主从复制**：主节点单点故障，脑裂场景无法安全处理
- **Gossip 协议**：最终一致，不支持强一致性决策

这些方案在容错性、可理解性或一致性上都有明显缺陷。

### 1.3 硬件/网络背景

共识算法的发展与网络硬件演进紧密相关：

- **1990s–2000s**：局域网延迟 ~1ms，但不可靠性普遍存在，需要算法层面容错
- **2010s**：广域部署成为常态（Spanner、CockroachDB），RTT 从 1ms 到 100ms+ 不等，算法需要适应跨地域延迟
- **2020s**：RDMA 网络将延迟压到微秒级，共识协议成为性能瓶颈，催生批量优化（Multi-Paxos、批处理 Raft）

---

## 2. Architecture（架构设计）

### 2.1 Paxos（Leslie Lamport, 1989–1998）

#### 角色模型

```
              Proposer              Acceptor              Learner
              (提出者)              (接受者)              (学习者)
                 │                     │                     │
   ┌─────────────┼─────────────┐       │                     │
   │             │             │       │                     │
  P1            P2            P3     A1──A2──A3──A4──A5     L1──L2
   │             │             │       │                     │
   └─────────────┴─────────────┘       └─────────────────────┘
        可以提出提案              多数派 Quorum (N/2+1)     学习最终结果
```

#### 两阶段协议

```
Phase 1: Prepare / Promise
──────────────────────────
Proposer                         Acceptor
    │  (1) Prepare(n) ──────────────►│
    │                               │ 若 n > max_promise:
    │  (2) Promise(n, accepted) ────│  更新 max_promise = n
    │◄──────────────────────────────│  返回已接受的最大值
    │                               │

Phase 2: Accept / Accepted
──────────────────────────
Proposer                         Acceptor
    │  (3) Accept(n, value) ────────►│ 若 n ≥ max_promise:
    │                               │  接受 (n, value)
    │  (4) Accepted(n, value) ──────│
    │◄──────────────────────────────│
```

**关键约束**：Quorum 交集性质——任意两个多数派至少有一个公共 Acceptor，这是安全性的核心保证。

#### Multi-Paxos（工程优化）

```
单一 Leader Paxos → Multi-Paxos 优化

Phase 1 (Prepare) ──────────────────► 只在 Leader 变更时执行
                                      Leader 稳定后跳过！

Phase 2 (Accept)  ──────────────────► 每个提案仍需执行
                                      但序号 n 由 Leader 顺序分配

实际效果: 延迟从 2 RTT → 1 RTT
```

### 2.2 Raft（Diego Ongaro & John Ousterhout, 2014）

#### 核心设计思想

Raft 将共识问题分解为三个独立的子问题：

1. **Leader Election（领导者选举）**
2. **Log Replication（日志复制）**
3. **Safety（安全性保证）**

#### Raft 节点状态机

```
┌──────────────────────────────────────────────────────────────┐
│                      Raft Node States                        │
│                                                              │
│                                                              │
│   ┌──────────┐    timeout / heartbeat     ┌──────────────┐   │
│   │          │ ──────────────────────────► │              │   │
│   │  Follower│                              │   Candidate  │   │
│   │          │ ◄────────────────────────── │              │   │
│   └────┬─────┘    (a) received vote        └──────┬───────┘   │
│        │            (b) election timeout           │           │
│        │                                           │           │
│        │              ┌────────────────────────────┘           │
│        │              │ timeout / tie                           │
│        │              ▼                                         │
│        │       ┌──────────────┐                                 │
│        │       │              │                                 │
│        └───────│    Leader    │ ◄─ new election round           │
│                │              │                                 │
│                └──────┬───────┘                                 │
│                       │                                         │
│                       │ heartbeat / AppendEntries               │
│                       ▼                                         │
│                ┌──────────────┐                                 │
│                │   Follower   │                                 │
│                └──────────────┘                                 │
│                                                              │
│  (a) 收到 Leader 的 AppendEntries 或 RequestVote 响应          │
│  (b) 选举超时（150–300ms 随机）                                │
└──────────────────────────────────────────────────────────────┘
```

#### Raft 日志复制

```
┌────────────────────────────────────────────────────────────────┐
│                    Raft Log Replication                        │
│                                                                │
│   Leader                        Followers                      │
│  ┌──────┐                    ┌──────┐  ┌──────┐  ┌──────┐     │
│  │  L   │                    │  F1  │  │  F2  │  │  F3  │     │
│  ├──────┤                    ├──────┤  ├──────┤  ├──────┤     │
│  │  1   │                    │  1   │  │  1   │  │  1   │  ← 已提交
│  │  2   │                    │  2   │  │  2   │  │  2   │  ← 已提交
│  │  3   │                    │  3   │  │  3   │  │      │     │
│  │  4   │                    │  4   │  │      │  │      │  ← 未提交
│  │  5   │                    │  5   │  │      │  │      │  ← 未提交
│  ├──────┤                    ├──────┤  ├──────┤  ├──────┤     │
│  │next=6│                    │match │  │match │  │match │     │
│  │idx=6 │                    │ = 5  │  │ = 4  │  │ = 3  │     │
│  └──────┘                    └──────┘  └──────┘  └──────┘     │
│     │                          │         │         │           │
│     └──AppendEntries───────────┼─────────┼─────────┘           │
│        (entries 3,4,5)         │         │                     │
│     ◄──Success (match=5)──────┘         │                     │
│     ◄──Success (match=4)────────────────┘                     │
│                                                                │
│  提交规则: nextIndex = matchIndex + 1                         │
│  当多数派的 matchIndex ≥ 某条目 index 时，该条目可提交           │
└────────────────────────────────────────────────────────────────┘
```

#### Raft 日志复制详细流程

```
Client Request → Log Replication → Commit
───────────────────────────────────────────

Step 1: Client 发送命令给 Leader
        Leader 将命令追加到本地日志

Step 2: Leader 并行发送 AppendEntries
        给所有 Follower（含新条目）

        Leader          F1    F2    F3    F4    F5
        ┌───┐          ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐
        │AE │─────────►│   │ │   │ │   │ │   │ │   │
        │AE │──────────────────►│   │ │   │ │   │ │   │
        │AE │─────────────────────────►│   │ │   │ │   │
        │AE │──────────────────────────────►│   │ │   │
        │AE │───────────────────────────────────►│   │
        └───┘          └───┘ └───┘ └───┘ └───┘ └───┘

Step 3: 收到多数派（≥ N/2+1）成功响应
        Leader 提交该条目，应用状态机
        返回响应给 Client

Step 4: 下次心跳通知 Follower 提交该条目
        Follower 应用状态机
```

#### Raft 关键术语

| 术语 | 含义 |
|------|------|
| **Term** | 逻辑时钟，单调递增；每次选举 +1 |
| **nextIndex** | Leader 对每个 Follower 维护的"下一条要发送的日志 index" |
| **matchIndex** | Leader 对每个 Follower 维护的"已确认复制的最高日志 index" |
| **commitIndex** | 已提交的最高日志 index |
| **lastApplied** | 已应用到状态机的最高日志 index |
| **election timeout** | Follower 等待心跳的超时时间（150–300ms 随机） |

#### 选举安全性约束

```
Raft 选举安全性:

条件: 投票者的日志至少和候选者一样新
      (last_log_term, last_log_index) 比较

规则:
  if candidate.last_log_term > voter.last_log_term → 投票
  if candidate.last_log_term == voter.last_log_term
     and candidate.last_log_index >= voter.last_log_index → 投票
  else → 拒绝

作用: 保证被选为 Leader 的节点包含所有已提交的日志
```

### 2.3 ZAB（ZooKeeper Atomic Broadcast）

ZAB 是 ZooKeeper 专门设计的原子广播协议，与 Raft 高度相似但有微妙差异。

```
ZAB 协议架构

┌─────────────────────────────────────────────────────────┐
│                    ZooKeeper Cluster                    │
│                                                         │
│   ┌─────────┐                                           │
│   │ Leader  │ ◄── 接收所有写请求                         │
│   └────┬────┘                                           │
│        │                                                │
│   ┌────┼────┬────────┬────────┐                         │
│   ▼    ▼    ▼        ▼        ▼                        │
│ ┌───┐┌───┐┌───┐   ┌───┐   ┌───┐                        │
│ │F1 ││F2 ││F3 │   │F4 │   │F5 │                        │
│ └───┘└───┘└───┘   └───┘   └───┘                        │
│                                                         │
│  两阶段提交（简化版 Paxos）                               │
│  Phase 1: PROPOSE  → 多数派 ACK                          │
│  Phase 2: COMMIT   → 所有节点应用                        │
│                                                         │
└─────────────────────────────────────────────────────────┘

ZAB 两种模式:

Recovery Mode (恢复模式)                  Broadcast Mode (广播模式)
─────────────────                        ─────────────────
Leader 选举中                              Leader 已选出
新 Leader 同步 Follower                    正常处理客户端请求
ZXID 单调递增                              两阶段广播
过半节点同步完成 → 切换                      原子有序广播
```

#### ZAB vs Raft 关键差异

| 维度 | ZAB | Raft |
|------|-----|------|
| **提出者** | ZooKeeper 核心团队 | Ongaro & Ousterhout (Stanford) |
| **提案编号** | ZXID (epoch + counter，64 位) | term + log index |
| **两阶段/三阶段** | 两阶段（类似 Basic Paxos） | 三阶段（PreVote 可选） |
| **Leader 崩溃** | 需要 Recovery 重新同步 | 新选举 + 日志回滚 |
| **读优化** | Leader 直接读（线性读需 SYNC） | 支持 ReadIndex / Lease Read |
| **设计目标** | 有序原子广播 | 通用状态机复制 |

### 2.4 Multi-Raft（多 Raft 组）

当单 Raft 组成为瓶颈时，将数据分片到多个独立 Raft 组，每组独立选举和复制。

```
Multi-Raft 架构（以 TiKV 为例）

┌──────────────────────────────────────────────────────────┐
│                        Placement Driver (PD)              │
│   ┌─────────────────────────────────────────────────────┐│
│   │  全局元数据: Region 分布、Leader 位置、调度决策       ││
│   └─────────────────────────────────────────────────────┘│
│                          │                                │
│              ┌───────────┼───────────┐                    │
│              ▼           ▼           ▼                    │
│     ┌────────────┐ ┌────────────┐ ┌────────────┐         │
│     │  Store 1   │ │  Store 2   │ │  Store 3   │         │
│     │            │ │            │ │            │         │
│     │ R1(L) R2   │ │ R1  R3(L)  │ │ R2(L) R3   │         │
│     │ R4    R5(L)│ │ R4(L) R5   │ │ R4  R6(L)  │         │
│     │ R6(L) R7   │ │ R7  R8(L)  │ │ R8  R7(L)  │         │
│     └────────────┘ └────────────┘ └────────────┘         │
│                                                          │
│  R1~R8 = 8 个独立 Raft Group                             │
│  (L) = Leader, 无标注 = Follower                          │
│  每个 Region (~96MB) 对应 1 个 Raft Group                  │
│  各 Group 独立选举、独立复制、互不影响                      │
│                                                          │
│  优势:                                                     │
│  • 并行写入: 8 个 Group 可同时处理写入                      │
│  • 故障隔离: R1 Leader 崩溃不影响 R2~R8                     │
│  • 灵活调度: PD 可迁移 Region 平衡负载                      │
└──────────────────────────────────────────────────────────┘
```

#### Multi-Raft 的跨分片事务挑战

```
跨 Raft Group 事务 (TiKV 方案: Percolator)

Client: "SET A=1, B=2"  (A 在 R1, B 在 R3)

R1 Raft Group              R3 Raft Group
─────────────              ─────────────
  Raft 复制                  Raft 复制
  ┌─────┐                    ┌─────┐
  │ A=1 │                    │ B=2 │
  └─────┘                    └─────┘
     │                          │
     └──── Percolator 两阶段提交 ─┘
           (Lock → Prewrite → Commit)

关键: Raft 保证单个 Group 内原子性
      Percolator 保证跨 Group 原子性
      两者组合 = 全局分布式 ACID
```

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 一致性模型

| 算法 | 一致性模型 | 说明 |
|------|-----------|------|
| **Paxos** | 线性一致性（Linearizability） | 每个提案顺序等价于全局全序 |
| **Raft** | 线性一致性 | 所有日志通过 Leader 串行化 |
| **ZAB** | 全序原子广播 | 所有操作按 ZXID 全序执行 |
| **Multi-Raft** | 分片内线性一致 | 跨分片需额外协议（如 Percolator、1PC+Raft） |

### 3.2 Quorum-Based Decision Making

Quorum（法定人数）是共识协议的核心机制：

```
Quorum 机制的核心数学:

条件: N 个节点，Quorum = ⌊N/2⌋ + 1（多数派）

安全保证: 任意两个 Quorum 必有交集

示例 (N=5):
  Quorum = 3
  
  Q1 = {A, B, C}
  Q2 = {C, D, E}    → 交集 = {C} ✓
  Q3 = {A, D, E}    → 与 Q1 交集 = {A} ✓
                     → 与 Q2 交集 = {D, E} ✓

容错能力: F = ⌊(N-1)/2⌋
  N=3 → F=1 (容忍 1 个故障)
  N=5 → F=2 (容忍 2 个故障)
  N=7 → F=3 (容忍 3 个故障)

延迟: 只需等待 Quorum 响应，无需等待全部
  N=5 时只需等 3 个，而非 5 个 → 降低尾部延迟
```

#### Quorum 在 Raft 中的具体应用

```
Raft 中的 Quorum 应用:

1. 选举 Quorum:
   Candidate 需获得多数派投票 → 成为 Leader
   两个 Leader 不可能在同一 Term 出现

2. 提交 Quorum:
   Leader 收到多数派 AppendEntries 成功 → 条目可提交
   已提交条目保证不会丢失

3. 读 Quorum (ReadIndex):
   Leader 确认自己仍是 Leader (心跳到多数派)
   → 返回当前 commitIndex
   → 保证读到最新已提交数据
```

### 3.3 安全性保证（Safety Properties）

```
Raft 安全性保证 (Election Safety + Log Matching + State Machine Safety)

S1. Election Safety
    ───────────────
    同一 Term 最多一个 Leader
    → 通过投票唯一性保证（每节点每 Term 只投一票）

S2. Leader Append-Only
    ──────────────────
    Leader 从不覆盖或删除自己的日志
    只追加（append-only）
    → Follower 日志通过一致性检查对齐

S3. Log Matching
    ────────────
    如果两个日志在某个 index 有相同的 term，
    那么该 index 及之前的所有日志条目完全相同
    → 通过 AppendEntries 一致性检查保证

S4. Leader Completeness
    ───────────────────
    如果某日志条目在某个 Term 已提交，
    那么该条目会出现在所有更高 Term 的 Leader 日志中
    → 通过选举限制（投票者日志不比候选者旧）保证

S5. State Machine Safety
    ────────────────────
    如果节点已应用 index 的日志条目到状态机，
    其他节点不会在 index 应用不同的条目
    → S3 + S4 共同保证
```

### 3.4 Leader 选举详细流程

```
Raft Leader Election — 详细时序图

Term 3: F1 崩溃, F2/F3/F4/F5 正常

F2 (超时)               F3                  F4                  F5
  │                      │                   │                   │
  │  election timeout    │                   │                   │
  │  (random 150-300ms)  │                   │                   │
  │  term=4              │                   │                   │
  │  变为 Candidate      │                   │                   │
  │                      │                   │                   │
  │  RequestVote(t=4) ──►│                   │                   │
  │  RequestVote(t=4) ──────────────────────►│                   │
  │  RequestVote(t=4) ──────────────────────────────────────────►│
  │                      │                   │                   │
  │  ◄─ VoteGranted ────│ (投票给 F2)        │                   │
  │  ◄─ VoteGranted ────────────────────────│ (投票给 F2)        │
  │                      │                   │                   │
  │  获得 3 票 (含自己)   │                   │                   │
  │  → 成为 Leader       │                   │                   │
  │                      │                   │                   │
  │  AppendEntries(t=4) ─►                   │                   │
  │  AppendEntries(t=4) ─────────────────────►                   │
  │  AppendEntries(t=4) ────────────────────────────────────────►│
  │                      │                   │                   │
  │  ◄─ Success ────────│                    │                   │
  │  ◄─ Success ─────────────────────────────│                   │
  │  ◄─ Success ────────────────────────────────────────────────│
  │                      │                   │                   │
  ▼                      ▼                   ▼                   ▼
  Leader                Follower            Follower            Follower


分票场景 (Split Vote):

F2 (超时)               F3 (同时超时)
  │                      │
  │  term=5              │  term=5
  │  Candidate           │  Candidate
  │                      │
  │  RequestVote(t=5) ──►│
  │  ◄─ VoteDenied ────│ (F3 已投自己)
  │                      │
  │  RequestVote(t=5) ──►│  (F2 投 F2)
  │  RequestVote(t=5) ──────────────► F4  (F4 投 F3, 因为先到)
  │                      │
  │                      │  RequestVote(t=5) ──────────────► F5
  │  ◄─ VoteDenied ───────────────── F5 (F5 已投 F3)
  │                      │
  │  获得 1 票 (自己)     │  获得 2 票 (自己 + F5)
  │  → 未过半           │  → 未过半 (N=5, 需 3)
  │                      │
  │  再次超时...          │  再次超时...
  │  term=6              │  term=6
  │  重新选举            │  重新选举
  ▼                      ▼

  随机超时解决活锁: F2 超时 200ms, F3 超时 280ms
  → F2 先发 RequestVote, 大概率成为 Leader
```

### 3.5 容错机制

```
共识协议容错矩阵

                      Paxos       Raft        ZAB
                      ─────────   ─────────   ─────────
Crash Fault Tolerance  F = ⌊(N-1)/2⌋           同左
Byzantine Fault        ✗         ✗             ✗
                        (需 PBFT)  (需 Raft-BFT)
Leader Fail            重新选举   新 Term 选举  Recovery 模式
Network Partition      Quorum    Quorum 内     Quorum 内
                       外只读    外降级        外降级
Log Safety             ✓         ✓             ✓
Log Compaction         Snapshot  Snapshot      快照
Membership Change      Joint     动态成员      动态成员
                       Consensus  变更

脑裂容错 (Split Brain):

场景: 5 节点集群, 网络分裂为 {A,B} 和 {C,D,E}

{A, B} 分区 (2 节点)              {C, D, E} 分区 (3 节点)
─────────────────                  ─────────────────
无法形成 Quorum (需 3)             形成 Quorum (有 3)
不能选 Leader                      可以选 Leader
不能提交新日志                     正常提交日志
服务降级 (只读/不可用)              正常服务

关键: 没有 Quorum 的分区自动退化
      避免了"两个 Leader 同时存在"的脑裂问题
```

---

## 4. Trade-offs (CAP / PACELC)

### 4.1 CAP 定位

```
CAP 三角中的共识协议

           Consistency (C)
                 ▲
                / \
               /   \
              /     \
             /       \
            /    ●    \     共识协议 (Paxos/Raft/ZAB)
           /           \    位于 C-P 边 — 牺牲可用性换取一致性
          /             \
         /               \
        /                 \
       ◄──────────────────►
  Availability (A)     Partition (P)

共识协议的选择: CP 系统
  • 分区时: 保证 Consistency, 牺牲 Availability
  • 只有 Quorum 内节点可服务
  • Quorum 外节点降级或不可用

典型 CP 系统:
  • etcd (Raft)     — 配置管理、服务发现
  • TiKV (Multi-Raft) — 分布式 KV 存储
  • ZooKeeper (ZAB) — 协调服务
  • CockroachDB (Raft) — 分布式 SQL
  • OceanBase (Paxos) — 金融级数据库
```

### 4.2 PACELC 定位

```
PACELC 扩展: 分区时 + 正常时的权衡

共识协议在 PACELC 中的位置:

              ┌──────────────────────────────────────┐
              │  PACELC 分析                         │
              ├──────────────────────────────────────┤
              │                                      │
              │  P (Partition 时): 选择 C             │
              │     → 无 Quorum 的节点拒绝写           │
              │     → 保证线性一致性                   │
              │                                      │
              │  E (Else 正常时): 选择 L 或 E?         │
              │     → Paxos: 倾向 L (延迟敏感)         │
              │     → Raft: 倾向 L (Leader 串行化)     │
              │     → Multi-Raft: L+E 混合             │
              │       (并行化降低延迟, 保持吞吐)        │
              │                                      │
              │  结论: CP + L (分区时一致, 正常时低延迟) │
              │                                      │
              └──────────────────────────────────────┘

各协议 PACELC 定位:

             P 时       E 时         延迟       吞吐
             ─────      ─────       ─────      ─────
Paxos        C          L           2 RTT      中
Multi-Paxos  C          L           1 RTT      高
Raft         C          L           1 RTT      中高
Multi-Raft   C          L+E         1 RTT      极高 (并行)
ZAB          C          L           2 RTT      高
```

### 4.3 复杂度 vs 运维简洁性

```
共识协议可理解性排名 (从易到难):

Multi-Raft  ████████████████████████████  最复杂 (多组协调 + 调度)
    │
Paxos       ████████████████████              经典但难懂 (两阶段 + 数学证明)
    │
ZAB         ████████████████                  中等 (类 Raft 但有两阶段)
    │
Raft        ████████                          最易理解 (分解为三子问题)

Raft 论文明确声明设计目标:
"Raft 的目标是提供一种更易于理解的替代方案"
→ 将共识分解为: Leader 选举 + 日志复制 + 安全性
→ 每个子问题可独立理解和验证
→ 工程实现复杂度显著低于 Paxos
```

### 4.4 成本 vs 性能

```
节点数量对性能和成本的影响

N=3 (最少配置)          N=5 (推荐生产)        N=7 (高可用)
───────────────         ──────────────         ──────────────
容忍故障: 1             容忍故障: 2            容忍故障: 3
Quorum: 2               Quorum: 3              Quorum: 4
写入延迟: 低            写入延迟: 中            写入延迟: 较高
成本: 低                成本: 中                成本: 高

适用场景:
N=3: 开发/测试/小规模
N=5: 生产环境标配 (etcd, TiKV 默认)
N=7: 金融级高可用 (OceanBase 部分场景)

性能公式:
  写延迟 ≈ RTT × 1 (Raft 单 RTT 提交)
  写吞吐 ≈ (N - F) × 单节点吞吐 / 批处理因子
  
  批处理优化:
  批量 n 条 → 1 次 AppendEntries → 吞吐提升 n 倍
  (但延迟增加，需权衡)
```

---

## 5. Influence & Legacy（影响与遗产）

### 5.1 技术谱系

```
共识算法技术谱系

1989                          1998                2001
Lamport 提出 Paxos ─────────► Lamport 发表        Chubby 使用 Paxos
(当时被拒绝)                  "Paxos Made Simple"  (Google 内部协调服务)
                              │
                              │ 理论完善
                              ▼
2014                          2008                2007
Raft 论文发表 ───────────────► ZAB 设计           ZooKeeper 开源
(Ongaro & Ousterhout)        (ZooKeeper 团队)     (基于 ZAB)
                              │
                              ▼
2016                          2015                2014
TiKV Multi-Raft ────────────► etcd Raft ───────► CockroachDB Raft
(PingCAP)                    (CoreOS/Now CNCF)  (Cockroach Labs)
                              │
                              ▼
2016                          2010
TiDB (上层 SQL) ────────────► OceanBase Paxos
(使用 TiKV Multi-Raft)       (蚂蚁集团, 内部 Paxos 变体)


影响传播图:

Paxos (理论)
  ├── Google Chubby (工程验证)
  │     ├── etcd (Raft, 借鉴 Chubby 经验)
  │     ├── Spanner (Paxos, 全球分布式)
  │     │     └── CockroachDB (Raft, 开源 Spanner)
  │     └── ZooKeeper (ZAB, 类 Paxos)
  │           └── Kafka (早期 ZK, 后 KRaft)
  │
  └── Raft (可理解性优先)
        ├── etcd (K8s 标配)
        ├── TiKV (Multi-Raft)
        │     └── TiDB (NewSQL)
        ├── CockroachDB
        ├── HashiCorp Consul
        ├── Vitess
        └── Dragonboat / braft / SOFAJRaft
```

### 5.2 成为行业标准的设计模式

| 设计模式 | 来源 | 成为标准的系统 |
|---------|------|--------------|
| Leader-based 串行化 | Raft | etcd, TiKV, CockroachDB, Consul |
| Quorum 读写 | Paxos | DynamoDB, Cassandra, Cosmos DB |
| 日志复制 | Raft | 几乎所有现代共识系统 |
| 随机选举超时 | Raft | 解决分票的标准方案 |
| 预投票 (PreVote) | Raft 扩展 | 防止网络抖动触发不必要选举 |
| 快照压缩 | Raft | etcd, TiKV 日志压缩标准 |
| 联合一致性 (Joint Consensus) | Raft/Paxos | 动态成员变更标准方案 |
| Multi-Raft | TiKV | 分片数据库标准方案 |

### 5.3 工业界采用现状

```
共识协议在主流系统中的采用

系统              共识协议      状态
────────────────  ────────────  ─────────────────────────
etcd              Raft          K8s 标配, 广泛使用
TiKV              Multi-Raft    TiDB 存储层, 大规模生产
CockroachDB       Raft          分布式 SQL, 全球部署
ZooKeeper         ZAB           经典协调服务, 存量巨大
Kafka (新)        KRaft         3.x 起去除 ZK 依赖
OceanBase         Paxos         金融级, TPC-C 冠军
Consul            Raft          服务发现
Vitess            Raft          MySQL 分片编排
SOFAJRaft         Raft          蚂蚁开源 Raft 实现
Dragonboat        Raft          Go 高性能 Raft 库
braft             Raft          C++ Raft, 百度开源
Nebula Graph      Raft          图数据库存储层
```

### 5.4 当前相关性与未来趋势

```
共识算法的当前状态与演进方向

已解决:
  ✓ Leader 选举 (Raft 随机超时基本解决分票)
  ✓ 日志复制 (AppendEntries 高效可靠)
  ✓ 成员变更 (Joint Consensus / 单节点变更)
  ✓ 日志压缩 (Snapshot)

仍在演进:
  ⟳ 跨地域复制 (降低 RTT: Raft-Learn, Parallel Raft)
  ⟳ 读优化 (Lease Read vs ReadIndex 权衡)
  ⟳ 共识与存储解耦 (共识层 + 存储层分离架构)
  ⟳ 异步共识 (降低延迟的变体)
  ⟳ 与 RDMA 硬件协同 (零拷贝、内核旁路)

新兴方向:
  → Fast Paxos / EPaxos (优化非冲突路径)
  → Flexible Paxos (动态 Quorum 大小)
  → VR2 / Cheap Paxos (减少通信开销)
  → 共识协议 + 可信执行环境 (TEE) 增强安全性
```

---

## 附录: 关键论文与参考

| 论文 | 作者 | 年份 | 贡献 |
|------|------|------|------|
| The Part-Time Parliament | Leslie Lamport | 1998 | Paxos 原始论文 |
| Paxos Made Simple | Leslie Lamport | 2001 | Paxos 简化解释 |
| In Search of an Understandable Consensus Algorithm | Ongaro & Ousterhout | 2014 | Raft 原始论文 |
| A Simple Totally Ordered Broadcast Protocol | Junqueira et al. | 2008 | ZAB 论文 |
| PacificA: Replication in Log-Structured Databases | Microsoft | 2008 | 类似 Raft 的协议 |
| Multi-Raft in TiKV | PingCAP | 2016+ | Multi-Raft 工程实践 |

---

## 总结

共识算法从 Paxos 的数学严谨性，到 Raft 的工程可理解性，再到 Multi-Raft 的横向扩展能力，体现了分布式系统设计中**正确性 → 可用性 → 可扩展性**的演进主线。

核心洞见:

1. **Quorum 是安全的基石** — 多数派交集保证了不可能同时出现两个冲突的决策
2. **Leader 简化了复杂性** — 通过串行化消除了并发冲突，代价是单点瓶颈
3. **Raft 的胜利是可理解性的胜利** — Paxos 在理论上完备，但 Raft 在工程上胜出
4. **Multi-Raft 解决了扩展性问题** — 将共识从"全局瓶颈"变为"可水平扩展的原语"
5. **共识是 CP 系统的引擎** — 选择共识 = 选择一致性优先，这是架构设计的根本取舍
