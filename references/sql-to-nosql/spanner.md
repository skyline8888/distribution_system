# Spanner

## 基本信息
- **发布时间：** 2012 年（OSDI 2012 论文发表），2017 年通过 Google Cloud 对外商用（Cloud Spanner）
- **开发方：** Google
- **开源/商用：** 商用（Google Cloud Spanner）；核心设计思想启发了多个开源项目（CockroachDB、YugabyteDB、TiDB 等）
- **核心论文：**
  - *Spanner: Google's Globally-Distributed Database*, OSDI 2012 — Jeffrey C. Corbett, Jeffrey Dean, Michael Epstein 等
  - *F1: The Fault-Tolerant Distributed RDBMS Supporting Google's Ad Business*, SIGMOD 2013 — Shilling et al.
  - *TrueTime API*, OSDI 2012 — 同步全球时钟的关键原语

## 架构设计

Spanner 是 Google 构建的**全球分布式、强一致、外部一致的关系型数据库**，是 NewSQL 流派的里程碑式系统。它在 NoSQL 的水平扩展能力之上，重新引入了完整的 ACID 事务和 SQL 查询能力。

### 整体分层架构

```
┌─────────────────────────────────────────────────┐
│                  SQL API 层                       │
│            (分布式查询优化与执行)                  │
├─────────────────────────────────────────────────┤
│                 事务层 (Txn)                      │
│     2PL 锁 + MVCC + 外部一致性时间戳              │
├─────────────────────────────────────────────────┤
│              分布式共识层 (Paxos)                 │
│          每个分片的副本组独立运行 Paxos           │
├─────────────────────────────────────────────────┤
│              存储层 (Pilsner / BigTable)          │
│        LSM-Tree 风格的键值存储引擎               │
└─────────────────────────────────────────────────┘
```

### 节点角色

| 角色 | 职责 |
|------|------|
| **Placement Server** | 管理 Zone 级别的物理部署拓扑，决定数据放置在哪些数据中心 |
| **Zone Master** | 每个 Zone 的协调者，管理该 Zone 内分片副本的分配与迁移 |
| **Spanserver** | 数据存储与服务的实际节点，负责读写请求、Paxos 副本、日志管理 |
| **TrueTime API** | 硬件辅助的全局时钟服务，提供带不确定区间的时间戳 |

### 数据流

1. 客户端向最近 Zone 的 Spanserver 发起读写请求
2. Spanserver 通过 Paxos 副本组协商，Leader 副本协调读写
3. 写操作经过 2PL 加锁 → 获取 TrueTime 时间戳 → Paxos 复制到多数副本 → 提交
4. 读操作利用 MVCC 在指定时间戳的版本上快照读取
5. 跨分片事务由分布式 2PL + Paxos 提交协议保证原子性

## 存储引擎

Spanner 的存储引擎经历了演进：

### 第一代：基于 BigTable（论文版本）

- 底层复用 Google 的 **BigTable** 分布式键值存储
- 数据以 `(key, timestamp, value)` 三元组存储
- 利用 BigTable 的 **LSM-Tree**（SSTable + MemTable）结构
- 每个 Spanserver 管理多个 tablet（分片），每个 tablet 底层对应 BigTable 的一段 key 空间

### 第二代：Pilsner

- BigTable 并非为 Spanner 定制，存在不必要的开销
- **Pilsner** 是 Google 内部为 Spanner 专门重写的存储引擎
- 保留了 LSM-Tree 的核心思想，但针对 Spanner 的多版本语义做了深度优化
- 去除了 BigTable 中与 Spanner 无关的特性，降低延迟和存储开销

### 第三代：P2

- Google 在后续演进中推出了 **P2** 存储引擎（2021 年左右公开）
- 进一步降低写放大和存储开销
- 支持更高效的副本间同步

### 关键设计要点

- **多版本存储：** 每个 key 保留多个时间戳版本，支持 MVCC 快照读
- **SSTable 分层：** L0（内存 flush）→ L1+（分层 compact），减少读放大
- **持久化日志：** 每个写操作先写持久化日志（Paxos 日志），再更新 MemTable

## 数据模型与查询语言

### 数据模型

- Spanner 的数据模型是**关系模型**：表、行、列、主键、外键
- 支持**交错表（Interleaved Tables）**：将父表与子表的物理存储交错放置，利用 locality 优化 JOIN 性能
- 支持**二级索引**，包括全局索引和交错索引

### 查询语言

- 支持 **ANSI SQL 2011**（Cloud Spanner 支持 GoogleSQL，即标准 SQL 方言）
- 支持完整的 **DML**（SELECT、INSERT、UPDATE、DELETE、MERGE）
- 支持 **DDL**（CREATE/ALTER/DROP TABLE、INDEX 等）
- 支持 **DCL**（GRANT/REVOKE）
- 云版本额外支持：
  - 分布式查询优化器（自动选择最优执行计划）
  - 客户端库支持多种语言（Java、Go、Python、C++、Node.js 等）
  - 与 PostgreSQL 兼容的接口（PostgreSQL on Cloud Spanner）

## 分布式共识与事务协议

### TrueTime API — 全局时钟原语

TrueTime 是 Spanner 最核心的创新。它通过以下方式实现全球精确时间：

- **硬件原子钟 + GPS 接收器：** 每个数据中心配备精确时钟源
- **Timekeeper 守护进程：** 持续校准本地时钟，维护本地时间与 UTC 的偏差区间

TrueTime API 提供两个关键调用：

```
TT.now() → [earliest, latest]  // 返回带不确定区间的时间戳
```

- `TT.now().earliest`：当前时间的最早可能值
- `TT.now().latest`：当前时间的最晚可能值
- 不确定度 ε = latest - earliest，Google 生产环境典型值 ≤ 7ms

TrueTime 的核心价值：**当 T2.earliest > T1.latest 时，可以确定 T2 在 T1 之后，无需等待。** 这是实现外部一致性的关键。

### Paxos — 副本组共识

- 每个分片（tablet/数据目录）的副本组独立运行 **Paxos** 协议
- 副本组内有一个 **Leader**，由 Paxos 选举产生
- 所有写操作必须经过 Leader → Paxos 复制到多数副本（Quorum）→ 确认
- 副本数量通常为 5（可容忍 2 个节点故障）
- 数据分布在多个 Zone，确保跨数据中心容灾

### 分布式事务 — 2PL + MVCC + TrueTime

Spanner 的分布式事务协议是 **两阶段锁（2PL） + MVCC + TrueTime 时间戳** 的组合：

1. **两阶段锁（2PL）：**
   - 读操作获取共享锁（Shared Lock）
   - 写操作获取排他锁（Exclusive Lock）
   - 锁在事务提交后释放（严格 2PL）

2. **MVCC（多版本并发控制）：**
   - 每个数据项存储多个版本，每个版本带有时间戳
   - 读操作在指定时间戳的快照上执行，不会阻塞写操作
   - 写操作创建新版本，不覆盖旧版本

3. **事务提交协议：**
   - 写事务首先通过 2PL 获取所有需要的锁
   - 调用 `TT.now()` 获取提交时间戳 `[earliest, latest]`
   - 等待直到 `TT.now().earliest > commit_timestamp.latest`，确保时间戳单调递增
   - 通过 Paxos 将提交记录复制到所有涉及分片的副本组
   - 释放锁，事务完成

4. **跨分片事务（两阶段提交）：**
   - Coordinator 节点协调所有参与分片
   - 使用 Paxos 做两阶段提交（2PC）
   - 每个分片的 Paxos 组独立提交，但受统一时间戳约束

### 外部一致性（External Consistency）

Spanner 保证的**外部一致性**是其最强的语义保证：

> 如果事务 T1 在事务 T2 开始之前完成提交，那么 T1 的提交时间戳 < T2 的提交时间戳。

这意味着：**真实世界中的因果顺序，在数据库的时间戳顺序中严格保留。**

这是通过 TrueTime 的等待机制实现的：提交前等待 `commit.latest - now().earliest`，确保时间戳单调递增。

## 复制与分片策略

### 分片策略（Sharding / Data Directories）

- 数据按 **目录（Directory）** 组织，目录是 Spanner 的最小分片单位
- 每个目录包含一组行数据，通过目录的层级结构（目录树）组织
- 目录可以跨行分裂（split）和合并（merge），实现动态负载均衡
- **目录路由：** 客户端通过目录路由信息直接定位到对应的 Spanserver

### 交错表与数据局部性

- 父表和子表可以声明为交错关系
- 交错后，子表数据物理上存储在父表行的目录中
- 例如：`Customers` 和 `Orders` 交错后，某客户的订单直接存储在客户所在目录
- 极大优化了按父行查询子行的场景，减少跨分片通信

### 副本管理

- 每个目录的副本组跨多个 Zone 部署（通常 3-5 个副本）
- 副本角色：
  - **Leader：** 处理读写请求，协调 Paxos
  - **Follower：** 接收复制数据，可服务只读请求（stale read）
  - **Learner：** 新加入的副本，异步追赶，不参与投票
- 副本自动迁移：Zone Master 根据负载均衡和故障检测自动迁移副本

### Paxos 复制组

```
         Zone A          Zone B          Zone C
       ┌────────┐      ┌────────┐      ┌────────┐
       │ Leader │◄────►│Follower│◄────►│Follower│
       │  Paxos │      │  Paxos │      │  Paxos │
       └────────┘      └────────┘      └────────┘
```

- 每个分片的 Paxos 组独立运行，不影响其他分片
- Leader 故障时，Paxos 自动选举新 Leader（秒级恢复）

## 一致性级别

### 外部一致性（默认）

- Spanner 默认提供**外部一致性**，这是比线性一致性（Linearizability）更强的语义
- 不仅保证同一数据项的读写线性化，还保证跨数据项、跨事务的全局因果顺序
- 客户端感知到的时序与真实世界时序严格一致

### 快照隔离（Snapshot Isolation）

- 只读事务在某个时间戳的快照上执行
- 快照通过 MVCC 实现，不阻塞写事务
- 支持 **强读（Strong Read）** 和 **Stale Read（历史读）**：
  - 强读：等待最新数据提交完成，读最新版本
  - Stale Read：指定时间戳，读该时刻的快照，延迟更低

### 时间戳的语义

- 每个事务的时间戳由 TrueTime 生成
- 时间戳具有物理意义（UTC 时间），而不仅是逻辑计数器
- 这使得 Spanner 天然支持时间旅行查询（Time Travel Query）

## 容错与高可用

### 节点故障

- **单个 Spanserver 故障：** Paxos 组自动选举新 Leader，通常在秒级恢复
- **副本丢失：** 异步创建新副本，不影响可用性（只要 Quorum 仍满足）

### 数据中心故障

- 副本跨 Zone 部署（通常跨 3-5 个 Zone）
- 容忍最多 ⌊(N-1)/2⌋ 个 Zone 同时故障（5 副本可容忍 2 个）
-  surviving Zone 的 Paxos Leader 继续服务

### 网络分区

- Paxos 协议天然处理网络分区：只有包含多数副本的分区能继续写
- 少数分区降级为只读（或不可用），保证一致性优先（CP 系统）

### 时钟故障

- TrueTime 依赖硬件时钟（原子钟 + GPS）
- 当 GPS/原子钟失效时，ε 增大，TrueTime 退化为软件时钟
- 影响：事务提交的等待时间增长（因为需要等待更大的不确定区间），但**正确性不受影响**

### 数据恢复

- 通过 Paxos 日志恢复：新副本加入时，从 Leader 回放日志
- 支持时间点恢复（Point-in-Time Recovery）：利用 MVCC 的历史版本

## 性能特征

### 典型延迟（Google 内部数据，论文时期）

| 操作 | 延迟 |
|------|------|
| 只读事务（本地） | ~10 ms |
| 只读事务（远程） | ~50 ms |
| 读写事务（单分片） | ~100-300 ms |
| 读写事务（跨分片） | ~200-500 ms |
| Paxos 写入（同区域） | ~10 ms |
| Paxos 写入（跨区域） | ~100 ms |
| TrueTime 不确定度 ε | ≤ 7 ms |

### 吞吐

- 论文中测试：跨 3 个大陆、5 个数据中心的部署
- 单集群支持数千台机器
- 读写混合场景可达数十万 QPS
- 写入吞吐受 Paxos 复制延迟和 TrueTime 等待影响

### 影响性能的关键因素

- **TrueTime ε 大小：** ε 越大，写事务等待时间越长
- **Paxos 副本数：** 副本越多，写入延迟越高，但容错性越强
- **跨 Zone 距离：** 跨洲复制增加延迟
- **分片分裂频率：** 热点目录分裂影响短期性能
- **事务大小：** 大事务持有锁时间长，影响并发

### Cloud Spanner 实测数据（现代部署）

- 节点级别：单节点可支持约 10,000 QPS（读密集型）
- 延迟：同区域读写 p99 < 10ms，跨区域读写 p99 ~ 50-100ms
- 线性扩展：增加节点数可线性提升吞吐

## 适用场景

### 最佳实践

- **全球分布式业务：** 需要跨地域部署且保持强一致性的应用
- **金融系统：** 需要 ACID 事务和外部一致性保证的金融交易
- **广告系统：** Google 内部 F1 系统就是支撑广告业务的核心数据库
- **多租户 SaaS：** 需要强隔离和多租户支持的云平台
- **时间敏感应用：** 利用 TrueTime 实现精确的全局排序

### 局限性

- **写入延迟较高：** TrueTime 等待 + Paxos 复制，写入延迟显著高于单节点数据库
- **成本高昂：** 需要专用硬件（原子钟/GPS）或高质量 NTP 服务器
- **复杂性高：** 运维和调试难度大，排障需要深入理解分布式协议
- **不适合极端低延迟场景：** 如果需要亚毫秒级写入，Spanner 不是最佳选择
- **查询复杂度限制：** 大规模分布式 JOIN 查询性能受限，需合理设计数据模型（交错表）

### 设计权衡

Spanner 的核心权衡是：**用可接受的写入延迟换取外部一致性和全球水平扩展**。它不是为极致性能设计的，而是为"既要扩展，又要一致"的场景设计的。

## 历史影响

### NewSQL 运动的催化剂

Spanner 论文（2012）是 **NewSQL** 概念的奠基性文献之一。它证明了：

> 分布式数据库可以同时具备水平扩展能力（NoSQL 的特长）和完整的 ACID 事务 + SQL 查询（传统 RDBMS 的特长）。

这直接挑战了当时流行的 "CAP 定理意味着必须放弃一致性来换取可用性" 的观点。Spanner 通过 TrueTime 巧妙绕过了 CAP 的限制——**当物理时钟足够精确时，不需要在网络分区时做一致性/可用性的妥协**。

### 对后续开源系统的直接影响

| 系统 | 借鉴 Spanner 的核心设计 |
|------|------------------------|
| **CockroachDB** | 直接使用 TrueTime 理念（使用 HLC 混合逻辑时钟替代）、Paxos/Raft 共识、交错表 |
| **YugabyteDB** | 基于 Raft 的分布式共识、MVCC、分布式事务，架构理念与 Spanner 高度一致 |
| **TiDB** | 借鉴了分离式架构（计算/存储分离）、PD 调度层、分布式事务（Percolator 方案） |
| **CockroachDB 的 HLC** | 用混合逻辑时钟（HLC）替代 TrueTime，无需硬件支持，是 Spanner 思想的重要变体 |
| **Google F1** | 基于 Spanner 构建的分布式 RDBMS，支撑 Google 广告业务，证明了 Spanner 的生产可用性 |

### 关键技术创新的传承

1. **TrueTime → HLC（混合逻辑时钟）：** CockroachDB 等系统将 TrueTime 的思想泛化为 HLC，无需硬件时钟也能实现类似的外部一致性保证，极大地降低了部署门槛。

2. **Paxos → Raft：** Raft 是 Paxos 的简化版本，被几乎所有后续分布式数据库采用（CockroachDB、TiKV、YugabyteDB 等）。

3. **交错表（Interleaved Tables）：** 数据局部性优化的重要思路，影响了后续多版本分布式数据库的表设计。

4. **外部一致性（External Consistency）：** 重新定义了分布式数据库的一致性标准，推动了整个行业对一致性语义的深入讨论。

### 学术影响

- Spanner 论文获 OSDI 2012 最佳论文提名，是系统领域引用率最高的论文之一
- TrueTime 引发了关于"物理时钟 vs 逻辑时钟"在分布式系统中角色的大量后续研究
- 推动了分布式事务协议（如 Calvin、Epic、Percolator）的发展

### Cloud Spanner 的商业化

- 2017 年 Google Cloud Spanner 正式商用
- 2019 年推出 Spanner PG（PostgreSQL 兼容接口）
- 持续演进：添加自动分片、向量索引、图查询等新能力
- 是 Google Cloud 的旗舰数据库产品，与 AWS Aurora、Azure Cosmos DB 竞争

---

> **总结：** Spanner 是数据库历史上最重要的系统之一。它第一次在生产规模上证明了"全球分布 + 强一致 + SQL"三者可以共存。其核心创新——TrueTime 全局时钟和 Paxos 分布式共识——不仅支撑了 Google 内部的关键业务，还直接启发了 CockroachDB、YugabyteDB、TiDB 等新一代分布式数据库的设计，深刻改变了整个数据库行业的技术走向。
