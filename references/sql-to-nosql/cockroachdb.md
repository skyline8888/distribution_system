# CockroachDB

## 基本信息
- **发布时间：** 2014 年立项，2015 年开源 1.0，Cockroach Labs 创立
- **开发方：** Cockroach Labs（创始人 Ben Darnell、Spencer Kimball、Peter Mattis — 均曾在 Google 参与 Bigtable、Spanner 等项目）
- **开源/商用：** 开源（BSL — Business Source License 1.1，非 AGPL）；企业版提供高级功能（备份恢复、变更数据捕获、企业级监控）
- **核心论文/技术参考：**
  - *CockroachDB Architecture* — 官方技术文档（持续更新）
  - *CockroachDB: The Resilient Geo-Distributed SQL Database* — 官方博客与工程博文系列
  - 核心思想源自 Google Spanner 论文（OSDI 2012），以开源方式重新实现

> **"Open-source Spanner"** — CockroachDB 是对 Google Spanner 架构的开源替代，保留了 Spanner 的核心分布式设计思想，但用 Raft 共识 + HLC（混合逻辑时钟）替代了 Spanner 的 Paxos + TrueTime，使系统不依赖专用硬件时钟同步设施。

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

2012 年 Google Spanner 论文发表后，业界看到了**全球分布式强一致 SQL 数据库**的可行性，但 Spanner 是 Google 内部闭源系统，普通企业无法使用。NoSQL 系统（Cassandra、DynamoDB、MongoDB）虽然提供了水平扩展能力，但在事务完整性、SQL 兼容性和强一致性方面做出了大量妥协。

CockroachDB 的核心目标：
1. **开源化 Spanner 的全部关键能力** — 分布式 SQL、强一致 ACID、水平扩展、地理分布
2. **去除对 TrueTime 硬件的依赖** — 用软件方案（HLC）替代硬件时钟
3. **兼容 PostgreSQL 协议** — 使现有 PostgreSQL 应用能够平滑迁移
4. **多活可用（Multi-Active Availability）** — 所有节点均可读写，无单点协调者

### 前代系统的局限

| 前代系统 | 局限 |
|----------|------|
| **PostgreSQL / MySQL** | 单机或主从架构，无法水平扩展写能力；跨分片事务困难 |
| **Cassandra / DynamoDB** | 最终一致性（或可调一致），不支持跨行 ACID 事务；SQL 支持弱 |
| **MongoDB** | 早期版本无分布式事务（4.0 才引入），一致性模型弱 |
| **Google Spanner** | 闭源、依赖 TrueTime 硬件、仅 Google Cloud 可用 |

### 硬件/网络背景

- **多区域云基础设施成熟**（AWS 多 Region、GCP 多 Region），地理分布应用成为刚需
- **SSD 普及**，使得每个节点的本地存储性能大幅提升，KV 引擎（RocksDB）成为可行的存储后端
- **网络延迟仍然显著**（跨洲 RTT 100–300ms），架构必须考虑延迟优化
- **Go 语言生态成熟**，CockroachDB 选择 Go 作为实现语言（Spanner 用 C++）

---

## 2. Architecture（架构设计）

### 系统拓扑：两层架构（无状态 SQL 层 + 分布式 KV 存储层）

```
┌─────────────────────────────────────────────────────────────────┐
│                        客户端 (PostgreSQL 协议)                  │
│                  psql / ORM / 应用连接池                         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ libpq / PostgreSQL wire protocol
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    无状态 SQL 层 (SQL Gateway)                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │  Parser     │  │ Optimizer   │  │ Executor    │             │
│  │  词法/语法   │  │ 成本优化     │  │ 分布式执行   │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│                                                                 │
│  ┌───────────────────────────────────────────────────┐         │
│  │  分布式查询引擎：JOIN、聚合、排序、子查询            │         │
│  │  自动将 SQL 分解为分布式 KV 操作                    │         │
│  └───────────────────────────────────────────────────┘         │
├─────────────────────────────────────────────────────────────────┤
│                    分布式 KV 事务层                              │
│  ┌───────────────────────────────────────────────────┐         │
│  │  事务管理：Percolator-style 2PC + 乐观/悲观锁       │         │
│  │  HLC（混合逻辑时钟）生成全局一致时间戳               │         │
│  │  MVCC 多版本并发控制                                │         │
│  └───────────────────────────────────────────────────┘         │
├─────────────────────────────────────────────────────────────────┤
│                    分布式共识层 (Raft)                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Range 1  │  │ Range 2  │  │ Range 3  │  │ Range N  │  ...   │
│  │ Raft组A  │  │ Raft组B  │  │ Raft组C  │  │ Raft组N  │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
│  每个 Range 独立运行 Raft，Leader 选举、日志复制                 │
├─────────────────────────────────────────────────────────────────┤
│                    存储引擎层 (RocksDB)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Node 1       │  │ Node 2       │  │ Node 3       │         │
│  │ RocksDB      │  │ RocksDB      │  │ RocksDB      │         │
│  │ LSM-Tree     │  │ LSM-Tree     │  │ LSM-Tree     │         │
│  │ 本地 SSD     │  │ 本地 SSD     │  │ 本地 SSD     │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘

  物理部署示例（多区域）：

  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
  │  Region A   │    │  Region B   │    │  Region C   │
  │  ┌─┐ ┌─┐ ┌─┐│    │┌─┐ ┌─┐ ┌─┐ │    │┌─┐ ┌─┐ ┌─┐ │
  │  │N│ │N│ │N││    ││N│ │N│ │N│ │    ││N│ │N│ │N│ │
  │  │1│ │2│ │3││    ││4│ │5│ │6│ │    ││7│ │8│ │9│ │
  │  └─┘ └─┘ └─┘│    │└─┘ └─┘ └─┘ │    │└─┘ └─┘ └─┘ │
  └─────────────┘    └─────────────┘    └─────────────┘
       ▲ Raft 副本跨 Region 分布，容忍 Region 级别故障
```

### 节点角色

CockroachDB 采用 **对等（peer-to-peer）架构**，所有节点角色相同，不存在主从之分：

| 角色/组件 | 职责 |
|-----------|------|
| **SQL Gateway** | 接收 PostgreSQL 协议连接，解析/优化/执行 SQL，无状态，可水平扩展 |
| **KV 事务层** | 处理分布式事务、HLC 时间戳、MVCC 快照隔离 |
| **Raft 副本** | 每个 Range 的副本参与 Raft 共识，负责日志复制和 Leader 选举 |
| **Store** | 管理节点上的一个 RocksDB 实例，管理该节点上所有 Range |
| **Node** | 物理进程，包含一个或多个 Store，负责网络通信和 Gossip 协议 |
| **Gossip 网络** | 节点间元数据传播（集群拓扑、Range 位置、节点健康状态） |

### 数据模型与命名空间

CockroachDB 的**逻辑数据模型**是关系型（表、行、列、索引），但**物理存储**将所有关系结构扁平化为有序键值对：

```
逻辑层:  Table "users" (id INT PRIMARY KEY, name STRING, email STRING)

物理 KV 编码:
  /Table/52/1/100                → {name: "Alice", email: "alice@..."}
  /Table/52/2/"alice@example.com" → {id: 100}    -- 二级索引反向映射

编码模式:
  /Table/<tableID>/<indexID>/<keyColumns>
```

- 每张表分配一个唯一的 Table ID
- 主键索引和二级索引都编码为有序 KV key
- 利用 KV key 的有序性，范围扫描对应索引范围扫描

### 元数据架构归类

**原生分布式元数据服务** — CockroachDB 通过 Gossip 协议 + system ranges（特殊的 Range 存储系统元数据）实现完全去中心化的元数据管理：

| 元数据内容 | 存储位置 |
|-----------|---------|
| 集群拓扑、节点列表 | Gossip 网络（每个节点持有部分信息） |
| Range 到 Store 的映射 | system 数据库的 `system.ranges` 和 `system.descriptor` |
| 表 schema 信息 | `system.descriptor`（也是一个 Range，由 Raft 保护） |
| SQL 统计信息 | `system.table_statistics` |

**不需要中心化 Master**，所有元数据本身也作为 Range 通过 Raft 复制。

### 数据架构分析

#### 分块方式：Range（范围分片）

- 数据按 key 范围切分为 **Range**，每个 Range 默认上限 **64MB**（可配置）
- 类似于 Bigtable 的 Tablet 和 TiKV 的 Region
- 当 Range 超过阈值时自动 **分裂（Split）**：一分为二，各自独立运行 Raft
- Range 也可以 **合并（Merge）**：当数据删除导致 Range 过小时

#### 分布策略：Range 级 Raft 复制 + 再平衡

- 每个 Range 默认 **3 副本**（可配置为 5 副本），通过 Raft 共识协议保证一致性
- 每个 Range **独立选举 Leader**，不同 Range 的 Leader 分布在不同节点上，实现**负载均衡**
- **再平衡器（Rebalancer）** 持续监控集群，自动迁移 Range 副本以平衡负载和存储
- 支持 **zone configs** 控制数据放置（地理分区）

#### 冗余方式：Raft 副本复制

- 每个 Range 的多个副本中，仅 **Raft Leader** 处理写请求
- 写操作通过 Raft 日志复制到多数派（quorum）后视为提交
- 支持 **Raft Learner** 副本（只读，不参与投票），用于备份和跨区域复制
- **Follower reads**：在允许稍旧数据时，可从 Follower 直接读取，减少跨区延迟

#### I/O 路径

- 用户态 Go 程序 → RocksDB（C++ 库，通过 CGO 调用）→ 本地 SSD/磁盘
- RocksDB 提供 LSM-Tree 存储引擎：MemTable → L0 → L1+ → SSTable

#### 生命周期管理

- **LSM-Tree 压缩（Compaction）**：后台线程定期合并 SSTable 层级
- **MVCC GC**：定期清理过期的旧版本数据
- **TTL 支持**：可对表或行设置生存时间，自动过期删除

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 一致性模型：外部一致的快照隔离（SI）

CockroachDB 提供 **快照隔离（Snapshot Isolation）** 级别的事务，通过 MVCC + HLC 实现：

- 每个事务在开始时获取一个 HLC 时间戳作为快照点
- 读取操作读取该快照点之前最新已提交版本
- 写操作创建新版本（新时间戳），不阻塞读操作
- 事务提交时进行冲突检测（通过锁或验证），确保可序列化或快照隔离语义
- 默认提供 **Serializable** 隔离级别（通过锁实现），也可选择 Snapshot Isolation

**与 Spanner 的对比：**
| 特性 | Spanner | CockroachDB |
|------|---------|-------------|
| 时间源 | TrueTime（硬件 GPS + 原子钟） | HLC（混合逻辑时钟，纯软件） |
| 不确定区间 | TrueTime API 返回 [earliest, latest] | HLC 通过物理时钟 + 逻辑计数器消歧 |
| 外部一致性保证 | 通过等待不确定区间实现 | 通过 HLC 单调性 + 节点间时钟跳跃检测 |

### 3.2 混合逻辑时钟（HLC）

HLC 是 CockroachDB 替代 TrueTime 的核心创新：

```
HLC 时间戳 = (物理时钟毫秒, 逻辑计数器)

算法要点：
1. 每个节点维护本地物理时钟
2. 消息传递时交换时钟信息
3. 收到消息时：local_clock = max(local_clock, received_clock) + 1
4. 逻辑计数器在物理时钟相同时递增，保证严格单调
5. 如果检测到本地时钟回退（NTP 调整等），节点暂停服务直到追上
```

**关键性质：**
- **单调递增**：同一节点上连续事件的 HLC 严格递增
- **因果一致**：如果事件 A 导致事件 B，则 HLC(A) < HLC(B)
- **与物理时钟对齐**：HLC 的物理部分接近真实时间，便于调试和运维
- **无需硬件支持**：纯软件实现，可在任何云基础设施上运行

### 3.3 共识协议：每 Range 独立 Raft

与 Spanner 使用 Paxos 不同，CockroachDB 每个 Range 独立运行 **Raft 共识协议**：

```
Range R1 (3 副本):
  Node 1: Leader (raft_term=5, commit_index=100)
  Node 4: Follower (raft_term=5, commit_index=100)
  Node 7: Follower (raft_term=5, commit_index=100)

Range R2 (3 副本):
  Node 2: Follower
  Node 5: Leader
  Node 8: Follower

→ 不同 Range 的 Leader 打散分布，避免单节点成为热点
```

**Raft 在 CockroachDB 中的关键设计：**
- **预投票（Pre-Vote）**：防止网络分区恢复后不必要的 Leader 选举
- **Raft Log 压缩（Snapshot）**：当日志过大时生成快照，避免无限增长
- **Lease Holder**：Raft Leader 额外持有读 Lease，允许 Follower reads（通过租约机制保证读一致性）
- **Quorum 读/写**：默认写需要多数派确认；读可通过 Lease Holder 或多数派进行

### 3.4 分布式事务：Percolator-style 2PC

CockroachDB 的分布式事务借鉴了 Google Percolator 的设计：

```
两阶段提交流程（简化）：

Phase 1 — Prepare (Prewrite):
  1. 客户端选择事务中的一个 key 作为 "txn record"（事务协调点）
  2. 对所有涉及的 key 加写锁（write intent）
  3. 将 write intent 写入每个 key 的 Raft 日志
  4. 记录事务元数据到 txn record

Phase 2 — Commit / Abort:
  5. 协调者检查是否所有 key 都 prepare 成功
  6. 若成功：在 txn record 写入 commit 状态
  7. 异步清理所有 write intent（变为正式写入）
  8. 若失败：在 txn record 写入 abort 状态
  9. 异步回滚所有 write intent
```

**关键机制：**
- **Write Intent**：预写锁标记，其他事务遇到时可检测冲突
- **事务记录（Txn Record）**：集中式事务元数据，提交/回滚状态的单一真相源
- **异步清理**：第二阶段完成后，write intent 的清理是异步的，不阻塞提交响应
- **死锁检测**：通过超时 + 死锁检测图（push transaction）解决
- **1PC 优化**：单 Range 事务可跳过两阶段，直接提交（减少一次 RTT）

### 3.5 性能优化技术

| 优化技术 | 说明 |
|----------|------|
| **1PC 单 Range 事务** | 事务只涉及一个 Range 时，跳过 prepare 阶段，直接写入并返回 |
| **并行查询执行** | 分布式查询引擎将 SQL 分解为多个 KV 操作，并行执行后汇聚结果 |
| **向量化执行引擎**（v21.1+） | 批量处理列数据，利用 CPU SIMD 指令，提升 OLAP 查询性能 |
| **Follower Reads** | 允许从 Raft Follower 读取稍旧数据，减少跨区延迟（bounded staleness） |
| **Cascading Zone Configs** | 数据自动放置在最近的可用区域，减少读写延迟 |
| **Locality-Optimized Search** | SQL 层优先向本地/近端节点路由请求 |
| **Range Cache** | 客户端缓存 Range 到节点的映射，减少路由开销 |
| **批量 Raft 提交** | 多个小写操作可合并到同一个 Raft 提议中 |

### 3.6 扩展方式

- **水平扩展（Scale-Out）**：添加节点后，Rebalancer 自动迁移 Range 副本，负载均衡
- **线性扩展**：读写吞吐量随节点数近似线性增长（受限于 Raft 延迟和跨区通信）
- **无状态 SQL 层**：SQL Gateway 节点可以任意增加，不承担存储，只负责计算
- **地理扩展**：支持跨洲际多 Region 部署，通过 zone configs 控制数据放置

---

## 4. Trade-offs（CAP / PACELC 权衡）

### CAP 定理定位：**CP 系统（强一致性优先）**

```
        Availability
            ▲
            │
            │         AP 系统
            │      (Cassandra, DynamoDB)
            │
            │
Consistency ┼────────────────────── Availability
   (CP)     │        ● CockroachDB
            │      (强一致, 牺牲部分可用性)
            │
            │
            │
```

- **当网络分区发生时**：CockroachDB 选择 **Consistency** — 如果多数派副本不可达，该 Range 不可写（Raft 要求多数派）
- **实际表现**：单个节点故障不影响可用性（3 副本容忍 1 故障）；整个 Region 故障时，如果该 Region 承载了某些 Range 的多数派，则这些 Range 暂时不可写
- **与 Spanner 一致**：两者都是 CP 系统，保证强一致性

### PACELC 定位

```
PACELC 框架:
  P (Partition) → 选 C (Consistency)
  E (Else, 正常运行) → 选 L (Latency) 或 E (Efficiency)

CockroachDB:
  ┌─────────────────────────────────────────────┐
  │  P → C  (分区时选强一致, 不可用部分数据)     │
  │  E → L  (正常时优先延迟, 通过多区域优化)     │
  │                                             │
  │  PACELC 定位: [P→C][E→L]                    │
  └─────────────────────────────────────────────┘
```

**详细说明：**

| 场景 | 选择 | 理由 |
|------|------|------|
| **网络分区** | **C (Consistency)** | Raft 需要多数派，分区后少数派不可写，保证不产生脑裂 |
| **正常运行** | **L (Latency)** | 多区域部署时通过 Lease Holder 就近读取、follower reads 等优化延迟 |

### 复杂度 vs 运维简易性

| 维度 | 权衡 |
|------|------|
| **架构复杂度** | 高 — 多层架构（SQL → KV → Raft → RocksDB），内部组件交互复杂 |
| **运维简易性** | 相对高 — 单二进制部署，自动再平衡，无需手动分片；但调优和故障排查需要深入理解 |
| **与 PostgreSQL 对比** | 运维更复杂（分布式系统固有的），但 API 兼容，应用迁移成本低 |
| **与 Cassandra 对比** | 运维更简单（自动 Raft 管理 vs 手动修复），但写延迟更高（Raft quorum vs 异步复制） |

### 成本 vs 性能

| 维度 | 说明 |
|------|------|
| **硬件成本** | 中等偏高 — 每个节点需要 SSD（RocksDB 性能依赖 SSD），3 副本意味着 3 倍存储开销 |
| **网络成本** | 跨区 Raft 复制增加网络流量，特别是写密集型工作负载 |
| **性能天花板** | 写延迟受限于 Raft 多数派 RTT（单区域 ~1–5ms，跨区 ~50–200ms） |
| **性价比** | 对于需要强一致 + 水平扩展 + SQL 的场景，是最佳开源选择之一 |

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统

| 系统 | 关系 |
|------|------|
| **YugabyteDB** | 同样对标 Spanner，使用 PostgreSQL 上层 + 自研分布式 KV（DocDB，Raft 共识），架构高度相似 |
| **TiDB** | 类似的两层架构（无状态 SQL + 分布式 KV TiKV），但 MySQL 兼容而非 PostgreSQL |
| **Amazon Aurora** | 虽然架构不同（存算分离），但在云原生分布式 SQL 方向上是同一趋势 |
| **Google Cloud Spanner（开源化）** | Spanner 本身在 2017 年通过 GCP 商用，与 CockroachDB 形成直接竞争 |

### 成为标准的设计模式

1. **无状态 SQL 层 + 分布式 KV 存储层** — 成为 NewSQL 的标准架构模式，被 TiDB、YugabyteDB 等广泛采用
2. **Range 级独立 Raft** — 每个分片独立共识组，Leader 打散分布，成为分布式 KV 的共识模式标准
3. **HLC 替代 TrueTime** — 证明了无硬件依赖的分布式全局时钟是可行的，降低了强一致数据库的部署门槛
4. **Percolator-style 2PC 在开源领域的应用** — 将 Google 内部的大规模分布式事务模式带到了开源世界
5. **Geo-Partitioning 作为一等公民** — 通过 zone configs 实现数据局部性控制，成为多区域数据库的标准功能

### 技术谱系位置

```
Google 内部系统谱系:

  Google Bigtable (2006)
       │
       ├──→ Apache HBase (2008)
       │
       └──→ Google Spanner (2012)
                │
                ├──→ TrueTime + Paxos + BigTable/Pilsner
                │     闭源, Google Cloud 独占
                │
                └──→ 设计思想开源化
                      │
                      ├──→ CockroachDB (2014)
                      │     HLC + Raft + RocksDB + PG 兼容
                      │
                      └──→ (间接影响) TiDB/TiKV (2015)
                            PD + Raft + RocksDB + MySQL 兼容

NewSQL 架构模式:

  FoundationDB (2013)
       │  有序 KV + ACID 层
       │
       └──→ Apple 收购, 2018 开源
              │
              └──→ 影响后续分布式 KV 设计

  Spanner 思想 ──→ CockroachDB ──→ YugabyteDB
       │               │
       │               └──→ 证明开源 Spanner 可行
       │
       └──→ TiDB/TiKV (独立实现，MySQL 兼容)
```

### 当前相关性与使用场景

| 场景 | 适合度 |
|------|--------|
| **金融级强一致分布式事务** | ⭐⭐⭐⭐⭐ 核心优势场景 |
| **多区域/多活部署** | ⭐⭐⭐⭐⭐ Geo-Partitioning + 多活架构 |
| **PostgreSQL 迁移 + 水平扩展** | ⭐⭐⭐⭐⭐ PG 协议兼容 |
| **高并发 OLTP** | ⭐⭐⭐⭐ 写延迟受 Raft 限制，但水平扩展可补偿 |
| **HTAP 混合负载** | ⭐⭐⭐ 有向量化引擎，但 OLAP 能力不如专门系统 |
| **边缘/IoT 低延迟写入** | ⭐⭐ Raft quorum 延迟不适合极低延迟场景 |

### 版本演进里程碑

| 版本 | 年份 | 关键变化 |
|------|------|----------|
| 1.0 | 2015 | 初始开源发布，基础 SQL + Raft + RocksDB |
| 2.0 | 2018 | 生产就绪，改进的查询优化器 |
| 19.1+ | 2019 | 版本号改为日历化，多区域 SQL 语法 |
| 20.1 | 2020 | Geo-Partitioning 成熟，Region-aware SQL |
| 21.1 | 2021 | 向量化执行引擎，显著提升 OLAP 性能 |
| 21.2+ | 2021 | 多区域类型、多租户支持 |
| 22.1+ | 2022 | 改进了分布式查询计划、物化视图 |
| 23.1+ | 2023 | 改进了变更数据捕获（CDC）、备份恢复 |
| 24.x | 2024 | 持续优化向量化执行、查询缓存 |

---

## 附录：与 Spanner 核心设计对比

| 维度 | Google Spanner | CockroachDB |
|------|---------------|-------------|
| **时钟同步** | TrueTime（GPS + 原子钟硬件） | HLC（纯软件，物理时钟 + 逻辑计数器） |
| **共识协议** | Paxos（每个分片） | Raft（每个 Range） |
| **存储引擎** | BigTable → Pilsner → P2（自研） | RocksDB（开源 LSM-Tree） |
| **SQL 兼容** | Google 标准 SQL | PostgreSQL 协议兼容 |
| **分布式事务** | 2PL + TrueTime 时间戳 | Percolator-style 2PC + HLC |
| **许可证** | 闭源（Google Cloud） | BSL 1.1（开源可阅读/修改，商业部署需许可） |
| **实现语言** | C++ | Go |
| **元数据管理** | Placement Server + Zone Master | Gossip + system ranges（完全去中心化） |
| **地理分区** | 支持 | 支持（zone configs） |
| **CAP** | CP | CP |
| **PACELC** | P→C, E→L | P→C, E→L |
| **多活** | 所有节点可读写 | 所有节点可读写 |
