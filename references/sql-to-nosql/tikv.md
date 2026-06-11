# TiKV

## 基本信息
- **发布时间：** 2016 年（PingCAP 开源，Apache 2.0）
- **开发方：** PingCAP（创始人刘奇、黄东旭）
- **开源/商用：** 开源（Apache 2.0）；云托管版本（TiDB Cloud / AWS Marketplace）
- **核心论文/技术参考：**
  - *TiKV: A Distributed Transactional Key-Value Store* — 官方文档与工程博文
  - *Raft Consensus Algorithm* (Ongaro & Ousterhout, 2014)
  - *Percolator: Large Scale Incremental Processing* (Google, 2010)
  - *RocksDB: A Fast Storage Engine* (Facebook, 2012)
- **CNCF 毕业项目：** 2020 年成为 CNCF 首个毕业的中国项目（仅次于 Kubernetes、Prometheus 等）

> **"TiDB 的存储引擎，独立的分布式 KV 数据库"** — TiKV 最初作为 TiDB 的底层行式存储层开发，但随后演化为一个独立的分布式事务型 KV 存储系统，支持 RawKV（直接 KV 访问）和 TxnKV（分布式事务）两种模式，可脱离 TiDB 独立使用。

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

2015–2016 年间，PingCAP 在构建 TiDB（分布式 SQL 数据库）时面临一个核心难题：**MySQL 生态无法原生支持水平扩展的强一致行式存储**。已有的解决方案存在明显短板：

- **MySQL 主从/分库分表**：需要手动分片、跨分片事务困难、运维复杂
- **HBase**：基于 ZooKeeper + HDFS，架构重、延迟高、不适合 OLTP 场景
- **Cassandra/DynamoDB**：最终一致性模型，不支持 ACID 分布式事务
- **Redis Cluster**：内存受限、缺乏持久化事务能力

PingCAP 需要为 TiDB 构建一个**原生分布式、强一致、低延迟、水平可扩展的行式 KV 存储**，同时希望这个存储层本身也能独立使用。

### 设计目标

1. **强一致性**：通过 Raft 共识实现线性一致读写
2. **透明水平扩展**：数据自动分片、自动调度，对应用完全透明
3. **低延迟 OLTP**：目标 P99 写延迟 < 10ms
4. **分布式事务**：支持 ACID 事务（Percolator 模型）
5. **可独立使用**：作为独立 KV 数据库，不绑定 TiDB

### 前代系统的局限

| 前代系统 | 局限 |
|----------|------|
| **HBase** | 依赖 ZooKeeper + HDFS，组件多、运维重；RegionServer 故障恢复慢；基于 HDFS 不适合低延迟 OLTP |
| **Cassandra** | 无分布式 ACID 事务；LWT（轻量事务）仅支持单行；调优复杂 |
| **etcd / ZooKeeper** | 面向元数据存储，不适合大规模数据存储；单 Raft Group 成为扩展瓶颈 |
| **FoundationDB** | 闭源（2015 年 Apple 收购后闭源，2018 年才重新开源）；社区生态受限 |

### 硬件/网络背景

- **SSD 成为主流**，本地 NVMe SSD 的随机读写性能（IOPS > 100K）使 LSM-Tree 引擎（RocksDB）性能大幅提升
- **Go + Rust 生态成熟**，TiKV 选择 Rust 实现存储引擎（性能 + 内存安全），Go 实现 PD（调度器）
- **多可用区部署**成为企业标准需求，跨区域延迟（RTT 1–5ms）要求共识协议高效
- **云原生趋势**：存算分离架构，计算层（TiDB）与存储层（TiKV）独立扩展

---

## 2. Architecture（架构设计）

### 系统拓扑：三层架构（PD 调度 + TiKV 存储 + 可选 TiDB SQL 层）

```
┌─────────────────────────────────────────────────────────────────────┐
│                         应用层                                       │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌─────────────┐  │
│  │  TiDB      │  │  RawKV     │  │  TxnKV     │  │  TiSpark    │  │
│  │  (SQL)     │  │  Client    │  │  Client    │  │  (Spark)    │  │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘  └─────┬───────┘  │
│        │               │               │               │           │
├────────┼───────────────┼───────────────┼───────────────┼───────────┤
│        │               │               │               │           │
│  ┌─────▼───────────────▼───────────────▼───────────────▼───────┐  │
│  │                   PD (Placement Driver)                       │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  Raft 集群 (3/5 副本)                                     │ │  │
│  │  │  - 元数据管理 (KV 存储)                                    │ │  │
│  │  │  - TSO (Timestamp Oracle) 全局时间戳分配                  │ │  │
│  │  │  - Region 调度 (Leader 选举、迁移、平衡)                    │ │  │
│  │  │  - 拓扑感知调度 (Label-aware scheduling)                  │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  └────────────────────────────┬────────────────────────────────┘  │
│                               │ gRPC                               │
├───────────────────────────────┼───────────────────────────────────┤
│                               ▼                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                    TiKV 存储节点                              │ │
│  │                                                             │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │ │
│  │  │  TiKV Node 1 │  │  TiKV Node 2 │  │  TiKV Node N │  ...  │ │
│  │  │              │  │              │  │              │      │ │
│  │  │  ┌────────┐  │  │  ┌────────┐  │  │  ┌────────┐  │      │ │
│  │  │  │Region A│  │  │  │Region B│  │  │  │Region K│  │      │ │
│  │  │  │Raft L  │  │  │  │Raft L  │  │  │  │Raft F  │  │      │ │
│  │  │  ├────────┤  │  │  ├────────┤  │  │  ├────────┤  │      │ │
│  │  │  │Region B│  │  │  │Region C│  │  │  │Region A│  │      │ │
│  │  │  │Raft F  │  │  │  │Raft F  │  │  │  │Raft L  │  │      │ │
│  │  │  ├────────┤  │  │  ├────────┤  │  │  ├────────┤  │      │ │
│  │  │  │Region C│  │  │  │Region A│  │  │  │Region B│  │      │ │
│  │  │  │Raft F  │  │  │  │Raft F  │  │  │  │Raft F  │  │      │ │
│  │  │  └────────┘  │  │  └────────┘  │  │  └────────┘  │      │ │
│  │  │              │  │              │  │              │      │ │
│  │  │  ┌────────────┐│  │  ┌────────────┐│  │  ┌────────────┐│ │
│  │  │  │ gRPC Server││  │  │ gRPC Server││  │  │ gRPC Server││ │
│  │  │  │ Raft Engine││  │  │ Raft Engine││  │  │ Raft Engine││ │
│  │  │  │  RocksDB   ││  │  │  RocksDB   ││  │  │  RocksDB   ││ │
│  │  │  └────────────┘│  │  └────────────┘│  │  └────────────┘│ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘      │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### 节点角色

#### PD (Placement Driver) — 集群大脑
- **元数据管理**：存储集群拓扑、Region 分布、节点状态（自身使用 etcd 存储）
- **TSO (Timestamp Oracle)**：生成全局单调递增的时间戳，用于事务排序和 MVCC
- **Region 调度**：监控 Region 分布，自动迁移以平衡负载和存储
- **拓扑感知**：支持按可用区/机架/主机标签调度，确保副本分布在不同故障域
- **自身高可用**：PD 节点使用 etcd（内置 Raft）组成 3 或 5 节点集群

#### TiKV Node — 存储节点
- **gRPC Server**：接收客户端读写请求（RawKV/TxnKV/SQL）
- **Raft Engine**：每个 Region 维护一个独立的 Raft Group（Multi-Raft）
  - Leader 处理读写，Follower 同步日志
  - 每个 Region 默认 3 副本（可配置为 5）
- **RocksDB 存储引擎**：底层 LSM-Tree 持久化存储
  - Write CF：最新写入数据
  - Default CF：用户数据（Default column family）
  - Lock CF：事务锁信息
  - Raft CF：Raft 日志和状态
- **Resolved TS**：跟踪每个 Region 的已提交事务最小时间戳，用于安全 GC

### 数据流：一次写入的完整路径

```
Client Write ──► TiKV Node (Leader)
                   │
                   ├─► 写入 Raft Log (内存)
                   │
                   ├─► Raft Append Entries ──► Follower 1 (同步)
                   │                        ──► Follower 2 (同步)
                   │
                   ├─◄─ Raft Commit (多数派确认)
                   │
                   ├─► Apply 到 RocksDB (Write Batch)
                   │     ├─ Write CF (MemTable → SSTable)
                   │     └─ Default CF (数据持久化)
                   │
                   └─◄─ 返回 Client (Ack)
```

### 关键设计选择

**Rust 实现存储引擎**：TiKV 是早期大规模使用 Rust 的系统级项目。选择 Rust 而非 Go/C++ 的核心原因：
- **内存安全**：无 GC 停顿、无内存泄漏，保障 OLTP 延迟 SLA
- **零成本抽象**：编译期保证，运行时性能接近 C++
- **并发安全**：Rust 的所有权系统天然防止数据竞争
- **生态成熟**：RocksDB 的 Rust 绑定（RustRocksDB）、gRPC-Rust 库成熟

**分离 PD 与 TiKV**：PD 专注元数据和调度，TiKV 专注数据存储，两者独立扩展。PD 轻量（etcd 元数据），TiKV 存储数据（RocksDB）。

---

## 3. Data Model & Query Language（数据模型与查询接口）

### 数据模型：有序 Key-Value

TiKV 的核心数据模型是**有序的 Key-Value 对**（Ordered KV）。Key 是字节数组，按字典序排序。有序性是实现范围扫描和高效分片的基础。

```
Key 编码示例（TiDB 模式下的 Row Key）:
  t{TableID}_r{RowID}
  
  ┌─────────┬──────────┬─────────────────┐
  │ Prefix  │ Table ID │ Row ID          │
  │ (1 byte)│ (8 bytes)│ (8 bytes)       │
  │ 't'     │ 42       │ 12345           │
  └─────────┴──────────┴─────────────────┘
  
  实际编码后的 Key（十六进制）:
  74 00 00 00 00 00 00 00 2A 5F 72 80 00 00 00 00 00 00 30 39
  
  Value: 该行的列数据（编码后）
```

### 两种使用模式

#### 模式一：RawKV（直接 KV 访问）

```
适用场景：缓存、会话存储、配置中心、队列

接口：
  put(key, value)        — 写入键值对
  get(key)               — 读取单个键
  delete(key)            — 删除键
  scan(startKey, endKey) — 范围扫描
  batch_put/delete       — 批量操作

特性：
  - 无事务语义，直接读写
  - 支持 TTL（过期时间）
  - 支持 RawKV v2 API（2023+，更高效的批量写入）
  - 延迟极低（P99 < 5ms）
```

#### 模式二：TxnKV（分布式事务）

```
适用场景：TiDB SQL 引擎、需要 ACID 事务的应用

接口：
  begin()                — 开启事务
  get(key)               — 事务内读取
  put(key, value)        — 事务内写入
  commit()               — 两阶段提交
  rollback()             — 回滚

特性：
  - Percolator 模型：乐观事务 + 悲观事务（4.0+）
  - 分布式 ACID，跨 Region 事务
  - MVCC 多版本并发控制
  - Snapshot Isolation（默认）或 Read Committed
```

### Key 编码方案

TiKV 对 Key 进行了精心编码，确保**字典序 = 业务序**，这对范围查询至关重要：

| 编码类型 | 说明 |
|----------|------|
| **表前缀** | `t{TableID}` 确保同一表的数据连续存储 |
| **行 Key** | `t{TableID}_r{RowID}` 行数据按 RowID 有序 |
| **索引 Key** | `t{TableID}_i{IndexID}_{IndexedValues}_{RowID}` 二级索引 |
| **字节序** | 大端编码（Big Endian），数值保持字典序 |

---

## 4. Distributed Consensus & Transaction Protocol（分布式共识与事务协议）

### 4.1 Raft 共识：Multi-Raft 架构

TiKV 的核心创新之一是 **Multi-Raft** 设计：

```
传统 Raft（etcd/ZooKeeper）：
  整个集群 = 1 个 Raft Group
  所有副本 = 同一份数据
  扩展瓶颈 = 单 Group 的吞吐量

TiKV Multi-Raft：
  整个集群 = N 个 Raft Groups（每个 Region 一个）
  每个 Group 独立选举、独立复制
  总吞吐量 = Σ(每个 Group 的吞吐量)
  扩展性 = 随 Region 数量线性增长
```

#### 每个 Region 的 Raft Group

```
Region [keyA, keyB) — Raft Group #42
  ┌──────────────────────────────────────┐
  │  Leader  (TiKV Node 1)                │
  │    ├─ 接收读写请求                     │
  │    ├─ 写入 Raft Log                    │
  │    ├─ AppendEntries → Followers        │
  │    └─ 多数派确认后 Commit              │
  ├──────────────────────────────────────┤
  │  Follower (TiKV Node 2)               │
  │    ├─ 接收 Leader 的 Log              │
  │    ├─ 写入本地 Raft Log                │
  │    └─ Apply 到 RocksDB                │
  ├──────────────────────────────────────┤
  │  Follower (TiKV Node 3)               │
  │    ├─ 接收 Leader 的 Log              │
  │    ├─ 写入本地 Raft Log                │
  │    └─ Apply 到 RocksDB                │
  └──────────────────────────────────────┘
```

#### Raft 日志存储

每个 TiKV 节点内部使用独立的 RocksDB 实例存储 Raft 状态：
- **Raft Log**：Append-only，顺序写入，高性能
- **Raft State**：Term、Vote、Commit Index
- **Apply State**：已应用到状态机的 Log Index

### 4.2 Percolator 分布式事务模型

TiKV 的事务模型基于 Google Percolator（2010），经过优化：

#### 两阶段提交（2PC）

```
第一阶段：Prewrite（写准备）

  Client ──► Primary Lock (主锁)
               ├─ 检查冲突（是否有其他事务的 Lock）
               ├─ 写入 Lock CF（标记该 Key 被锁定）
               └─ 记录 Primary Key 位置

  Client ──► Secondary Locks (从锁，可并行)
               ├─ 对每个要写入的 Key
               ├─ 检查冲突
               └─ 写入 Lock CF

第二阶段：Commit（提交）

  Client ──► Primary Commit
               ├─ 清除 Primary Lock
               ├─ 写入 Write CF（记录提交时间戳）
               └─ 获取 Commit 时间戳（从 PD TSO）

  Client ──► Secondary Commits (可异步)
               ├─ 清除 Secondary Locks
               └─ 写入 Write CF
```

#### 悲观事务（TiKV 4.0+）

```
传统乐观事务：冲突检测在 Commit 时才发现 → 回滚重试

悲观事务（TiDB 4.0+）：
  BEGIN
    SELECT ... FOR UPDATE  ──► 立即加锁（悲观锁）
    UPDATE ...             ──► 持有锁期间修改
    COMMIT                 ──► 释放锁

  优势：
  - 避免写写冲突到 Commit 时才发现
  - 更接近传统数据库的行为
  - 减少事务重试
```

#### MVCC（多版本并发控制）

```
每个 Key 在 RocksDB 中维护多个版本：

  Key = "user:123"

  Write CF（提交记录）:
    ┌──────────────────────────────────────────┐
    │  TS=100  │ StartTS=95  │ Short Value="v3" │  ← 最新
    │  TS=80   │ StartTS=75  │                  │
    │  TS=50   │ StartTS=45  │ Short Value="v1" │
    └──────────────────────────────────────────┘

  Default CF（大数据值）:
    ┌──────────────────────────────────────────┐
    │  TS=100  │ Value="完整数据 v3..."          │
    │  TS=50   │ Value="完整数据 v1..."          │
    └──────────────────────────────────────────┘

  Lock CF（未提交的锁）:
    ┌──────────────────────────────────────────┐
    │  Key="user:123" │ Primary=... │ StartTS=110 │
    └──────────────────────────────────────────┘

  读取时：
  - 获取 Snapshot 时间戳 TS_snapshot
  - 在 Write CF 中查找 TS ≤ TS_snapshot 的最新记录
  - 通过 Short Value 或回查 Default CF 获取实际值
  - 忽略 TS > TS_snapshot 的版本（未提交或未来的版本）
```

#### GC（垃圾回收）

```
GC 策略：
  - Safe Point = 全局最小活跃事务的 StartTS
  - 定期扫描，删除 Safe Point 之前的旧版本
  - 使用 Resolved TS 跟踪每个 Region 的最低已提交时间戳
  - GC 通过 PD 协调，确保不会删除活跃事务需要的版本

  GC 流程：
  1. PD 更新 Safe Point
  2. TiKV 逐 Region 扫描 Write CF
  3. 删除 Safe Point 之前的旧版本
  4. Compaction 回收磁盘空间
```

### 4.3 读一致性保障

```
读路径（线性一致读）：
  Client Read ──► TiKV Leader
                    │
                    ├─ Raft Read Index（确认 Leader 身份）
                    │  ├─ 向 Followers 发送 Heartbeat
                    │  └─ 确认多数派认为自己是 Leader
                    │
                    ├─ Apply 所有已 Commit 的 Log
                    │
                    ├─ 从 RocksDB 读取（基于 MVCC Snapshot）
                    │
                    └─ 返回结果

读路径（Follower 读 — 优化）：
  Client Read ──► TiKV Follower
                    │
                    ├─ 向 Leader 确认 Read Index
                    │  └─ Leader 返回当前 Commit Index
                    │
                    ├─ 等待 Apply 到该 Index
                    │
                    └─ 从本地 RocksDB 读取
                      （减少跨节点 RPC，降低读延迟）
```

---

## 5. Tradeoffs, Replication & Sharding Strategy（权衡、复制与分片策略）

### 5.1 Region 分片策略

#### Region 的自动管理

```
Region 生命周期：

  创建：
    ── 初始 Region: [−∞, +∞)
    ── 随着数据写入，Region 增长

  分裂（Split）：
    ── 触发条件：Region 大小 > 96MB（默认）或 Keys > 1,000,000
    ── 找到 Split Key（中间 Key，确保两半均衡）
    ── 原子分裂：原 Region 停止服务 → 分裂为两个 → 新 Region 开始服务
    ── 新 Region 继承原 Raft Group 配置
    ── PD 感知分裂，开始调度

  合并（Merge）：
    ── 触发条件：相邻 Region 都很小（大量删除后）
    ── PD 决策合并
    ── 合并后减少 Raft Group 数量，降低开销

  调度（Schedule）：
    ── PD 持续监控 Region 分布
    ── 触发条件：
       * 节点间存储不均衡（> 10% 差异）
       * 节点负载不均衡（QPS/CPU）
       * 节点故障（迁移副本）
       * 拓扑变化（新增/删除节点）
    ── 调度方式：
       * Region 迁移（Transfer Region）
       * Leader 迁移（Transfer Leader，更轻量）
```

#### 分裂流程详解

```
分裂前：
  Region R1: [A, Z), 大小 120MB, Raft Group [N1(L), N2(F), N3(F)]

  达到分裂阈值 → PD 选择 Split Key = "M"

  分裂后：
  Region R1: [A, M),  大小 ~60MB, Raft Group [N1(L), N2(F), N3(F)]
  Region R2: [M, Z),  大小 ~60MB, Raft Group [N1(L), N2(F), N3(F)]

  后续 PD 调度可能将 R2 迁移到其他节点：
  Region R2: [M, Z),  Raft Group [N4(L), N5(F), N6(F)]
```

### 5.2 复制策略

#### 副本配置

```
默认配置：
  - 副本数：3（可配置为 5）
  - Raft 多数派：2/3（或 3/5）
  - 写入确认：Leader + 至少 1 个 Follower 写入成功

灵活配置（Placement Rules，5.0+）：
  - 按标签定义副本分布规则
  - 例：可用区 A 放 2 个副本，可用区 B 放 1 个副本
  - 例：热点表放 5 个副本，冷数据放 3 个副本
  - 例：Learner 副本用于备份/分析（TiFlash）
```

#### Learner 副本

```
Learner 副本（Raft Learner）：
  - 接收 Raft Log 但不参与投票
  - 不阻塞 Leader 的写入（不需要 Learner 的 Ack）
  - 用途：
    * TiFlash：列式分析引擎，通过 Learner 复制
    * 备份节点：异步复制用于备份
    * 跨域复制：远端副本不参与本地共识

  TiFlash 复制流：
  TiKV (Row) ──Raft Learner──► TiFlash (Columnar)
                  │
                  ├─ 不阻塞 TiKV 写入
                  ├─ 异步同步
                  └─ TiDB 优化器自动选择 TiFlash 执行分析查询
```

### 5.3 CAP 分析

```
TiKV 选择 CP（Consistency + Partition Tolerance）：

  C — 强一致性：
    ✅ Raft 共识保证线性一致读写
    ✅ Percolator 2PC 保证跨 Region 事务原子性
    ✅ MVCC 提供 Snapshot Isolation
    ✅ TSO 提供全局事务排序

  A — 可用性（部分妥协）：
    ⚠️ 单个 Region 的 Leader 故障时，需要 Raft 重新选举（~秒级）
    ⚠️ PD 故障时（3 副本中挂 1 个仍可服务，挂 2 个则集群不可用）
    ✅ 但不同 Region 独立，一个 Region 的故障不影响其他 Region
    ✅ TiDB 层无状态，可无限水平扩展

  P — 分区容错：
    ✅ Raft 在分区期间继续服务多数派分区
    ✅ 少数派分区自动降级（无法提供写入）
    ✅ 网络恢复后自动数据同步

  总结：TiKV 在 CAP 中坚定选择 CP。
  与 Dynamo/Cassandra（AP）形成对比，
  与 Spanner/CockroachDB（CP）一致。
```

### 5.4 性能特征

| 指标 | 典型值 | 说明 |
|------|--------|------|
| **水平扩展** | 透明，在线添加节点 | PD 自动迁移 Region |
| **单集群节点数** | 数百个 TiKV 节点 | 实际部署通常 < 100 节点 |
| **写延迟 (P99)** | 3–10ms | 本地 Raft + SSD |
| **读延迟 (P99)** | 1–5ms | Leader 读；Follower 读更低 |
| **QPS (单 Region)** | ~10K | 受限于单 Raft Group |
| **集群 QPS** | 数十万 | = 单 Region QPS × Region 数量 |
| **存储容量** | PB 级 | 取决于 TiKV 节点数和磁盘 |
| **Region 大小** | 默认 96MB | 可配置 1MB–1GB |
| **副本数** | 默认 3 | 可配置 3/5 |

#### 性能瓶颈分析

```
瓶颈 1：单 Region 的 Raft Group 吞吐量
  - 一个 Region 的写操作串行化通过 Leader
  - 热点 Key（同一 Region 频繁写入）成为瓶颈
  - 解决：热点 Region 自动分裂、打散 Key 设计

瓶颈 2：PD 的 TSO 分配
  - 全局单调递增时间戳需要 PD 串行分配
  - 高并发事务场景下 TSO 成为瓶颈
  - 解决：TSO 批量分配（一次分配多个时间戳）、TSO 缓存

瓶颈 3：跨 Region 事务的 2PC 开销
  - 涉及 N 个 Region 的事务需要 N 次 Prewrite + N 次 Commit
  - 网络延迟累加
  - 解决：尽量将相关数据放在同一 Region（Local 事务）

瓶颈 4：RocksDB Compaction
  - LSM-Tree 后台 Compaction 消耗 CPU 和 IO
  - Compaction 期间写入放大（Write Amplification）
  - 解决：调整 Compaction 策略、使用 NVMe SSD
```

### 5.5 容错与高可用

```
故障场景与恢复：

1. 单个 TiKV 节点故障：
   - PD 检测到节点失联（心跳超时 ~10s）
   - 将该节点上的 Region Leader 迁移到其他副本
   - Follower 提升为 Leader（秒级）
   - PD 调度创建新副本到健康节点
   - 新副本通过 Raft Log 同步追赶

2. PD 节点故障：
   - PD 使用 etcd Raft，3 副本允许挂 1 个
   - 挂 1 个 PD：集群继续正常服务
   - 挂 2 个 PD：元数据不可访问，TiKV 继续服务已有数据（但无法调度）

3. 网络分区：
   - Raft 多数派分区继续读写
   - 少数派分区降级为只读（Follower 无法成为 Leader）
   - 网络恢复后自动数据同步

4. 磁盘故障：
   - TiKV 节点磁盘损坏 → PD 将该节点标记为 Down
   - 调度新副本到健康节点
   - RocksDB WAL + SSTable 在 SSD 故障时可能丢失数据
   - 建议：RAID / 云盘快照 + Raft 多副本保护
```

---

## 适用场景

### 最佳场景
- **TiDB 存储后端**：作为 TiDB 的底层行式存储，提供分布式 OLTP 能力
- **RawKV 独立使用**：替代 Redis Cluster / HBase 作为高可用 KV 存储
- **会话/配置存储**：需要强一致性的分布式缓存
- **HTAP 架构中的行式层**：配合 TiFlash 实现行列融合分析

### 优势
- **强一致性**：Raft 保证线性一致读写，Percolator 保证分布式 ACID
- **透明扩展**：自动分片、自动调度，运维负担小
- **Rust 实现**：内存安全、无 GC、低延迟
- **CNCF 生态**：Kubernetes 原生支持、Prometheus 监控集成
- **双模式**：RawKV（简单 KV）+ TxnKV（分布式事务），适应不同场景
- **多副本高可用**：Rack/Zone 感知部署

### 局限
- **TSO 单点**：PD 的 TSO 是全局串行化瓶颈，限制极限写入性能
- **大事务开销**：跨多 Region 事务的 2PC 开销与 Region 数成正比
- **热点 Key**：同一 Region 的热点写入受限于单 Raft Group 吞吐
- **运维复杂度**：PD + TiKV + TiDB 三层架构，组件较多
- **学习曲线**：Region 调度、Placement Rules、Compaction 调优需要经验
- **存储放大**：LSM-Tree + Raft Log + MVCC 三重放大，实际存储占用约为原始数据的 3–5 倍

---

## 历史影响

1. **CNCF 毕业项目**：2020 年成为 CNCF 毕业的存储类项目，是中国首个 CNCF 毕业项目，证明了 Rust 在大规模分布式存储系统中的可行性

2. **TiDB 生态核心**：作为 TiDB 的存储引擎，支撑了 TiDB 成为最具影响力的开源分布式数据库之一。TiDB 与 TiKV 的解耦设计影响了后续 NewSQL 数据库的架构选择

3. **Rust 数据库生态先驱**：TiKV 推动了 Rust 在数据库领域的应用，催生了 rust-rocksdb、grpc-rs 等项目，为后续的 Neon、Apache DataFusion 等 Rust 数据库项目铺路

4. **Multi-Raft 实践标杆**：将 Raft 从元数据共识（etcd）扩展到大规模数据存储，验证了 Multi-Raft 架构在 PB 级存储场景下的可行性

5. **Row-Column 融合架构**：TiKV（行式）+ TiFlash（列式，通过 Raft Learner 复制）开创了行列融合的 HTAP 模式，被后续多个数据库（Doris、StarRocks）借鉴

6. **Percolator 开源实现**：作为 Google Percolator 论文的完整开源实现，为学术界和工业界提供了可参考的分布式事务实践

7. **云原生数据库标杆**：存算分离（TiDB 计算 + TiKV 存储 + TiFlash 分析）架构成为云原生数据库的标准范式

---

## 版本演进

| 版本 | 时间 | 核心特性 |
|------|------|----------|
| **TiKV 1.0** | 2017 | 初始版本，基础 Raft + RocksDB + RawKV |
| **TiKV 2.0** | 2018 | TxnKV 事务支持、Percolator 模型、Placement Driver 优化 |
| **TiKV 3.0** | 2019 | Raft Engine 优化、性能大幅提升、CDC 支持 |
| **TiKV 4.0** | 2020 | 悲观事务、Async Commit（降低 2PC 延迟）、TiFlash 集成 |
| **TiKV 5.0** | 2021 | Placement Rules（灵活副本调度）、RawKV v2 API |
| **TiKV 6.0** | 2022 | Unified Thread Pool、自适应 Compaction |
| **TiKV 7.0** | 2023 | Resource Control（多租户资源隔离）、 disaggregated storage 探索 |
| **TiKV 8.0** | 2024 | 云原生存储引擎优化、Log-Structured Merge 改进 |

---

## 与同类系统对比

### TiKV vs CockroachDB

| 维度 | TiKV | CockroachDB |
|------|------|-------------|
| **实现语言** | Rust（存储）+ Go（PD） | Go（全栈） |
| **SQL 层** | 分离（TiDB，可选） | 内置 |
| **存储引擎** | RocksDB（LSM-Tree） | Pebble（LSM-Tree，Cockroach 自研 Fork） |
| **事务模型** | Percolator 2PC | Percolator 2PC（相似） |
| **时间戳** | TSO（PD 集中分配） | HLC（混合逻辑时钟，分布式） |
| **分片单位** | Region（96MB） | Range（64MB） |
| **共识** | Multi-Raft | Multi-Raft |
| **CAP** | CP | CP |
| **部署模式** | 三层（PD + TiKV + TiDB） | 单层（一体化） |

### TiKV vs FoundationDB

| 维度 | TiKV | FoundationDB |
|------|------|-------------|
| **开源** | Apache 2.0（始终开源） | BSL（2018 重新开源） |
| **事务模型** | Percolator 2PC | 乐观并发控制 + 串行化隔离 |
| **存储引擎** | RocksDB | Redwood（自研 B-Tree） |
| **分片** | Region + Multi-Raft | Shard + 版本化分片 |
| **事务隔离** | Snapshot Isolation | 严格串行化（Serializable） |
| **客户端 API** | RawKV / TxnKV | Layer API（灵活数据模型） |
| **CNCF 状态** | 毕业项目 | 非 CNCF |

### TiKV vs HBase

| 维度 | TiKV | HBase |
|------|------|-------|
| **架构** | Raft + RocksDB（轻量） | ZooKeeper + HDFS（重量） |
| **一致性** | 强一致（Raft） | 最终一致（WAL + 异步复制） |
| **事务** | ACID（Percolator） | 单行原子，无跨行事务 |
| **延迟** | ms 级（OLTP） | 10–100ms（OLAP 倾向） |
| **运维** | 3 组件（PD + TiKV + TiDB） | 多组件（ZK + HDFS + HMaster + HRegionServer） |
| **适用场景** | OLTP / HTAP | 大规模批量写入、离线分析 |
