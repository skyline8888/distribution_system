# FoundationDB

## 基本信息

- **发布时间：** 2013 年（FoundationDB Inc 成立，产品正式对外）；2015 年被 Apple 收购；2018 年 4 月 Apple 将其开源（Apache 2.0 许可证）
- **开发方：** FoundationDB Inc → Apple
- **开源/商用：** 开源（Apache 2.0），但核心开发团队在 Apple 内部，社区维护活跃
- **核心论文/文档：**
  - *FoundationDB Architecture* — 官方 Architecture Guide（foundationdb.org/documentation）
  - *FoundationDB Record Layer* — 开源多模型记录层（GitHub: Apple/foundationdb-recipes）
  - 无正式学术论文；技术细节全部通过官方文档和社区白皮书披露

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

2010 年代初的 NoSQL 运动带来了水平扩展能力和灵活的 schema，但付出了**放弃 ACID 事务**的代价。Dynamo/Cassandra 选择最终一致性（AP），Bigtable/HBase 提供单行原子性但缺乏跨行/跨表事务。开发者不得不在"扩展性"与"正确性"之间做艰难取舍。

FoundationDB 的核心理念是：**不应该有这样的取舍**。通过构建一个以**有序键值存储**为基石、在其之上叠加事务层的系统，FoundationDB 同时提供：

- 分布式系统的水平扩展能力（Shared-Nothing 架构）
- 严格的 ACID 事务语义（跨节点、跨分片）
- 灵活的数据模型（通过 API Layer 实现文档、图、关系等）

### 前代系统的局限性

| 前代系统 | 局限 |
|---------|------|
| Dynamo/Cassandra | AP 系统，最终一致性；无跨行事务 |
| Bigtable/HBase | 仅单行原子性；缺乏完整 ACID |
| MongoDB (2013 前) | 无多文档事务（4.0 才引入） |
| 传统 RDBMS | 垂直扩展为主，水平分片需要应用层处理 |
| Redis | 内存型，单机为主，事务能力有限 |

### 硬件/网络背景

-  commodity 硬件（多核 CPU、SSD）成熟，Shared-Nothing 架构成为主流
- 万兆以太网开始普及，网络带宽不再是瓶颈
- 开发者对"强一致性+高可用"的需求在金融、电商等领域日益增长

---

## 2. Architecture（架构设计）

### 整体分层架构

FoundationDB 的架构是其最显著的设计特征——**严格的两层分离**：

```
┌──────────────────────────────────────────────────────┐
│                   Client Application                  │
├──────────────────────────────────────────────────────┤
│              API Layer(s) — Language Bindings         │
│  ┌──────────┬──────────┬──────────┬────────────────┐ │
│  │ Document │   Graph  │Relational│    Custom      │ │
│  │  Model   │  Model   │  Model   │    Model       │ │
│  │          │          │  (SQL)   │                │ │
│  └──────────┴──────────┴──────────┴────────────────┘ │
├──────────────────────────────────────────────────────┤
│              Core — Distributed Ordered KV Store     │
│  ┌────────────────────────────────────────────────┐  │
│  │           Transaction Processing               │  │
│  │   (MVCC + 2PC + Conflict Resolution)           │  │
│  └────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────┐  │
│  │         Coordination (Paxos-based)              │  │
│  │   ┌─────────┐  ┌─────────┐  ┌─────────┐       │  │
│  │   │ CC Proc │  │ CC Proc │  │ CC Proc │       │  │
│  │   └─────────┘  └─────────┘  └─────────┘       │  │
│  └────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────┐  │
│  │        Storage Engine (Redwood B-Tree)          │  │
│  │   ┌─────────┐  ┌─────────┐  ┌─────────┐       │  │
│  │   │ SS Proc │  │ SS Proc │  │ SS Proc │       │  │
│  │   └─────────┘  └─────────┘  └─────────┘       │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

### 节点角色与进程类型

| 进程类型 | 缩写 | 职责 |
|---------|------|------|
| **Cluster Controller** | CC | 集群总协调者，负责进程分配、故障恢复决策、维护集群配置 |
| **Coordinators** | Coordinators | Paxos 共识组成员，维护数据库配置和事务日志；通常部署奇数个（3/5/9） |
| **Storage Server** | SS | 实际存储数据，响应读写请求，管理本地 B-Tree 存储 |
| **Commit Proxy** | Commit Proxy | 接收客户端事务写请求，协调 2PC 提交流程 |
| **Grv Proxy** | GRV Proxy | 处理客户端获取版本号（Get Read Version）请求 |
| **Resolver** | Resolver | 事务冲突检测——决定并发事务是否产生写冲突 |
| **Backup/DR Agent** | — | 备份与灾难恢复代理 |

### 数据模型与命名空间

FoundationDB 的核心数据模型是**有序键值存储（Ordered Key-Value Store）**：

- **Key 是字节序列**，按字典序排序
- **Key 使用 Tuple 编码**：通过 `tuple layer` 将多种类型（字符串、整数、UUID、嵌套元组等）编码为字节序列，**保持排序语义**
- 一个事务可以读写任意数量的 key-value 对
- 利用有序性，`getRange()` 操作可以高效扫描连续 key 空间——这是构建上层模型的基础

**Tuple 层示例：**
```
("user", 42)       →  bytes with sortable encoding
("user", 42, "email") → sub-key under user:42
```

Tuple 层保证：同前缀的 key 在排序后相邻，天然支持范围查询和前缀匹配。

### 元数据架构归类

**归类：分布式共识（算法计算式 + Paxos）**

FoundationDB 的元数据（集群配置、key 空间分片映射、事务日志）由 **Coordinators 组**通过 Paxos 共识协议维护。没有单独的元数据服务器——元数据本身就存储在分布式共识状态机中。

数据分片（Shard）到 Storage Server 的映射由 Cluster Controller 动态决定，并通过 Coordinator 组持久化。

### 数据架构分析

**分片方式（Sharding）**

- Key 空间被划分为多个 **Shard**（默认约 5 个 shard/GB，可动态调整）
- Shard 是 key 范围的连续区间，由 Cluster Controller 根据负载自动分裂/合并
- 每个 Shard 复制到多个 Storage Server（默认 3 副本）

**分布策略**

- **范围分片（Range Sharding）**：按 key 字典序划分区间
- 支持基于负载的 **split/merge**：shard 过大时自动分裂，负载低时合并
- Cluster Controller 根据 CPU、磁盘、网络利用率动态迁移 shard

**冗余方式**

- **副本复制（Replication）**：每个 shard 默认 3 副本
- 支持多种复制策略：`single`、`double`、`triple`、`zone-aware`（跨故障域）
- 写入时需要多数副本确认

**I/O 路径**

- 用户态网络通信（基于 Flow 网络库）
- 持久化通过 **Redwood B-Tree 存储引擎** 完成（2021 年 7.0 版本默认启用，替代了原来的 `ssd`/`memory` 引擎）
- 数据先写入持久化日志，再更新 B-Tree

**生命周期管理**

- 通过 MVCC 保留多版本数据
- 版本垃圾回收：旧版本在安全后被清理
- 内置备份代理支持连续备份和恢复到任意版本

---

## 3. Core Technical Innovations（核心技术创新）

### 一致性模型：强一致性 + 线性化（Serializable）

FoundationDB 提供**严格的事务串行化（Serializable Isolation）**：

- 所有并发事务的执行结果等价于某种串行执行顺序
- 读操作获取一个 **read version**，写操作在提交时获得一个 **commit version**
- 通过 version-stamp 机制，事务可以获取全局唯一的单调递增版本标识
- 支持 **snapshot isolation** 作为读端优化

### 共识协议：Paxos-based Coordination

FoundationDB 使用 Paxos 变体来维护集群配置状态：

- **Coordinators 组**（通常 3 或 5 个进程）运行 Paxos 协议
- Paxos 管理的状态包括：数据库配置、事务恢复日志、集群成员信息
- Cluster Controller 本身不运行 Paxos——它是被 Coordinator 组选举/指定的单一协调者
- 当 Cluster Controller 故障时，Coordinator 组通过 Paxos 选出新的 Controller

**关键设计权衡：** FoundationDB 选择了"轻量级 Paxos"——仅用于协调元数据和控制平面，数据路径（读写事务）不经过 Paxos，而是通过 **2PC + Resolver 冲突检测** 实现。这避免了 Paxos 在数据路径上的性能瓶颈。

### MVCC（多版本并发控制）

FoundationDB 的核心并发控制机制：

```
时间线:  version v1 ────→ v2 ────→ v3 ────→ v4

事务 T1: 获取 read_version = v2
         在 v2 的快照上读取数据（不会看到 v3, v4 的写入）

事务 T2: 获取 read_version = v2
         修改数据，提交 → 获得 commit_version = v5

事务 T3: 与 T2 冲突（写-写冲突同一 key）
         Resolver 检测到冲突 → T3 提交失败，需要重试
```

- 每个 Storage Server 维护 **多版本 B-Tree**
- 读操作在指定的 read version 快照上执行——不会阻塞写操作
- 写操作追加新版本，不覆盖旧版本
- 旧版本由垃圾回收异步清理

### 2PC（两阶段提交）分布式事务

跨 Storage Server 的事务通过 **两阶段提交** 实现：

```
Phase 1: Prepare（准备阶段）
  ┌─────────┐     ┌──────────────┐     ┌──────────┐     ┌───────────┐
  │ Client  │────→│ Commit Proxy │────→│ Resolver  │────→│ 冲突检测   │
  │ (写请求) │     │  协调提交     │     │ (Resolver)│     │             │
  └─────────┘     └──────────────┘     └──────────┘     └───────────┘

Phase 2: Commit（提交阶段）
  ┌──────────────┐     ┌──────────────────────────────────┐
  │ Commit Proxy │────→│ Storage Servers (写持久化日志)    │
  │  广播提交     │     │ 确认写入 → 返回成功               │
  └──────────────┘     └──────────────────────────────────┘
```

1. **客户端**将事务的读写集发送给 Commit Proxy
2. **Commit Proxy** 分配一个预提交的 version
3. **Resolver** 检查该事务的读集是否与已提交事务的写集冲突
4. 无冲突 → Commit Proxy 向涉及的 Storage Server 广播提交
5. Storage Server 写入持久化日志并确认
6. 客户端收到提交成功

### 容错机制

- **自动故障检测**：进程间心跳机制，Cluster Controller 监控所有进程
- **自动恢复**：Storage Server 故障时，Cluster Controller 将其负责的 shard 重新分配给其他节点
- **数据恢复**：通过事务日志（存储在 Coordinators）恢复未完成的提交
- **配置变更**：通过 Paxos 原子地更新集群配置

### 性能优化技术

- **Batching**：多个小事务批量处理，减少网络往返
- **Flow 网络库**：FoundationDB 自研的基于 actor-model 的异步网络库，避免回调地狱
- **Speculative reads**：乐观读取，冲突时重试
- **Redwood B-Tree**：Copy-on-Write B-Tree，减少写入放大，提升 SSD 寿命

### 扩展方式（Scale-Out）

- **Shared-Nothing** 架构：每个 Storage Server 独立管理自己的数据
- 水平扩展通过增加 Storage Server 进程实现
- Cluster Controller 自动重新平衡 shard 分布
- 理论上限：数百台节点、PB 级数据、数百万 QPS（取决于硬件配置）

### Redwood 存储引擎（B-Tree, Copy-on-Write）

FoundationDB 7.0 引入的 **Redwood** 存储引擎是其最新的持久化层：

- **B+Tree 结构**：与 LSM-Tree 不同，B-Tree 提供稳定的读写延迟（无 compaction 引起的尾延迟尖峰）
- **Copy-on-Write**：写入时创建新的页面版本，旧版本保留供 MVCC 读取
- **Delta 压缩**：页面内的键值对增量编码，节省存储
- **SSD 优化**：针对 NVMe/SSD 的顺序写特性优化
- 替代了早期的 `ssd`（基于 SQLite 的 B-Tree）和 `memory`（内存+磁盘日志）引擎

---

## 4. Trade-offs（CAP / PACELC）

### CAP 定位：CP 系统

```
        Consistency
            ▲
            │
            │    ● FoundationDB
            │    ● Spanner
            │    ● etcd
            │
            │                ● Cassandra
            │                ● Dynamo
            │
            └────────────────────────────▶ Availability
```

- **FoundationDB 严格选择 CP**：在网络分区时，优先保证一致性，牺牲部分可用性
- 分区期间，部分节点可能无法服务（无法达成多数派确认）
- 这是由 Serializable 事务语义的内在要求决定的——分区下无法保证全局串行化

### PACELC 定位

| 场景 | 选择 | 说明 |
|------|------|------|
| **P（Partition）** | **C（Consistency）** | 分区时拒绝写入，保证一致性 |
| **EL（Else/Normal）** | **E（Efficiency/Latency）** | 正常运行时，通过 MVCC 和异步冲突检测优化延迟 |

FoundationDB 在正常运行时选择了**效率优先**：通过 MVCC 快照读避免读锁、通过 Resolver 异步冲突检测减少写路径延迟。但这种优化是有代价的——冲突事务需要重试，在写冲突密集的场景下性能会退化。

### 复杂度 vs 运维简单性

| 优势 | 挑战 |
|------|------|
| 分层设计使核心 KV 层简洁 | 分层架构增加了系统理解难度 |
| 自动分片和负载均衡 | 排障需要理解多个进程类型的交互 |
| 单一二进制部署 | 事务冲突重试需要应用层配合 |
| 内置备份/恢复 | 升级需要停机窗口（虽然很短） |

### 成本 vs 性能

- **存储成本**：3 副本 + MVCC 多版本 → 存储放大系数约 3-5x
- **网络成本**：2PC 跨节点通信 → 延迟敏感场景需要低延迟网络
- **CPU 成本**：Resolver 冲突检测、Tuple 编解码 → 适中
- **性能收益**：在低冲突场景下，可达到数十万 QPS（单集群）

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统

| 系统 | 关系 |
|------|------|
| **TiDB (FDB Layer)** | TiDB 早期版本曾探索过 FoundationDB 作为存储层，其分层理念（SQL 层 + KV 层）与 FDB 高度相似；TiKV 的设计参考了 FDB 的有序 KV + 事务模型 |
| **Apple 内部基础设施** | Apple 收购后，FDB 成为 iCloud、App Store、Siri 等核心服务的底层存储基础设施 |
| **FoundationDB Record Layer** | Apple 开源的多模型记录层，在 FDB KV 层之上实现了文档模型、图模型、关系模型，**验证了分层架构的可行性** |
| **CockroachDB / YugabyteDB** | 虽直接灵感来自 Spanner，但"分布式 KV + 事务层"的两层范式与 FDB 理念一致 |
| **Snowflake (内部)** | 据报道，Snowflake 内部使用 FDB 作为元数据存储 |

### 成为标准的设计模式

1. **分层架构范式（Layered Architecture）**：
   - 底层：有序分布式 KV 存储（提供事务原语）
   - 上层：多种 API Layer 实现不同数据模型
   - 这一模式被 TiDB、CockroachDB、YugabyteDB 等 NewSQL 系统广泛采用

2. **有序键值作为通用基石**：
   - 证明了一个简单的有序 KV 存储可以作为所有数据模型的基础
   - Range 查询能力使得 B-Tree 索引、范围扫描等 SQL 语义可以自然映射

3. **MVCC + Resolver 冲突检测**：
   - 不同于传统 2PL（两阶段锁），FDB 选择了乐观并发控制
   - 在读多写少、冲突稀少的场景下性能优异
   - 这一模式被后续多个分布式事务系统采用

4. **控制面与数据面分离**：
   - Paxos 仅用于控制面（元数据协调）
   - 数据面使用 2PC + Resolver
   - 避免了共识协议在数据路径上的性能瓶颈

### 技术谱系位置

```
Google BigTable (2006)
    │
    ├──→ HBase (2008)
    │
    └──→ FoundationDB (2013) ◄── 本文
              │
              ├──→ TiDB/TiKV (2016) ── 分层 KV + SQL 理念传承
              │
              ├──→ CockroachDB (2014) ── 间接影响（两层范式）
              │
              ├──→ Apple Cloud Infrastructure ── 内部大规模使用
              │
              └──→ Record Layer (2019) ── 多模型 API Layer 验证
```

### 当前相关性和使用场景

- **Apple 生态**：FDB 是 Apple 云服务的核心存储基础设施，支撑 iCloud、App Store 等亿级用户服务
- **开源社区**：虽然核心开发由 Apple 控制，但社区活跃，定期发布新版本
- **适用场景**：
  - 需要强一致 ACID 事务的分布式存储
  - 需要灵活数据模型（通过 Record Layer）
  - 对尾部延迟敏感（Redwood B-Tree 无 compaction 尖峰）
  - 多租户/多模型平台的基础层
- **不适用场景**：
  - 高写冲突的 OLTP（冲突重试开销大）
  - 简单的缓存场景（过度设计）
  - 对写入延迟极度敏感的场景（2PC 增加了延迟）

---

## 参考资料

- FoundationDB 官方文档：<https://apple.github.io/foundationdb/>
- FoundationDB Architecture Guide: <https://apple.github.io/foundationdb/architecture-guide.html>
- FoundationDB GitHub: <https://github.com/apple/foundationdb>
- FoundationDB Record Layer: <https://github.com/FoundationDB/fdb-record-layer>
- Apple WWDC 2021: FoundationDB 内部使用分享
- *The FoundationDB Record Layer: A Multi-Model Distributed Database*, 2019
