# MongoDB

## 基本信息
- **发布时间：** 2009 年 2 月（首个公开版本）
- **开发方：** 10gen（2013 年更名为 MongoDB Inc.）
- **开源协议：** AGPL v3 → SSPL（Server Side Public License，2018）
- **数据模型：** 文档数据库（Document Database）
- **核心论文：** 无单一学术论文；技术文档与官方博客为主要参考
- **创始人：** Dwight Merriman、Eliot Horowitz、Kevin Ryan（DoubleClick 基础设施经验）

---

## 1. Context & Motivation（背景与动机）

### 解决的问题
2000 年代中后期，Web 2.0 应用爆发式增长，关系型数据库在以下场景暴露出明显局限：
- **灵活 schema 缺失：** 产品迭代中数据模型频繁变化，ALTER TABLE 代价高昂
- **JSON/对象映射阻抗不匹配：** ORM 层复杂且低效，开发者在代码中使用对象/JSON，在数据库中却要映射到行/列
- **水平扩展困难：** 传统 RDBMS 写扩展依赖分库分表中间件，运维成本高
- **高写入吞吐场景：** 日志、事件、内容管理等写入密集场景，RDBMS 锁粒度不够精细

### 前代局限
- RDBMS 的固定 schema 与敏捷开发节奏不匹配
- 早期 ORM 框架（Hibernate、ActiveRecord）增加了复杂度而非简化
- 分片需要手动管理，缺乏内建的水平扩展能力

### 硬件/网络背景
- 廉价 x86 集群成为主流，横向扩展（scale-out）取代纵向扩展（scale-up）
- 多核 CPU 普及，需要更高并发控制粒度
- SSD 开始进入数据中心，I/O 模型面临变革

### 创始人背景
三位创始人曾在 DoubleClick 工作，亲身经历了从 MySQL 到定制存储方案的演进。他们将 DoubleClick 内部的数据库经验产品化，最初 10gen 的平台包含应用层 + 数据库，后聚焦为独立的 MongoDB 数据库。

---

## 2. Architecture（架构设计）

### 系统拓扑

```
                    ┌─────────────────────────────────────────────────┐
                    │              Application Layer                   │
                    │   (MongoDB Drivers / ODMs / MongoDB Compass)     │
                    └────────────────────┬────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────────────┐
                    │                  mongos (Router)                 │
                    │        无状态路由层，客户端直连入口               │
                    │        缓存 config server 的 chunk 映射表         │
                    └───────┬──────────────┬──────────────┬────────────┘
                            │              │              │
                  ┌─────────▼──┐  ┌───────▼──────┐  ┌────▼────────┐
                  │ Shard A    │  │   Shard B    │  │  Shard C    │
                  │ Replica Set│  │ Replica Set  │  │ Replica Set │
                  │ ┌───────┐  │  │ ┌───────┐    │  │ ┌───────┐   │
                  │ │Primary│  │  │ │Primary│    │  │ │Primary│   │
                  │ │Sec 1  │  │  │ │Sec 1  │    │  │ │Sec 1  │   │
                  │ │Sec 2  │  │  │ │Sec 2  │    │  │ │Sec 2  │   │
                  │ └───────┘  │  │ └───────┘    │  │ └───────┘   │
                  └────────────┘  └──────────────┘  └─────────────┘
                                    ▲
                                    │
                    ┌───────────────┴───────────────────────────────┐
                    │          Config Server Replica Set             │
                    │  (存储元数据：chunk 分布、shard 列表、数据库分片 │
                    │   配置；3.4 起必须为 Replica Set)               │
                    └───────────────────────────────────────────────┘
```

### Replica Set 架构（无 Sharding 时）

```
                    ┌──────────┐
                    │ Primary  │ ← 唯一接受读写
                    │ (Node 1) │
                    └────┬─────┘
                         │ oplog 异步复制
              ┌──────────┼──────────┐
              ▼          ▼          ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │Secondary │ │Secondary │ │Secondary │
        │(Node 2)  │ │(Node 3)  │ │(Node 4)  │
        │ 可读     │ │ 可读     │ │ 仲裁者   │
        └──────────┘ └──────────┘ │(arbiter) │
                                  └──────────┘
```

### 数据模型与命名空间

- **文档模型：** 数据以 BSON（Binary JSON）格式存储，每个文档是一个类似 JSON 的键值对结构
- **集合（Collection）：** 同类文档的容器，类比 RDBMS 的表，但无固定 schema
- **数据库（Database）：** 集合的容器，命名空间格式为 `database.collection`
- **索引：** 支持单字段、复合、多键（数组）、地理空间、文本、哈希索引
- **BSON 扩展类型：** ObjectId、Date、Binary、Decimal128、UUID 等，弥补 JSON 类型缺失

### 元数据架构归类：**集中式分区 → 分布式副本**

| 组件 | 模式 | 说明 |
|------|------|------|
| Config Server | 集中式分区 + 副本 | 存储 chunk 映射、shard 拓扑；3.4+ 自身为 Replica Set |
| mongos | 无状态缓存 | 启动时拉取 config，缓存 chunk 路由表 |
| 各 Shard 内部 | Replica Set | 每个 shard 独立管理自己的数据副本 |

### 数据架构分析

**分块方式（Chunking）：**
- 默认 chunk 大小 64 MB（可调）
- 基于分片键（shard key）的范围分片
- chunk 分裂：当 chunk 超过大小时，mongod 自动分裂为两个
- chunk 迁移：balancer 在后台将 chunk 从负载高的 shard 迁到负载低的 shard

**分布策略（Range Sharding）：**
- 按分片键值范围分布数据到不同 shard
- 选择分片键至关重要：单调递增键（如 timestamp）导致写入热点；哈希分片键可均匀分布
- 支持**标签感知分片**（tag-aware sharding），将特定数据范围固定到特定 shard

**冗余方式（Replica Set 复制）：**
- 主从异步复制（oplog）
- 支持 read concern 和 write concern 控制复制确认级别
- 仲裁者（arbiter）节点不存数据，仅参与选举投票

**I/O 路径：**
- WiredTiger 引擎：用户态存储引擎，通过 VFS 层与 OS 交互
- 支持 POSIX fsync、直接 I/O（配置项）
- 缓存管理：WiredTiger 内部缓存（默认 max(50% RAM - 1GB, 256MB)）

**数据生命周期：**
- TTL 索引：自动删除过期文档
- 容量规划：Capped Collection（固定大小集合，自动覆写最旧数据）
- 压缩：WiredTiger 支持 Snappy（默认）、Zlib、Zstandard 压缩

---

## 3. Core Technical Innovations（核心技术创新）

### 一致性模型

MongoDB 的一致性模型是可配置的，提供了从强一致到最终一致的频谱：

| 写入确认级别 | write concern | 语义 |
|-------------|---------------|------|
| 无确认 | `w: 0` | 发后即忘（Fire and Forget） |
| 主节点确认 | `w: 1`（默认） | 主节点落盘即返回 |
| 多数确认 | `w: "majority"` | 多数节点落盘后返回 |
| 全部确认 | `w: <total>` | 所有节点确认 |

| 读取一致性级别 | read concern | 语义 |
|---------------|-------------|------|
| local（默认） | `local` | 返回主节点最新数据（可能回滚） |
| 多数确认 | `majority` | 返回已被多数节点确认的数据 |
| 线性化 | `linearizable` | 提供线性读（需 majority 写配合） |
| 可用 | `available` | 从 secondary 读取，最低延迟 |

### 共识协议：Replica Set Election（类 Raft 选举）

- **主节点选举：** 基于优先级的投票机制，类似 Raft 的 Leader Election
- 需要多数节点（majority）投票才能选出 Primary
- 心跳机制：节点间每 2 秒发送心跳，10 秒无响应判定为不健康
- **选举超时：** 默认约 12 秒（心跳超时 + 选举流程）
- **oplog（Operations Log）：** 副本集复制的核心机制
  - 存储在 `local.oplog.rs` 集合中的 capped collection
  - 记录所有改变数据的写操作（幂等形式）
  - Secondary 通过拉取 oplog 并重放实现异步复制
  - oplog 窗口大小可配置，默认约 24 小时（取决于磁盘空间）

### 容错机制

- **自动故障转移：** Primary 故障后，Secondary 自动选举新 Primary
- **读写分离：** Secondary 可配置为可读（`slaveOk` / `secondary` read preference），分担读负载
- **回滚（Rollback）：** 当旧 Primary 重新加入时，若其包含未复制到多数的写操作，则回滚这些操作（保存到 rollback 目录）
- **Chunk 迁移的原子性：** 使用两阶段提交式迁移，确保迁移过程中数据不丢失

### 存储引擎演进：MMAPv1 → WiredTiger

这是 MongoDB 架构演进中最关键的技术决策之一。

#### MMAPv1（默认，3.0 之前）

| 特性 | 说明 |
|------|------|
| 映射方式 | 内存映射文件（mmap），数据文件直接映射到进程地址空间 |
| 锁粒度 | **集合级锁**（collection-level locking），后改为数据库级 → 文档级（3.0） |
| 并发 | 写操作串行化（单写锁），读操作可并发 |
| 压缩 | 无内置压缩（依赖文件系统压缩） |
| 崩溃恢复 | journal 日志（WAL 风格） |
| 文档存储 | 需要 padding（预分配空间），否则文档增长时需移动 |

**局限：** 集合级锁在高并发写入场景下成为严重瓶颈，无法充分利用多核 CPU。

#### WiredTiger（默认，3.2 至今）

| 特性 | 说明 |
|------|------|
| 数据结构 | **B-Tree**（B+ 树变体） |
| 并发控制 | **MVCC**（Multi-Version Concurrency Control） |
| 锁粒度 | **文档级锁**（document-level locking） |
| 缓存 | 内部 LRU 缓存管理，避免频繁 I/O |
| 压缩 | 内置 Snappy（默认）、Zlib、Zstandard |
| Checkpoint | 定期持久化检查点（默认 60 秒） |
| 崩溃恢复 | 基于 checkpoint + journal 的双重保障 |

**MVCC 工作原理：**
- 每个事务获得一个快照时间戳
- 读操作读取快照版本，不会被写操作阻塞
- 写操作创建新版本，提交时更新可见性标记
- 读操作和写操作完全解耦，大幅提升并发性能

**B-Tree 与 LSM-Tree 对比：**
- WiredTiger 使用 B-Tree，读取性能更稳定（O(log n)），写放大较低
- 相比 Cassandra/HBase 的 LSM-Tree，MongoDB 更适合读多写少或读写混合场景
- WiredTiger 也支持 LSM-Tree 作为可选存储类型，但 B-Tree 是默认和主流选择

### 聚合管道（Aggregation Pipeline）

MongoDB 的数据处理范式，将数据转换组织为阶段（stage）流水线：

```
Collection → [$match] → [$group] → [$sort] → [$project] → [$limit] → Result
             过滤         分组        排序       投影        截断
```

- 每个 stage 接收上游文档流，转换后输出到下游
- 支持 `$lookup`（类 JOIN）、`$unwind`、`$graphLookup`（图遍历）、`$facet`（多面分析）等
- 可利用索引优化 `$match` 和 `$sort` 阶段
- 4.2 起支持 `$merge` / `$out` 将结果写回集合
- 4.4 起支持 `$unionWith`（集合间合并）
- 聚合框架是 MongoDB 最强大的数据分析工具，在分析场景中部分替代了 SQL 的能力

### 扩展方式

- **垂直扩展（Scale-Up）：** 增加单机资源（CPU、内存、存储）
- **水平扩展（Scale-Out）：** 通过 Sharding 分布式部署
  - 在线添加/移除 shard
  - 自动 chunk 分裂和迁移
  - 对应用透明（通过 mongos 路由）
- **混合策略：** 先垂直扩展到单机上限，再通过 sharding 水平扩展

---

## 4. Trade-offs（CAP / PACELC）

### CAP 定位

**MongoDB 在 CAP 中是可调的——默认偏向 CP（Consistency + Partition Tolerance），但可根据 write/read concern 向 AP 偏移。**

| 场景 | 默认行为 | CAP 倾向 |
|------|---------|----------|
| 默认配置（`w: 1` + `readConcern: local`） | Primary 确认写入，Primary 读取 | **CP**（写操作需 Primary，分区时 Primary 不可用则写入失败） |
| `w: "majority"` + `readConcern: "majority"` | 多数确认 + 多数读取 | **CP**（更强的一致性保证） |
| `w: 0` + secondary read | 发后即忘 + 从节点读 | **AP**（可用性优先，可能读到过时或不一致数据） |

**分区场景行为：**
- 当网络分区发生时，只有包含多数节点的分区能选出 Primary
- 少数分区中的节点变为 Secondary（只读或不可用）
- 这是典型的 CP 行为：牺牲可用性保证一致性

### PACELC 定位

| 条件 | 选择 | 说明 |
|------|------|------|
| **P（分区时）→ C** | 一致性优先 | 少数分区无法写入，保证不会出现脑裂写入 |
| **E（正常时）→ C** | 一致性优先（默认） | 写入 Primary，读取 Primary，保证最新数据 |
| **E（正常时）→ L（可调）** | 低延迟模式 | 配置 `readPreference: secondary` + `w: 0`，牺牲一致性换取低延迟 |

**MongoDB 默认 = PA/EC**：分区时选 C，正常时也选 C（一致性）。

### 复杂度 vs 运维

| 维度 | 优势 | 代价 |
|------|------|------|
| Schema 灵活 | 快速迭代，无需迁移 | 数据一致性需应用层保证 |
| 自动 Sharding | 对应用透明 | 分片键选择至关重要且不可逆 |
| Replica Set | 自动故障转移 | 需要至少 3 节点，运维复杂度增加 |
| 聚合管道 | 强大的数据处理能力 | 复杂聚合性能不如专用分析引擎 |
| 多存储引擎 | 可插拔灵活性 | 配置和调优复杂度增加 |

### 成本 vs 性能

| 场景 | 表现 | 备注 |
|------|------|------|
| 文档 CRUD | 优秀 | B-Tree + 文档级锁，高并发读写 |
| 点查询 | 优秀 | 索引优化良好 |
| 范围查询 | 良好 | 受分片键设计影响大 |
| JOIN 操作 | 一般 | `$lookup` 性能不及 RDBMS JOIN |
| 事务 | 4.0+ 支持多文档 ACID | 跨文档事务性能低于单文档操作 |
| 分析查询 | 中等 | 聚合管道可用，但不如 OLAP 引擎 |

**关键权衡：MongoDB 在文档 CRUD 场景下性能出色，但在需要强事务一致性或复杂 JOIN 的场景下，RDBMS 仍是更好的选择。**

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统

| 系统 | 关联 |
|------|------|
| Couchbase | 文档模型 + 内存优先，竞争关系 |
| RethinkDB | 文档模型 + 实时推送 |
| FaunaDB | 文档模型 + 强一致 + Spanner 灵感 |
| AWS DocumentDB | MongoDB 兼容的云托管文档数据库 |
| Azure Cosmos DB (MongoDB API) | MongoDB 协议兼容的多模型数据库 |
| Percona Server for MongoDB | 社区增强版 |
| ArangoDB | 多模型（文档 + 图 + KV），受文档模型启发 |

### 成为标准的设计模式

1. **文档模型的主流化：** MongoDB 是将"存储与代码中使用的数据结构保持一致"这一理念推向主流的最大功臣。JSON/BSON 原生存储消除了在 ORM 中做对象-关系映射的阻抗不匹配。

2. **灵活 Schema 范式：** "schema-less"（更准确说是 dynamic schema）成为 NoSQL 数据库的标志性特征之一，影响了后续大量数据库的设计。

3. **JSON 原生 API 设计：** MongoDB 的 BSON 查询语言启发了大量 API-first 数据库的设计——查询本身就是数据结构，而非字符串拼接的 SQL。

4. **存储引擎可插拔：** WiredTiger 作为可插拔存储引擎的架构（3.0 引入多引擎支持）为数据库设计提供了新思路，后续 RocksDB 等引擎也被集成到多种数据库中。

5. **托管数据库服务标杆：** MongoDB Atlas 成为云托管数据库服务的标杆，证明了"DBaaS"可以成为独立业务线。

### 技术谱系位置

```
  JSON/对象存储理念
        │
        ├──→ CouchDB (2005, Erlang, HTTP API)
        │         │
        │         └──→ BigCouch / Cloudant (分布式 CouchDB)
        │
        └──→ MongoDB (2009, C++, BSON)
                  │
                  ├──→ 存储引擎可插bage (WiredTiger, 2014)
                  ├──→ 自动 Sharding (内建水平扩展)
                  ├──→ 聚合管道 (数据处理范式)
                  ├──→ MongoDB Atlas (DBaaS 标杆)
                  │
                  ├──→ AWS DocumentDB (协议兼容)
                  ├──→ Azure Cosmos DB MongoDB API (协议兼容)
                  └──→ 大量 ORM/ODM 生态 (Mongoose 等)

  分布式复制:
        Dynamo (oplog 灵感) ──→ MongoDB oplog (异步复制)
        Raft/Paxos 理论       ──→ Replica Set Election (类 Raft)
```

### 当前相关性和使用场景

- **内容管理系统（CMS）：** 文档模型天然适合文章、页面、媒体等异构内容
- **用户配置文件 / 目录服务：** 灵活 schema 适合不断演化的用户数据
- **实时分析 / IoT 数据收集：** 高写入吞吐 + 灵活结构适合时序事件
- **产品目录（电商）：** 不同品类商品有不同属性，文档模型比固定 schema 更自然
- **移动应用后端：** JSON 原生减少数据转换开销
- **快速原型 / 敏捷开发：** 无需预先设计 schema，迭代速度快

### 行业地位

MongoDB 是 NoSQL 运动中最成功的商业开源数据库之一：
- Stack Overflow 开发者调查中常年位列"最受欢迎的数据库"前五
- MongoDB Atlas 云收入快速增长，验证了 DBaaS 模式
- SSPL 协议变更（2018）引发开源社区争议，但推动了云厂商推出兼容服务
- 2021 年纽交所上市（MDB），标志着 NoSQL 文档数据库的商业成熟

**历史定位：如果说 MySQL 定义了开源关系数据库的时代，那么 MongoDB 定义了文档数据库的时代——证明了"存储你代码中实际使用的数据结构"这一理念在生产环境中可行且高效。**

---

## 参考资料

- MongoDB Official Documentation: https://www.mongodb.com/docs/
- WiredTiger Storage Engine: https://source.wiredtiger.com/
- MongoDB Manual: Replica Sets, Sharding, Aggregation Framework
- "MongoDB: The Definitive Guide" (Kristina Chodorow)
- MongoDB Architecture Blog: https://www.mongodb.com/blog
