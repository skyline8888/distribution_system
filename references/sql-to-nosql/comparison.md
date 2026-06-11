# SQL vs NoSQL vs NewSQL 数据库对比分析

> 基于 5-Dim Analysis Framework 的横向对比文档
> 覆盖: MySQL, PostgreSQL, Spanner, CockroachDB, TiDB, Cassandra, DynamoDB, HBase, MongoDB, Redis, Neo4j, FoundationDB, VoltDB

---

## 目录

1. [历史演进总览](#1-历史演进总览)
2. [对比矩阵](#2-对比矩阵)
   - 2.1 数据模型
   - 2.2 一致性模型
   - 2.3 复制策略
   - 2.4 分片/分区
   - 2.5 CAP/PACELC 定位
   - 2.6 查询语言
   - 2.7 ACID 支持
   - 2.8 可扩展性
   - 2.9 运维复杂度
3. [决策指南: 何时选择何种系统](#3-决策指南)
4. [ASCII 决策树](#4-ascii-决策树)
5. [5-Dim 系统深度分析](#5-5-dim-系统深度分析)

---

## 1. 历史演进总览

### 1.1 时代划分

```
┌─────────────────────────────────────────────────────────────────────┐
│  SQL → NoSQL → NewSQL 演进时间线                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1970s         1990s           2000s           2010s           2020s│
│  ─────         ─────           ─────           ─────           ─────│
│              ┌──────┐                                              │
│  System R ──→│MySQL │──┐                                           │
│              └──────┘  │    ┌────────┐    ┌─────────┐              │
│              ┌──────┐  │    │BigTable├───→│ HBase   │              │
│  ──────────→ │Postgre│  │    └────────┘    └─────────┘              │
│              │SQL   │  │    ┌────────┐    ┌─────────┐              │
│              └──────┘  └───→│Dynamo  ├───→│Cassandra│              │
│                              └────────┘    └─────────┘              │
│                              ┌────────┐    ┌─────────┐              │
│                              │MongoDB ├───→│ Document│              │
│                              └────────┘    │  生态    │              │
│                              ┌────────┐    └─────────┘              │
│                              │ Redis  │────────────────→            │
│                              └────────┘                             │
│                              ┌────────┐    ┌───────────┐            │
│                              │ Neo4j  │───→│   图生态   │            │
│                              └────────┘    └───────────┘            │
│                                               ┌─────────┐           │
│                               ┌────────┐     →│ Spanner ├──→        │
│                               │VoltDB  │    /  └────┬────┘          │
│                               └────────┘   /    ┌──┴───┐           │
│                               ┌────────┐  /  ┌──→│Cockroach│        │
│                               │Foundatn ├──→  │   └──────┘          │
│                               │nDB     │  \  └──→│ TiDB  │          │
│                               └────────┘   \    └──────┘           │
│                                             ╲───  NewSQL 时代       │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 核心驱动力

| 时代 | 核心驱动力 | 硬件背景 | 代表范式 |
|------|-----------|---------|---------|
| 1970-1990s | 企业信息化需求 | 单机大内存, 磁盘存储 | 关系模型, SQL |
| 1990-2000s | Web 2.0 爆发 |  commodity x86 集群 | 主从复制, 分库分表 |
| 2000-2010s | 超大规模互联网 | 分布式集群, 廉价磁盘 | NoSQL, CAP 理论 |
| 2010-2020s | 全球化+云原生 | SSD, NVMe, 跨洲网络 | NewSQL, 外部一致性 |
| 2020s-至今 | 实时+HTAP+AI | 持久内存, RDMA, 云原生 | 融合统一 |

### 1.3 技术演进主线

```
关系模型 ──→ 主从复制 ──→ 分库分表(中间件) ──→ NoSQL(去关系)
                                            │
                                            ↓
                               NewSQL(关系+分布式)
                                    │
                                    ├── Spanner: TrueTime 全局时钟
                                    ├── CockroachDB: 开源 Spanner 替代
                                    └── TiDB: Raft + Percolator 事务
```

---

## 2. 对比矩阵

### 2.1 数据模型

| 系统 | 数据模型 | 存储引擎 | 索引类型 | Schema 策略 |
|------|---------|---------|---------|------------|
| MySQL | 关系表 | InnoDB (B+Tree) | B+Tree, Full-Text, Hash | 固定 Schema |
| PostgreSQL | 关系表 + 扩展 | Heap + TOAST | B-Tree, GIN, GIST, BRIN, SP-GiST | 固定 + JSONB 半结构化 |
| Spanner | 关系表 | 分布式 B+Tree (Spanner) | 全局二级索引 | 固定 Schema |
| CockroachDB | 关系表 | RocksDB (LSM) | 全局二级索引 | 固定 Schema |
| TiDB | 关系表 (MySQL 兼容) | TiKV (RocksDB/LSM) | 全局二级索引 | 固定 Schema |
| Cassandra | 宽列 (Column Family) | LSM-Tree | 主键 + SASI/SAI 二级索引 | 灵活 Schema (CQL) |
| DynamoDB | 键值 + 文档 | SSD B-Tree | 主键 + GSI/LSI | Schema-less |
| HBase | 宽列 (Bigtable 模型) | HFile (LSM-like) | RowKey 唯一索引 | 灵活 (列族级) |
| MongoDB | 文档 (BSON) | WiredTiger (B-Tree) | 单字段, 复合, 地理, 全文 | 灵活 Schema |
| Redis | 内存数据结构 | 内存 + AOF/RDB | 无独立索引 (数据结构即索引) | 无 Schema |
| Neo4j | 原生图 | 原生图存储 | 节点/关系属性索引 | 灵活 Schema |
| FoundationDB | 有序键值 (分层) | Redwood (B-Tree) | 有序键 (范围扫描) | 应用层定义 (Layer) |
| VoltDB | 关系表 | 内存行存储 + 快照/日志 | 树索引 + 哈希索引 | 固定 Schema |

### 2.2 一致性模型

| 系统 | 默认一致性 | 可配置选项 | 读一致性 | 写一致性 | 事务隔离级别 |
|------|-----------|-----------|---------|---------|------------|
| MySQL | 强一致 (单节点) | 半同步复制 | 强一致 | 强一致 | READ COMMITTED, REPEATABLE READ, SERIALIZABLE |
| PostgreSQL | 强一致 (单节点) | 同步复制 | 强一致 | 强一致 | READ COMMITTED, REPEATABLE READ, SERIALIZABLE (SSI) |
| Spanner | 外部一致性 | - | 外部一致性 (TrueTime) | 外部一致性 | SERIALIZABLE, SNAPSHOT ISOLATION |
| CockroachDB | 外部一致性 | - | SERIALIZABLE 默认 | SERIALIZABLE | SERIALIZABLE (默认), SNAPSHOT ISOLATION |
| TiDB | 快照隔离 (SI) | - | SI / RC | SI / RC | RC, SI, SERIALIZABLE (实验性) |
| Cassandra | 最终一致 | 可调 (ONE→ALL) | 可调 (Tunable) | 可调 (Tunable) | 轻量事务 (LWT, Paxos) |
| DynamoDB | 最终/强一致 | 可选强一致读 | 默认最终, 可选强一致 | 强一致 | 无完整事务 (2018+ 支持跨分区事务) |
| HBase | 强一致 (Region 级) | - | 强一致 | 强一致 | 行级原子性 |
| MongoDB | 事件一致 (副本集) | 可调 readConcern | 可调 | 可调 (多数写) | 单文档原子, 4.0+ 多文档 ACID |
| Redis | 最终一致 (集群) | - | 最终一致 (集群) | 强一致 (单节点) | 单命令原子, Lua 脚本复合原子 |
| Neo4j | 强一致 (单节点) | 因果集群 | 因果一致 | 因果一致 | READ COMMITTED (默认) |
| FoundationDB | 严格串行化 | - | 严格串行化 | 严格串行化 | SERIALIZABLE (快照隔离+冲突检测) |
| VoltDB | 严格串行化 | - | 严格串行化 | 严格串行化 | SERIALIZABLE (确定性执行) |

### 2.3 复制策略

| 系统 | 复制模式 | 同步/异步 | 故障切换 | 复制协议 |
|------|---------|----------|---------|---------|
| MySQL | 主从, 主主, Group Replication | 异步/半同步/同步 (GTID) | MHA/Orchestrator/InnoDB Cluster | Binlog 复制 |
| PostgreSQL | 流复制, 逻辑复制 | 同步/异步 | Patroni/Repmgr | WAL 流复制 |
| Spanner | Paxos 多副本 | 同步 (Paxos) | 自动 (Paxos 重选) | Paxos (每 Paxos Group) |
| CockroachDB | Raft 多副本 | 同步 (Raft) | 自动 (Raft Leader 重选) | Raft (每个 Range) |
| TiDB | Raft 多副本 (TiKV) | 同步 (Raft) | 自动 (PD 调度) | Multi-Raft (TiKV Region) |
| Cassandra | 去中心化多主 | 异步 +  hinted handoff | 自动 (Gossip + 一致性哈希) | Read/Write Repair, Anti-Entropy |
| DynamoDB | 多 AZ 副本 | 同步 (托管) | 自动 (AWS 托管) | 内部协议 (不可见) |
| HBase | HDFS 副本 + Region 副本 | 异步 | HMaster 故障切换 | HDFS 复制 (数据) + ZooKeeper (协调) |
| MongoDB | 副本集 (Primary-Secondary) | 异步 (默认) | 自动 (选举) | Oplog 复制 |
| Redis | 主从 + 哨兵/集群 | 异步 | Sentinel/Cluster 故障切换 | AOF 子进程 + 全量/增量同步 |
| Neo4j | 因果集群 (Primary+Secondaries) | 同步/异步 | 自动 (Raft) | Raft (核心数据), Catch-up (follower) |
| FoundationDB | Paxos 多副本 | 同步 (Paxos) | 自动 | Paxos |
| VoltDB | K-Safe 副本 | 同步 (K-Safe) | 自动 (多副本) | 内存快照 + 命令日志 |

### 2.4 分片/分区

| 系统 | 分区策略 | 自动再平衡 | 分区粒度 | 跨分区事务 |
|------|---------|-----------|---------|-----------|
| MySQL | 手动分库分表/Proxy | 否 (需工具) | 表/数据库级 | 否 (需中间件) |
| PostgreSQL | 表分区 (声明式) | 否 | 表级 | 否 (单节点) |
| Spanner | 范围分片 (Split) | 自动 | Split (动态分裂/合并) | 是 (两阶段提交+TrueTime) |
| CockroachDB | 范围分片 (Range, 64MB) | 自动 | Range (动态分裂/合并) | 是 (两阶段提交+时间戳) |
| TiDB | Region 分片 (96MB) | 自动 (PD 调度) | Region (动态分裂/合并) | 是 (Percolator 事务模型) |
| Cassandra | 一致性哈希 (vnode) | 自动 (一致性哈希) | Token Range | 否 (单分区) / 有限 (LWT) |
| DynamoDB | 一致性哈希 + 分区键 | 自动 (AWS 托管) | 分区 (10GB/3000 RCU) | 有限 (2018+ 跨分区事务) |
| HBase | RowKey 范围分片 | 自动 (Region Split) | Region (动态分裂) | 否 (单行原子) |
| MongoDB | 范围分片 / 哈希分片 | 自动 (Balancer) | Chunk (64MB 默认) | 是 (4.2+ 多文档事务) |
| Redis | 哈希槽 (16384 slots) | 手动/半自动 | Slot (集群模式) | 否 (单节点原子) |
| Neo4j | Fabric (逻辑分区) | 否 (手动) | 数据库级 | 是 (Fabric 跨库查询) |
| FoundationDB | 范围分片 (Shard) | 自动 | Shard (动态分裂) | 是 (严格串行化) |
| VoltDB | 表分区 (哈希/复制表) | 自动 (Site 分配) | 表分区 (哈希) | 是 (单分区事务, 跨分区受限) |

### 2.5 CAP / PACELC 定位

```
                    Consistency (C)
                         │
                    ┌────┴────┐
                    │         │
                    │  CP     │  分区时选择一致性
                    │  NewSQL │
                    │ Spanner │
                    │ CRDB    │
                    │ TiDB    │
                    │ FDB     │
                    │ VoltDB  │
                    │         │
    Availability ───┼─────────┼─── Availability (A)
       (A)          │         │       (C 放弃)
                    │         │
                    │  AP/CA  │  分区时选择可用性
                    │  NoSQL  │
                    │ Cass    │
                    │ DDB     │
                    │ MongoDB │
                    │ Redis   │
                    │ HBase   │
                    │ Neo4j   │
                    └─────────┘
```

| 系统 | CAP 分类 | PACELC: 分区时 | PACELC: 正常时 | 说明 |
|------|---------|---------------|---------------|------|
| MySQL | CA | 不适用(单分区无P) | L | 单节点强一致+低延迟 |
| PostgreSQL | CA | 不适用(单分区无P) | L | 单节点强一致+低延迟 |
| Spanner | CP | C | E | TrueTime 保证全局一致, 牺牲延迟 |
| CockroachDB | CP | C | E | Raft 同步, 正常时也优先一致 |
| TiDB | CP | C | L/E | Raft 同步, 但读路径可优化延迟 |
| Cassandra | AP | A | L | 最终一致, 低延迟优先 |
| DynamoDB | AP (可 C) | A/C(可选) | L | 默认最终一致, 可选强一致读 |
| HBase | CP | C | E | Region 强一致, 依赖 HDFS |
| MongoDB | AP (可 C) | A/C(可调) | L | 默认最终一致, readConcern 可调 |
| Redis | AP | A | L | 异步复制, 内存极速 |
| Neo4j (集群) | CP | C | L | Raft 核心, 因果一致 |
| FoundationDB | CP | C | E | Paxos 严格串行化 |
| VoltDB | CP | C | L | 内存确定性执行, 低延迟+强一致 |

**PACELC 解读:**
- **P**artition: 网络分区发生时选 **C**onsistency 还是 **A**vailability?
- **E**lse: 正常时选 **L**atency 还是 **C**onsistency?
- NewSQL (Spanner/CRDB/FDB) = **CE** (分区选C, 正常也选E)
- 传统 NoSQL (Cassandra/Redis) = **AL** (分区选A, 正常选L)
- 可配置系统 (MongoDB/DynamoDB) = 根据场景切换

### 2.6 查询语言

| 系统 | 查询语言 | 查询能力 | 聚合/分析 | 全文搜索 | 地理查询 |
|------|---------|---------|---------|---------|---------|
| MySQL | SQL (ANSI) | JOIN, 子查询, 窗口函数 | ✅ | ✅ (Full-Text) | ✅ (GIS) |
| PostgreSQL | SQL (最完整) | JOIN, CTE, 窗口函数, 递归查询 | ✅ (最丰富) | ✅ | ✅ (PostGIS) |
| Spanner | Google Standard SQL | JOIN, 子查询 | 有限 | ❌ | 有限 |
| CockroachDB | PostgreSQL 方言 SQL | JOIN, CTE, 窗口函数 | ✅ | ❌ | 有限 |
| TiDB | MySQL 方言 SQL | JOIN, 子查询, 窗口函数 | ✅ (TiFlash) | ✅ (TiFlash) | 有限 |
| Cassandra | CQL (类 SQL) | 无 JOIN (应用层), 分区查询为主 | ❌ (有限聚合) | ❌ | ❌ |
| DynamoDB | API (Get/Put/Scan) | 条件表达式, PartiQL(SQL兼容) | ❌ | ❌ | ❌ |
| HBase | API / Shell / Thrift | Get, Scan, Filter | ❌ (需 Phoenix/Spark) | ❌ (需 Solr) | ❌ |
| MongoDB | MQL (JSON-like) | $lookup(JOIN), 聚合管道 | ✅ (聚合管道) | ✅ | ✅ |
| Redis | 命令协议 | 无查询语言 (数据结构操作) | ❌ | ❌ (RedisSearch) | ❌ (模块) |
| Neo4j | Cypher / GQL | 图模式匹配, 路径查询 | ✅ | ✅ | ✅ |
| FoundationDB | FDB API (有序KV) | 无查询语言 (应用层构建) | ❌ | ❌ | ❌ |
| VoltDB | SQL | 存储过程+SQL, 无跨分区JOIN | ✅ | ❌ | 有限 |

### 2.7 ACID 支持

| 系统 | 事务支持 | ACID 级别 | 跨节点事务 | 两阶段提交 | 快照/版本控制 |
|------|---------|----------|-----------|-----------|------------|
| MySQL (InnoDB) | ✅ | 完全 ACID | ❌ | ❌ | ✅ (MVCC) |
| PostgreSQL | ✅ | 完全 ACID | ❌ | ❌ | ✅ (MVCC) |
| Spanner | ✅ | 完全 ACID | ✅ | ✅ (TrueTime) | ✅ (MVCC) |
| CockroachDB | ✅ | 完全 ACID | ✅ | ✅ | ✅ (MVCC) |
| TiDB | ✅ | ACID | ✅ | ✅ (Percolator) | ✅ (MVCC) |
| Cassandra | 有限 (LWT) | 非 ACID (最终一致) | ❌ | ❌ | ❌ (轻量事务) |
| DynamoDB | 有限 (2018+) | 非 ACID (单分区ACID) | 有限 (2018+ TransactWrite) | ❌ | ❌ |
| HBase | 行级原子 | 行级 ACID | ❌ | ❌ | ✅ (多版本) |
| MongoDB | ✅ (4.0+) | 多文档 ACID | ✅ (4.2+) | ❌ | ✅ (WiredTiger MVCC) |
| Redis | 有限 (单命令/Lua) | 单命令原子 | ❌ | ❌ | ❌ |
| Neo4j | ✅ | ACID (图) | ❌ (Fabric 跨库有限) | ❌ | ✅ |
| FoundationDB | ✅ | 完全 ACID | ✅ | ✅ | ✅ (MVCC) |
| VoltDB | ✅ | 完全 ACID | ✅ (分区内) | ❌ (确定性单线程) | ✅ (快照) |

### 2.8 可扩展性

| 系统 | 水平扩展 | 垂直扩展 | 写入扩展 | 读取扩展 | 最大规模参考 |
|------|---------|---------|---------|---------|------------|
| MySQL | 有限 (分库分表) | ✅ | 有限 | ✅ (只读副本) | 单表 ~10亿行 |
| PostgreSQL | 有限 (Citus/分区) | ✅ | 有限 | ✅ (逻辑复制) | 单表 ~数十亿行 |
| Spanner | ✅ 线性 | ✅ | ✅ | ✅ | 全球数百节点 |
| CockroachDB | ✅ 线性 | ✅ | ✅ | ✅ | 全球数百节点 |
| TiDB | ✅ 线性 | ✅ (TiKV+TiDB 分离) | ✅ | ✅ (TiKV/TiFlash) | 数千节点 |
| Cassandra | ✅ 线性 | ✅ | ✅ | ✅ | 数千节点, PB 级 |
| DynamoDB | ✅ 自动 | ✅ (AWS 托管) | ✅ | ✅ | 无上限 (AWS 托管) |
| HBase | ✅ | ✅ | ✅ | ✅ | 数千节点, PB 级 |
| MongoDB | ✅ | ✅ | ✅ (分片) | ✅ | 数百节点 |
| Redis | ✅ (集群) | 受限于内存 | 有限 | ✅ (只读副本) | 内存容量上限 |
| Neo4j | 有限 (Fabric) | ✅ | 有限 | ✅ (只读副本) | 单图 ~数十亿节点 |
| FoundationDB | ✅ | ✅ | ✅ | ✅ | 数百节点 |
| VoltDB | ✅ | 受限于内存 | ✅ | ✅ | 数百节点, 内存受限 |

### 2.9 运维复杂度

| 系统 | 部署复杂度 | 监控/告警 | 备份恢复 | 升级难度 | 运维团队规模 | 学习曲线 |
|------|-----------|---------|---------|---------|------------|---------|
| MySQL | 低 | 成熟 (PMM, Orchestrator) | 成熟 (mysqldump, xtrabackup) | 低 | 小 | 低 |
| PostgreSQL | 低 | 成熟 (Patroni, pg_stat) | 成熟 (pg_basebackup, WAL-G) | 低 | 小 | 中 |
| Spanner | 极高 (GCP 托管) | GCP 托管 | GCP 托管 | GCP 托管 | 零 (托管) | 中 |
| CockroachDB | 中 | 内置 Admin UI | 内置 (BACKUP/RESTORE) | 低 | 中 | 中 |
| TiDB | 中高 | TiDB Dashboard, Prometheus | BR, TiDB Lightning | 低 | 中 | 中 |
| Cassandra | 高 | nodetool, Prometheus | sstableloader, 快照 | 中 | 大 | 高 |
| DynamoDB | 零 (AWS 托管) | CloudWatch | PITR, 按需备份 | 零 | 零 | 低 |
| HBase | 高 | HMaster UI, Ambari | 快照, 复制 | 高 | 大 | 高 |
| MongoDB | 中 | Atlas / Ops Manager | mongodump, 快照 | 低 | 小-中 | 中 |
| Redis | 低-中 | redis-cli, Sentinel 监控 | RDB/AOF | 低 | 小 | 低 |
| Neo4j | 中 | Neo4j Browser, Metrics | 备份/恢复工具 | 中 | 小-中 | 中 |
| FoundationDB | 中 | fdbcli, 内置监控 | 快照, 备份/恢复 | 中 | 中 | 高 |
| VoltDB | 中 | 内置管理控制台 | 快照 + 命令日志 | 低 | 中 | 中 |

---

## 3. 决策指南: 何时选择何种系统

### 3.1 按场景分类决策矩阵

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              场景 → 系统推荐                                  │
├────────────────────────┬─────────────────────────────────────────────────────┤
│ 场景                   │ 推荐系统 (优先级排序)                                │
├────────────────────────┼─────────────────────────────────────────────────────┤
│ OLTP 事务 (金融/支付)  │ Spanner > CockroachDB > TiDB > PostgreSQL > VoltDB  │
│ 高吞吐 OLTP (游戏/广告)│ VoltDB > Redis > FoundationDB > Cassandra           │
│ 全球分布式应用          │ Spanner > CockroachDB > TiDB                        │
│ 内容管理/CMS           │ MongoDB > PostgreSQL > MySQL                        │
│ 物联网/时间序列        │ Cassandra > HBase > TiDB                            │
│ 社交网络/推荐          │ Neo4j > MongoDB > Cassandra                         │
│ 缓存/会话存储          │ Redis > DynamoDB > Memcached                        │
│ 大数据分析 (写入密集)  │ HBase > Cassandra > MongoDB                         │
│ Serverless/Microservice│ DynamoDB > MongoDB > CockroachDB                    │
│ 游戏排行榜/实时计分    │ Redis > VoltDB > FoundationDB                       │
│ 电商目录/产品目录      │ MongoDB > Elasticsearch > DynamoDB                  │
│ 知识图谱/关系查询      │ Neo4j > PostgreSQL (图扩展)                         │
│ 金融风控 (低延迟)      │ VoltDB > FoundationDB > Redis                       │
│ 消息队列/事件流        │ Cassandra > MongoDB > DynamoDB (Streams)            │
│ 简单 Key-Value 存储    │ FoundationDB > Redis > DynamoDB                     │
│ 需要 JOIN + 分布式     │ Spanner > CockroachDB > TiDB > PostgreSQL(Citus)    │
│ MySQL 生态兼容         │ TiDB > MySQL > CockroachDB                          │
│ 最小运维 (完全托管)    │ DynamoDB > Spanner(GCP) > MongoDB Atlas             │
│ 边缘/轻量部署          │ SQLite > Redis > MongoDB                            │
└────────────────────────┴─────────────────────────────────────────────────────┘
```

### 3.2 按数据量与团队规模决策

```
┌─────────────────────────────────────────────────────────────────┐
│                    数据量 + 团队 → 系统选择                       │
├──────────────────┬──────────────────────────────────────────────┤
│ < 100GB, 小团队  │ MySQL / PostgreSQL / MongoDB / Redis         │
│ 100GB - 10TB     │ PostgreSQL / MongoDB / TiDB / CockroachDB    │
│ 10TB - 100TB     │ TiDB / CockroachDB / Cassandra / HBase       │
│ > 100TB          │ Cassandra / HBase / Spanner / TiDB           │
│ 不确定, 要弹性   │ DynamoDB / MongoDB Atlas / CockroachDB       │
│ 全球多区域       │ Spanner / CockroachDB / TiDB                 │
│ 严格金融合规     │ Spanner / CockroachDB / VoltDB / OceanBase   │
│ 零运维团队       │ DynamoDB / Spanner / MongoDB Atlas           │
└──────────────────┴──────────────────────────────────────────────┘
```

### 3.3 核心选择维度速查

```
需要 ACID + 分布式 JOIN? ──→ Spanner / CockroachDB / TiDB
需要极致低延迟 + ACID?   ──→ VoltDB / FoundationDB
需要海量写入 + 最终一致?  ──→ Cassandra / HBase / DynamoDB
需要灵活 Schema + 文档?   ──→ MongoDB / DynamoDB
需要缓存/会话/排行榜?     ──→ Redis
需要图关系遍历?           ──→ Neo4j
需要最小运维?             ──→ DynamoDB / Spanner(GCP)
需要 MySQL 兼容分布式?    ──→ TiDB
需要 PG 兼容分布式?       ──→ CockroachDB
需要有序 KV + ACID 层?    ──→ FoundationDB
```

---

## 4. ASCII 决策树

```
                     你的数据是什么类型?
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
      关系型/表格        文档/半结构化       图/关系密集
          │                 │                 │
          ▼                 ▼                 ▼
   需要分布式部署?      需要 ACID?         需要图遍历?
          │                 │                 │
     ┌────┴────┐      ┌────┴────┐       ┌────┴────┐
     │         │      │         │       │         │
    是        否     是        否      是        否
     │         │      │         │       │         │
     ▼         ▼      ▼         ▼       ▼         ▼
  CAP选择?   MySQL  ACID      文档DB   Neo4j   MongoDB
    │       /PG    要求高    + 弹性   /图存储  /文档
  ┌─┴───┐          且分布式  Schema          + 文档
  │     │          需要?               特性
 CP     AP
  │      │
  ▼      ▼
强一致  最终一致
低延迟  高可用
  │      │
  ├──────┤
  │      │
  ▼      ▼
Spanner  Cassandra
CRDB     DynamoDB
TiDB     HBase
VoltDB   MongoDB
FDB      (最终
         一致
         模式)

  ─────────────────────────────────────────────
  
  更精确的决策路径:
  
  1. 数据结构?
     ├── 表格/关系 → 进入 2A
     ├── 文档/JSON → 进入 2B
     ├── Key-Value → 进入 2C
     └── 图        → Neo4j (或 PG + 图扩展)
  
  2A. 需要分布式 + JOIN?
     ├── 是, 且需要全球一致 → Spanner / CockroachDB
     ├── 是, MySQL 兼容    → TiDB
     ├── 是, 低延迟内存     → VoltDB
     ├── 否 (单节点够用)    → PostgreSQL / MySQL
     └── 是, 有序 KV 基础   → FoundationDB
  
  2B. 需要 ACID 事务?
     ├── 是 → MongoDB (4.2+)
     ├── 否, 需要托管      → DynamoDB
     └── 否, 需要海量写入  → Cassandra
  
  2C. 需要持久化 + ACID?
     ├── 是, 有序范围查询   → FoundationDB
     ├── 是, 极致低延迟     → VoltDB
     ├── 是, 托管           → DynamoDB
     └── 否 (缓存/临时)    → Redis
```

---

## 5. 5-Dim 系统深度分析

### 5.1 MySQL

#### Context & Motivation
- **解决问题**: 90年代 Web 应用爆发, 需要轻量、开源的关系型数据库
- **前代局限**: 商业数据库 (Oracle, DB2) 昂贵且笨重
- **硬件背景**: 单机 x86, 磁盘 I/O 是瓶颈

#### Architecture
```
  ┌──────────────────────────┐
  │       应用层              │
  ├──────────────────────────┤
  │    MySQL Server (SQL)    │
  │  Parser → Optimizer → Exec│
  ├──────────────────────────┤
  │     Storage Engines      │
  │  ┌─────────┬──────────┐  │
  │  │ InnoDB  │ MyISAM   │  │
  │  │ (B+Tree)│ (表级锁)  │  │
  │  └─────────┴──────────┘  │
  └──────────────────────────┘
```
- **拓扑**: 主从复制 (Master-Slave), 可升级为 Group Replication (Paxos)
- **元数据**: 集中式 (Information Schema + 系统表)
- **数据架构**: B+Tree 行存储, 固定页大小 (16KB), InnoDB 行级锁 + MVCC
- **复制**: Binlog (逻辑复制), 异步/半同步/GTID

#### Core Technical Innovations
- InnoDB 引擎: 行级锁 + MVCC + 崩溃恢复 (redo/undo log)
- 复制拓扑: 主从复制链, 支持半同步
- Group Replication: 基于 Paxos 的多主/单主模式

#### Trade-offs (CAP / PACELC)
- **CAP**: CA (单节点无分区概念, 复制时偏向一致性)
- **PACELC**: 正常时选 L (低延迟), 分区场景取决于复制模式
- **复杂度 vs 运维**: 运维成熟度高, 但分库分表需外部中间件
- **成本 vs 性能**: 开源免费, 但大规模需要分片中间件增加复杂度

#### Influence & Legacy
- 最广泛部署的开源 RDBMS
- 启发了 MariaDB, Percona Server
- 云厂商均有托管 MySQL (RDS, Aurora)
- 现代 Web 应用的默认数据库选择

---

### 5.2 PostgreSQL

#### Context & Motivation
- **解决问题**: 学术界开源 RDBMS, 强调可扩展性和标准合规
- **前代局限**: 类型系统受限, 扩展性差
- **硬件背景**: 单机 x86, 逐步扩展到 SMP 多核

#### Architecture
- **拓扑**: 单节点为主, 流复制 + 逻辑复制, 外部方案 (Patroni, Citus)
- **元数据**: 集中式 (系统目录表)
- **数据架构**: Heap 存储 + TOAST (大对象外溢), MVCC 多版本并发控制
- **索引**: B-Tree, GIN (倒排), GIST (空间), BRIN (块范围), SP-GiST

#### Core Technical Innovations
- MVCC: 无读锁, 读写不阻塞
- 可扩展类型系统: 自定义类型, 操作符, 函数
- 窗口函数, CTE, 递归查询 (图遍历)
- JSONB: 半结构化数据 + 关系查询融合
- 逻辑复制: 行级 CDC 支持

#### Trade-offs (CAP / PACELC)
- **CAP**: CA (单节点设计, 无内置分区容忍)
- **PACELC**: EL (正常时低延迟, 无分区概念)
- **复杂度 vs 运维**: 功能最丰富但配置复杂, VACUUM 需维护
- **成本 vs 性能**: 开源, 但大规模分布式需外部扩展 (Citus)

#### Influence & Legacy
- 现代 "功能最丰富的开源 RDBMS"
- CockroachDB 的 SQL 方言直接兼容 PostgreSQL
- JSONB 成为关系+文档融合的标杆
- Citus 扩展使其具备分布式能力

---

### 5.3 Google Spanner

#### Context & Motivation
- **解决问题**: Google 需要全球分布式、外部一致性的关系数据库
- **前代局限**: MySQL 分片方案运维复杂, 无法保证跨数据中心一致性
- **硬件背景**: 原子钟 + GPS 提供全球时间同步 (TrueTime)

#### Architecture
```
  ┌─────────────────────────────────────┐
  │         全球分布式 Spanner            │
  │                                     │
  │  ┌─────────┐  ┌─────────┐ ┌───────┐│
  │  │ Zone A  │  │ Zone B  │ │Zone C ││
  │  │┌──┬──┐  │  │┌──┬──┐  │ │┌──┬──┐││
  │  ││P1│P2│  │  ││P3│P4│  │ ││P5│P6│││
  │  │└──┴──┘  │  │└──┴──┘  │ │└──┴──┘││
  │  └─────────┘  └─────────┘ └───────┘│
  │   Paxos Group (每个 Span 独立)        │
  └─────────────────────────────────────┘
```
- **拓扑**: 多区域, 每个 Span (分片) 由 Paxos Group 管理
- **元数据**: 原生分布式元数据服务
- **数据架构**: 范围分片 (Split), B+Tree 存储, TrueTime 版本控制
- **复制**: Paxos 同步复制, 每 Span 独立 Paxos Group

#### Core Technical Innovations
- **TrueTime**: 原子钟 + GPS 提供有界误差的全局时钟, 实现外部一致性
- **两阶段提交**: 跨 Span 事务使用 2PC + TrueTime 优化
- **MVCC + TrueTime**: 消除时钟回拨问题, 实现真正的快照隔离
- **自动分片**: Split 动态分裂/合并, 无需人工干预

#### Trade-offs (CAP / PACELC)
- **CAP**: CP (分区时选择一致性)
- **PACELC**: CE (分区选C, 正常也选E — 为一致性可接受延迟)
- **复杂度 vs 运维**: GCP 托管, 用户侧复杂度低; 自建复杂度极高
- **成本 vs 性能**: 高成本 (专用硬件 + 全球部署), 但提供不可替代的外部一致性

#### Influence & Legacy
- 开创了 NewSQL 时代
- 直接启发了 CockroachDB (开源替代)
- TrueTime 概念影响深远 (CockroachDB 使用 HLC 近似)
- 定义了全球分布式数据库的标准

---

### 5.4 CockroachDB

#### Context & Motivation
- **解决问题**: 提供 Spanner 的开源替代, 不依赖 TrueTime 硬件
- **前代局限**: Spanner 闭源 + 依赖专用硬件, 无法自建
- **硬件背景**: 通用 x86 集群, NTP 时间同步

#### Architecture
- **拓扑**: 去中心化对等节点, Raft 一致性
- **元数据**: 原生分布式 (Range 元数据存储在系统 Range)
- **数据架构**: Range (64MB) 范围分片, RocksDB (LSM-Tree) 存储引擎
- **复制**: Raft (每个 Range 独立 Raft Group), 同步复制
- **事务**: 分布式事务 + 乐观并发控制 + Hybrid Logical Clock (HLC)

#### Core Technical Innovations
- **HLC (Hybrid Logical Clock)**: 替代 TrueTime, 用逻辑时钟 + 物理时间近似实现外部一致性
- **自动再平衡**: Range 自动分裂/合并, 跨节点均衡
- **SQL 层与存储层分离**: SQL 层无状态, 可独立扩展
- **生存时间 (TTL)**: 内置数据生命周期管理

#### Trade-offs (CAP / PACELC)
- **CAP**: CP (分区时选择一致性)
- **PACELC**: CE (正常时也优先一致性)
- **复杂度 vs 运维**: 中等复杂度, 内置监控, 自动运维友好
- **成本 vs 性能**: 开源免费, 但 Raft 同步复制有写延迟开销

#### Influence & Legacy
- Spanner 最成功的开源实现
- PostgreSQL 方言兼容降低迁移成本
- 推动 NewSQL 在中小企业落地
- 被多家企业用作全球分布式数据库方案

---

### 5.5 TiDB

#### Context & Motivation
- **解决问题**: 中国场景下 MySQL 分库分表方案运维复杂, 需要兼容 MySQL 的分布式数据库
- **前代局限**: MySQL 分片需要中间件 (DRDS, MyCat), 跨分片事务困难
- **硬件背景**: 通用 x86 集群 + SSD, 中国云环境

#### Architecture
```
  ┌──────────────────────────────────────┐
  │              TiDB                     │
  │                                      │
  │  ┌──────────┐  ┌─────────┐          │
  │  │ TiDB    │  │ TiDB    │  ← SQL 层  │
  │  │ (无状态) │  │ (无状态) │          │
  │  └────┬─────┘  └────┬────┘          │
  │       └──────┬──────┘               │
  │              ▼                      │
  │  ┌──────────────────────┐          │
  │  │        PD            │  ← 调度层  │
  │  │ (Placement Driver)   │          │
  │  │ 元数据 + Region 调度  │          │
  │  └──────────┬───────────┘          │
  │             ▼                      │
  │  ┌──────────────────────┐          │
  │  │        TiKV          │  ← 存储层  │
  │  │  ┌──┐ ┌──┐ ┌──┐     │          │
  │  │  │R1│ │R2│ │R3│ ... │ Raft     │
  │  │  └──┘ └──┘ └──┘     │ Group    │
  │  └──────────────────────┘          │
  │  ┌──────────────────────┐          │
  │  │       TiFlash        │  ← 列存   │
  │  │   (HTAP 分析)        │          │
  │  └──────────────────────┘          │
  └──────────────────────────────────────┘
```
- **拓扑**: 三层分离 (TiDB 无状态 SQL 层 → PD 调度层 → TiKV 存储层)
- **元数据**: PD 集中式调度 + 分布式存储
- **数据架构**: Region (96MB) 范围分片, RocksDB (LSM-Tree), Multi-Raft
- **复制**: Multi-Raft (每个 Region 独立 Raft Group)
- **事务**: Percolator 分布式事务模型 (乐观锁 + Timestamp Oracle)

#### Core Technical Innovations
- **计算存储分离**: 三层架构, 各层独立扩展
- **HTAP**: TiKV (行存 OLTP) + TiFlash (列存 OLAP), 实时分析
- **Percolator 事务**: 借鉴 Google Percolator 的分布式事务模型
- **MySQL 兼容**: 协议层完全兼容 MySQL, 迁移成本极低

#### Trade-offs (CAP / PACELC)
- **CAP**: CP (分区时选择一致性)
- **PACELC**: C/L (分区选C, 正常时可优化延迟)
- **复杂度 vs 运维**: 中高复杂度 (三层架构), 但 TiUP 简化部署
- **成本 vs 性能**: 开源, 通用硬件; 写入性能受 Raft 限制

#### Influence & Legacy
- 中国最成功的开源分布式数据库
- PingCAP 商业化, 在金融/互联网广泛落地
- TiKV 作为独立 KV 存储被其他项目使用
- 推动 MySQL 生态的分布式转型

---

### 5.6 Apache Cassandra

#### Context & Motivation
- **解决问题**: Facebook 需要去中心化、高可用、可线性扩展的列式存储
- **前代局限**: RDBMS 无法水平扩展, 分片方案复杂
- **硬件背景**: 廉价 commodity 集群, 跨数据中心

#### Architecture
- **拓扑**: 去中心化对等节点 (Ring), 无主节点
- **元数据**: 算法/计算式 (一致性哈希 + Gossip 协议)
- **数据架构**: 一致性哈希 (vnode), LSM-Tree 存储, 列族模型
- **复制**: 多主异步复制 + Hinted Handoff + Read/Write Repair
- **一致性**: 可调一致性 (Tunable Consistency), N/W/R 策略

#### Core Technical Innovations
- **一致性哈希 + vnode**: 自动负载均衡, 节点增删影响最小化
- **可调一致性**: N/W/R 策略 (写 N 节点, 等 W 确认, 读 R 节点), W+R>N 实现强一致
- **LSM-Tree**: 写入追加, 读时合并, 适合写入密集
- **去中心化**: 无单点故障, Gossip 协议协调
- **SSTable + Compaction**: 后台合并优化

#### Trade-offs (CAP / PACELC)
- **CAP**: AP (分区时选择可用性, 可配置为 C)
- **PACELC**: AL (分区选A, 正常选L — 低延迟优先)
- **复杂度 vs 运维**: 高复杂度, Compaction 策略选择影响性能
- **成本 vs 性能**: 高写入吞吐, 但读延迟不稳定, 无 JOIN 能力

#### Influence & Legacy
- 融合了 Dynamo (去中心化) + Bigtable (列族) 的设计
- Apache Cassandra → DataStax 商业化
- 影响 ScyllaDB (C++ 重写, 更高性能)
- 适合 IoT、消息队列、日志等写入密集场景

---

### 5.7 Amazon DynamoDB

#### Context & Motivation
- **解决问题**: 托管的、自动扩展的 NoSQL 数据库, 开发者零运维
- **前代局限**: 自运维 Cassandra/HBase 需要专业 DBA 团队
- **硬件背景**: AWS 云基础设施, 多 AZ 部署

#### Architecture
- **拓扑**: AWS 托管多 AZ 部署, 对用户透明
- **元数据**: 算法/计算式 (一致性哈希, AWS 内部管理)
- **数据架构**: 一致性哈希分区, 每分区 10GB 上限, SSD 存储
- **复制**: 多 AZ 同步复制 (AWS 托管), 跨区域全局表
- **一致性**: 默认最终一致读, 可选强一致读

#### Core Technical Innovations
- **按需扩展**: 自动分区, 无需手动分片管理
- **DAX**: 内存加速层 (微秒级读)
- **Streams**: 变更数据捕获 (CDC), 触发 Lambda
- ** PartiQL**: SQL 兼容查询层 (2019+)
- **TransactWrite/TransactGet**: 跨分区事务支持 (2018+)
- **单 digit ms 延迟**: 全托管优化

#### Trade-offs (CAP / PACELC)
- **CAP**: AP (默认), 可选 C (强一致读)
- **PACELC**: A/L (默认), 可选 C (强一致读增加延迟)
- **复杂度 vs 运维**: 零运维 (托管), 但查询模型受限 (无 JOIN)
- **成本 vs 性能**: 按吞吐计费, 大规模可能昂贵; 性能稳定

#### Influence & Legacy
- Serverless 数据库标杆
- 定义了托管 NoSQL 的标准
- 影响了 FaunaDB, Azure Cosmos DB 等
- 适合微服务、Serverless 架构

---

### 5.8 Apache HBase

#### Context & Motivation
- **解决问题**: 开源版 Bigtable, 运行在 Hadoop 生态上
- **前代局限**: Bigtable 闭源, HDFS 需要结构化查询
- **硬件背景**: Hadoop 集群, 廉价 commodity 硬件

#### Architecture
```
  ┌─────────────────────────────────────┐
  │            HBase                     │
  │                                     │
  │  ┌──────────┐    ┌──────────────┐   │
  │  │ HMaster  │    │ ZooKeeper    │   │
  │  │ (元数据)  │    │ (协调/选主)   │   │
  │  └──────────┘    └──────────────┘   │
  │         │                           │
  │         ▼                           │
  │  ┌──────────┐  ┌──────────┐        │
  │  │Region    │  │Region    │        │
  │  │Server 1  │  │Server 2  │        │
  │  │[R1,R2]   │  │[R3,R4]   │        │
  │  └────┬─────┘  └────┬─────┘        │
  │       └──────┬──────┘              │
  │              ▼                     │
  │  ┌──────────────────────┐         │
  │  │       HDFS           │         │
  │  │  (HFile 持久化存储)    │         │
  │  └──────────────────────┘         │
  └─────────────────────────────────────┘
```
- **拓扑**: HMaster + RegionServer 分层架构
- **元数据**: 集中式 (HMaster + hbase:meta 表)
- **数据架构**: RowKey 范围分片 (Region), HFile (LSM-like), 列族存储
- **复制**: HDFS 副本 (数据) + 异步跨集群复制

#### Core Technical Innovations
- Bigtable 开源实现: 列族 + 多版本 + 行级原子性
- 与 Hadoop 生态深度集成: MapReduce, Spark, Hive
- Bloom Filter 优化随机读
- Coprocessor: 类似 Hadoop 的 MapReduce, 在 RegionServer 端执行

#### Trade-offs (CAP / PACELC)
- **CAP**: CP (Region 级强一致, 依赖 HDFS)
- **PACELC**: CE (分区选C, 正常也选E)
- **复杂度 vs 运维**: 高复杂度, 依赖 ZooKeeper + HDFS
- **成本 vs 性能**: 适合大规模, 但小集群运维成本高于单机 DB

#### Influence & Legacy
- Bigtable 的开源化身
- Hadoop 生态的核心存储组件
- 影响了 Apache Phoenix (SQL on HBase)
- 逐渐被云原生方案 (Spanner, Bigtable) 替代

---

### 5.9 MongoDB

#### Context & Motivation
- **解决问题**: Web 应用需要灵活 Schema, 关系模型过于僵化
- **前代局限**: RDBMS Schema 变更成本高, JSON 存储不便
- **硬件背景**:  commodity 集群, SSD 普及

#### Architecture
- **拓扑**: 副本集 (Primary-Secondary) + 分片集群
- **元数据**: 集中式 (Config Server, 3 节点副本集)
- **数据架构**: BSON 文档存储, WiredTiger (B-Tree), Chunk 分片 (64MB)
- **复制**: Oplog (操作日志) 异步复制
- **事务**: 4.0+ 多文档 ACID (副本集), 4.2+ 分片集群事务

#### Core Technical Innovations
- **文档模型**: BSON 存储, 天然匹配 JSON 应用数据
- **聚合管道**: 强大的数据处理能力 (类似 MapReduce)
- **灵活索引**: 复合, 地理空间, 全文, 文本
- **变更流**: 实时数据变更通知
- **WiredTiger**: 文档级并发控制 + 压缩

#### Trade-offs (CAP / PACELC)
- **CAP**: AP (默认), 可配置 C (readConcern: "majority")
- **PACELC**: A/L (默认), 可调 C/E
- **复杂度 vs 运维**: 中等, Atlas 托管降低复杂度
- **成本 vs 性能**: 写入性能好, 但 JOIN 性能差于 RDBMS

#### Influence & Legacy
- 文档数据库的事实标准
- MEAN/MERN 栈的核心组件
- MongoDB Atlas 推动 DBaaS 发展
- 灵活 Schema 范式影响 DynamoDB, Couchbase 等

---

### 5.10 Redis

#### Context & Motivation
- **解决问题**: 需要极速的内存数据存储, 替代 Memcached + 更多数据结构
- **前代局限**: Memcached 只支持字符串, 无持久化
- **硬件背景**: 大内存服务器, 千兆/万兆网络

#### Architecture
- **拓扑**: 主从 + Sentinel 高可用 / Cluster 分片
- **元数据**: 无中心 (Cluster 模式: 16384 哈希槽分布)
- **数据架构**: 纯内存, 5 种原生数据结构 (String, Hash, List, Set, Sorted Set)
- **持久化**: RDB (快照) + AOF (追加日志)
- **复制**: 异步复制, 全量同步 + 增量同步

#### Core Technical Innovations
- **单线程事件循环**: 无锁, 极高单核性能
- **丰富数据结构**: Bitmap, HyperLogLog, Geo, Stream
- **Lua 脚本**: 原子执行复合操作
- **Pub/Sub + Stream**: 消息队列能力
- **模块系统**: RedisSearch, RedisGraph, RedisJSON

#### Trade-offs (CAP / PACELC)
- **CAP**: AP (异步复制, 分区时可能丢数据)
- **PACELC**: AL (始终选择可用性和低延迟)
- **复杂度 vs 运维**: 低, 但集群模式需额外管理
- **成本 vs 性能**: 极致性能, 但受内存容量限制, 成本高

#### Influence & Legacy
- 缓存/内存数据库的事实标准
- 数据结构即索引范式影响后续 KV 存储
- 模块生态 (Search, Graph, JSON, TimeSeries)
- 被 Amazon ElastiCache, Google Memorystore 托管

---

### 5.11 Neo4j

#### Context & Motivation
- **解决问题**: 关系型数据库无法高效处理深度图遍历 (多跳 JOIN 性能差)
- **前代局限**: RDBMS JOIN 深度增加时性能指数级下降
- **硬件背景**: 单机 → 集群, 图遍历对内存/指针友好

#### Architecture
- **拓扑**: 单节点 → 因果集群 (Primary + Secondaries)
- **元数据**: 集中式 (单节点) / Raft (因果集群)
- **数据架构**: 原生图存储 (节点 + 关系 + 属性), 指针遍历
- **复制**: Raft (核心数据) + Catch-up (follower 同步)

#### Core Technical Innovations
- **原生图存储**: 节点直接通过指针连接关系, 无 JOIN 开销
- **Cypher 查询语言**: 声明式图查询, 直观的模式匹配
- **索引自由关系遍历**: 关系遍历时不依赖索引, O(1) 跳转
- **图算法库**: PageRank, 最短路径, 社区检测

#### Trade-offs (CAP / PACELC)
- **CAP**: CA (单节点), CP (因果集群 Raft)
- **PACELC**: CL (正常时低延迟)
- **复杂度 vs 运维**: 中等, 图建模需要专业知识
- **成本 vs 性能**: 图遍历性能远超 RDBMS, 但非图场景性能一般

#### Influence & Legacy
- 图数据库的事实标准
- 推动 GQL (ISO 图查询语言) 标准化
- 影响 JanusGraph, Amazon Neptune, TigerGraph
- 在推荐系统、欺诈检测、知识图谱广泛应用

---

### 5.12 FoundationDB

#### Context & Motivation
- **解决问题**: 需要一个有序 KV 存储, 在此之上构建 ACID 分布式事务层
- **前代局限**: 现有 KV 存储无事务或无序, 无法构建强一致应用
- **硬件背景**: Apple 收购后重新开源, 通用 x86 集群

#### Architecture
```
  ┌──────────────────────────────────────┐
  │         FoundationDB                  │
  │                                      │
  │  ┌────────────────────────────┐      │
  │  │    API Layers (应用层)      │      │
  │  │ Document / SQL / Graph ... │      │
  │  └──────────────┬─────────────┘      │
  │                 ▼                    │
  │  ┌────────────────────────────┐      │
  │  │    Transaction Layer       │      │
  │  │  (MVCC + 冲突检测)          │      │
  │  └──────────────┬─────────────┘      │
  │                 ▼                    │
  │  ┌────────────────────────────┐      │
  │  │    Ordered Key-Value       │      │
  │  │    (Redwood B-Tree)        │      │
  │  └──────────────┬─────────────┘      │
  │                 ▼                    │
  │  ┌────────────────────────────┐      │
  │  │    Paxos Replication       │      │
  │  │    (多副本强一致)           │      │
  │  └────────────────────────────┘      │
  └──────────────────────────────────────┘
```
- **拓扑**: 分层架构 (存储层 → 事务层 → API Layer)
- **元数据**: 原生分布式 (Paxos 协调)
- **数据架构**: 有序键值 (Redwood B-Tree), 范围分片 (Shard)
- **复制**: Paxos 同步复制, 严格串行化
- **事务**: 快照隔离 + 冲突检测 (乐观并发控制)

#### Core Technical Innovations
- **分层架构**: 底层有序 KV + 上层事务 = 可插拔数据模型
- **严格串行化**: 有序 KV 天然支持范围事务, MVCC + 冲突检测
- **Redwood 存储引擎**: 乐观 B-Tree, 无锁写入
- **Apple 内部使用**: iCloud, Apple Music 等核心服务底层

#### Trade-offs (CAP / PACELC)
- **CAP**: CP (分区时选择一致性)
- **PACELC**: CE (分区选C, 正常也选E — 严格串行化)
- **复杂度 vs 运维**: 中等, 分层架构抽象良好, 但应用需理解 KV 模型
- **成本 vs 性能**: 开源, 但生态不如主流数据库

#### Influence & Legacy
- Apple 核心基础设施 (iCloud)
- 分层架构理念影响 TiKV, CockroachDB
- Layer 概念: 同一存储上可构建多种数据模型
- 2013 年 Apple 收购, 2018 年重新开源

---

### 5.13 VoltDB

#### Context & Motivation
- **解决问题**: 传统数据库事务延迟过高, 需要内存级 ACID 性能
- **前代局限**: 磁盘 I/O 是事务瓶颈, MVCC 锁竞争
- **硬件背景**: 大内存服务器, 多核 CPU, 内存容量大幅提升

#### Architecture
- **拓扑**: 对等节点集群, 每个节点运行相同代码
- **元数据**: 集中式协调 (选举 Coordinator)
- **数据架构**: 纯内存行存储, 表分区 (哈希), K-Safe 副本
- **执行模型**: 确定性单线程执行 (每分区一个线程), 无锁
- **持久化**: 快照 + 命令日志 (Command Log)

#### Core Technical Innovations
- **确定性执行**: 相同输入 → 相同输出, 无需锁/闩
- **单线程分区**: 每分区单线程, 消除并发冲突
- **K-Safe 容错**: K 个节点故障不影响服务
- **存储过程**: 事务封装为 Java 存储过程, 确保确定性

#### Trade-offs (CAP / PACELC)
- **CAP**: CP (分区时选择一致性)
- **PACELC**: CL (分区选C, 正常选L — 内存级低延迟)
- **复杂度 vs 运维**: 中等, 但应用需重写为存储过程
- **成本 vs 性能**: 极致低延迟 (百万级 TPS), 但受内存限制

#### Influence & Legacy
- Michael Stonebraker (图灵奖获得者) 设计
- 开创内存 NewSQL 范式
- 适合金融风控、实时计费、电信计费
- 被 Volt Active Data 商业化

---

## 附录: 系统速查卡

```
┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ 系统        │ 类型     │ 一致性   │ 扩展     │ 事务     │ 最佳场景  │
├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┤
│ MySQL       │ RDBMS    │ 强一致   │ 垂直/主从│ ACID     │ Web OLTP │
│ PostgreSQL  │ RDBMS    │ 强一致   │ 垂直/复制│ ACID     │ 复杂查询  │
│ Spanner     │ NewSQL   │ 外部一致 │ 水平线性 │ 全球ACID │ 全球应用  │
│ CockroachDB │ NewSQL   │ 外部一致 │ 水平线性 │ 分布式   │ 全球分布  │
│ TiDB        │ NewSQL   │ 快照隔离 │ 水平线性 │ 分布式   │ MySQL替代 │
│ Cassandra   │ 宽列     │ 可调     │ 水平线性 │ 轻量事务 │ 写入密集  │
│ DynamoDB    │ KV/文档  │ 可调     │ 自动扩展 │ 有限事务 │ Serverless│
│ HBase       │ 宽列     │ 强一致   │ 水平线性 │ 行级原子 │ 大数据    │
│ MongoDB     │ 文档     │ 可调     │ 水平分片 │ 多文档   │ 灵活数据  │
│ Redis       │ 内存KV   │ 最终一致 │ 集群分片 │ 单命令   │ 缓存      │
│ Neo4j       │ 图       │ 强一致   │ 有限扩展 │ ACID     │ 图遍历    │
│ FoundationDB│ 有序KV   │ 强一致   │ 水平线性 │ 严格ACID │ 平台层    │
│ VoltDB      │ 内存SQL  │ 强一致   │ 水平分片 │ 严格ACID │ 低延迟    │
└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```

---

## 参考资料

1. Google Spanner: Corbett et al., "Spanner: Google's Globally-Distributed Database", OSDI 2012
2. Amazon Dynamo: DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store", SOSP 2007
3. Google Bigtable: Chang et al., "Bigtable: A Distributed Storage System for Structured Data", OSDI 2006
4. Apache Cassandra: Lakshman & Malik, "Cassandra: A Decentralized Structured Storage System", 2009
5. CockroachDB: "CockroachDB: The Resilient Geo-Distributed SQL Database", 2016
6. TiDB: "TiDB: A Cloud-Native Distributed HTAP Database", PingCAP
7. FoundationDB: "FoundationDB Architecture & Design", Apple (开源版文档)
8. VoltDB: Stonebraker et al., "VoltDB: A Main-Memory Database System", 2010
9. MongoDB: "MongoDB: The Definitive Guide", O'Reilly
10. Neo4j: "Neo4j in Action", Manning
11. Gilbert & Lynch, "Brewer's Conjecture and the Feasibility of Consistent, Available Partition-Tolerant Web Services", 2002
12. Abadi et al., "PACELC: A Practical Alternative to Brewer's CAP Theorem", 2012

---

*文档生成时间: 2026-06-11 | 基于 5-Dim Analysis Framework | 架构师 Skill*
