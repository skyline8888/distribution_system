# 分布式数据库技术谱系 (Distributed Database Technology Genealogy)

> 追溯分布式数据库设计模式的思想传播路径，展示关键论文、开创性系统及其衍生后代之间的血缘关系。

---

## 一、总览：分布式数据库演进时间线

```
 年代         │ 时代            │ 代表系统
──────────────┼─────────────────┼─────────────────────────────────────
 1970s        │ 关系型启蒙       │ System R, Ingres
 1980s–1990s  │ 商业化 RDBMS     │ Oracle, DB2, Informix, Sybase
 1990s        │ 开源 RDBMS       │ MySQL (1995), PostgreSQL (1996)
 2003–2005    │ Google 三驾马车   │ GFS (2003), MapReduce (2004), Bigtable (2006)
 2007         │ Amazon Dynamo    │ Dynamo 论文发布
 2008–2010    │ NoSQL 爆发       │ HBase, Cassandra, MongoDB, Redis, Riak
 2010         │ NewSQL 萌芽      │ VoltDB
 2012         │ Spanner         │ Spanner 论文 + DynamoDB 商用
 2013–2015    │ NewSQL 成型      │ FoundationDB (2013), CockroachDB (2014), TiDB (2015)
 2015–2018    │ 云原生数据库      │ Aurora (2015), PolarDB (2017), Google Cloud Spanner (2017)
 2018–present │ 融合与开源化      │ YugabyteDB, OceanBase 开源, 湖仓一体
```

---

## 二、ASCII 思想传播树

### 2.1 关系型数据库血脉

```
System R (IBM, 1970s) ── 关系模型 + SQL 原型
│
├──► IBM DB2 (1983) ── 企业级 RDBMS
│
├──► Oracle (1979) ── 商业 RDBMS 霸主
│
└──► PostgreSQL (1996, UC Berkeley)
     │   源于 Ingres → POSTGRES → PostgreSQL
     │
     ├──► Greenplum (2003) ── MPP 分析型数据库
     │
     └──► CockroachDB (2014) ── 语法兼容 PostgreSQL
     │
     └──► YugabyteDB (2017) ── 语法兼容 PostgreSQL
```

### 2.2 MySQL 血脉

```
MySQL (1995, MySQL AB)
│
├──► 主从复制 + InnoDB (2001) ── 成为互联网标配
│
├──► MySQL Cluster (NDB, 2003) ── 早期 Shared-Nothing 尝试
│
├──► MariaDB (2009) ── 开源分支
│
├──► Percona Server ── 性能增强分支
│
├──► Amazon Aurora (2015) ── 云原生 MySQL，存算分离
│    │
│    └──► 阿里云 PolarDB (2017) ── 存算分离架构，与 Aurora 设计理念相似
│
└──► TiDB (2015, PingCAP) ── MySQL 协议兼容，NewSQL 架构
```

### 2.3 Bigtable 血脉（列式 / 宽列）

```
Google Bigtable 论文 (2006)
│   核心贡献：排序的分布式映射、SSTable、Tablet 分裂、GFS 底层
│
├──► Apache HBase (2008, Apache)
│    │   开源 Bigtable 实现，HDFS 底层存储
│    │
│    └──► 阿里云 Lindorm ── 增强型宽列数据库
│
├──► Google Cloud Bigtable ── Bigtable 的云服务版本
│
├──► Apache Cassandra (2008, Facebook → Apache)
│    │   混合了 Bigtable 数据模型 + Dynamo 一致性模型
│    │
│    ├──► ScyllaDB (2015) ── C++ 重写，Seastar 框架
│    │
│    ├──► DataStax Enterprise ── 企业版 Cassandra
│    │
│    └──► 字节跳动 Bytable ── 内部 Bigtable-like 系统
│
└──► Apache Accumulo (2011, NSA)
     └── 在 HBase 基础上增加 cell 级安全
```

### 2.4 Dynamo 血脉（去中心化 KV / 最终一致）

```
Amazon Dynamo 论文 (2007)
│   核心贡献：一致性哈希、Vector Clocks、最终一致性、Quorum NWR
│
├──► Amazon DynamoDB (2012) ── 托管 KV 服务，单 digit ms 延迟
│    │   内部架构已大幅演进，不再完全遵循原论文
│    │
│    └──► DynamoDB Accelerator (DAX) ── 内存缓存层
│
├──► Riak (2010, Basho)
│    │   最忠实于 Dynamo 论文的开源实现
│    │
│    └──► Riak TS ── 时序数据专用版本
│
├──► Voldemort (2008, LinkedIn)
│    │   分布式 KV 存储，用于 LinkedIn 用户画像
│    │
│    └──► 影响：证明 Dynamo 架构可在非 Amazon 环境落地
│
└──► Apache Cassandra (2008)
     │   数据模型来自 Bigtable，一致性模型来自 Dynamo
     │   （因此是 "跨血脉杂交" 的典型）
     └──► (已在上方 Bigtable 血脉中标注)
```

### 2.5 Spanner 血脉（NewSQL / 全球分布式 SQL）

```
Google Spanner 论文 (2012)
│   核心贡献：TrueTime (原子钟+GPS)、外部一致性、Paxos 共识、
│             自动分片、同步复制
│
├──► CockroachDB (2014, Cockroach Labs)
│    │   "开源 Spanner"，Raft 替代 Paxos，时间戳替代 TrueTime
│    │   PostgreSQL 兼容层
│    │
│    ├──► CockroachDB Serverless ── 云托管版本
│    │
│    └──► YugabyteDB (2017) ── 与 CockroachDB 同源不同实现
│
├──► Google Cloud Spanner (2017)
│    │   Spanner 的云服务版本，对外商业化
│    │
│    └──► AlloyDB (2022, Google) ── 融合 Spanner 技术的 PostgreSQL 兼容层
│
├──► TiDB (2015, PingCAP)
│    │   Raft (via TiKV) + Percolator 分布式事务 + MySQL 兼容
│    │   不直接复制 Spanner 但解决同类问题
│    │
│    ├──► TiKV ── 分布式 KV 存储层 (Raft + RocksDB)
│    │
│    ├──► TiFlash ── 列式分析引擎 (HTAP)
│    │
│    └──► TiDB Cloud ── 托管版本
│
├──► YugabyteDB (2017)
│    │   Raft + PostgreSQL 兼容，DocDB 存储引擎
│    │
│    └──► YugaByte Cloud
│
├──► OceanBase (2010 开发, 2017 开源, 蚂蚁集团)
│    │   与 Spanner 几乎同期独立研发，Paxos 共识，金融级
│    │   TPC-C 世界纪录 (2019, 2020)
│    │
│    └──► OceanBase Cloud
│
└──► 华为 GaussDB (2019)
     │   分布式 SQL，MySQL/PG/Oracle 兼容
     └──► GaussDB (for openGauss)
```

### 2.6 NewSQL 与融合血脉

```
VoltDB (2010, Stonebraker 团队)
│   内存 ACID + 确定性执行 + 存储过程
│   └──► 证明 "单线程无锁 + 分片" 的可行性
│
FoundationDB (2013) ── Apple 2015 收购
│   有序 KV + ACID 事务层 + 分层架构
│   │
│   ├──► Apple 内部大量使用
│   │
│   └──► FoundationDB 开源 (2018) ── 影响后续分层架构设计
│
CockroachDB / TiDB / YugabyteDB
│   各自融合上述思想，形成 NewSQL 三大分支
│
TiDB 特别之处:
│   计算层 (TiDB, 无状态) + 事务层 (PD) + 存储层 (TiKV, Raft)
│   └── 严格分层架构受 FoundationDB 启发
```

### 2.7 完整谱系总图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    分布式数据库技术谱系总图                                │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─ 关系型源头 ────────────────────────────────────────────────────┐    │
│  │                                                                 │    │
│  │  System R (1970s) ─┬─► DB2 (1983)                             │    │
│  │  (关系模型 + SQL)   ├─► Oracle (1979)                          │    │
│  │                     └─► Ingres → PostgreSQL (1996) ─┬─► Greenplum │ │
│  │                                                     ├─► CockroachDB│ │
│  │                                                     └─► YugabyteDB │ │
│  │                                                                 │    │
│  │  MySQL (1995) ──┬─► MariaDB / Percona                          │    │
│  │                  ├─► Amazon Aurora (2015) ──► PolarDB (2017)    │    │
│  │                  └─► TiDB (2015) [协议兼容 + 全新架构]           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─ NoSQL 革命 ───────────────────────────────────────────────────┐    │
│  │                                                                 │    │
│  │  Bigtable 论文 (2006) ─┬─► HBase (2008) ──► Lindorm           │    │
│  │  (列族 + SSTable)      ├─► Cloud Bigtable                     │    │
│  │                        ├─► Cassandra (2008) ◄──┐              │    │
│  │                        └─► Accumulo (2011)     │              │    │
│  │                                                │ 混合          │    │
│  │  Dynamo 论文 (2007) ──┬─► DynamoDB (2012)     │              │    │
│  │  (一致性哈希 + VC)     ├─► Riak (2010)         │              │    │
│  │                        ├─► Voldemort (2008)    │              │    │
│  │                        └───────────────────────┘              │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─ NewSQL 时代 ──────────────────────────────────────────────────┐    │
│  │                                                                 │    │
│  │  Spanner 论文 (2012) ─┬─► Cloud Spanner (2017)                │    │
│  │  (TrueTime + Paxos)   ├─► CockroachDB (2014) ──► Serverless   │    │
│  │                       ├─► TiDB (2015) ──► TiKV / TiFlash      │    │
│  │                       ├─► YugabyteDB (2017)                   │    │
│  │                       ├─► OceanBase (2010 开发, 2017 开源)     │    │
│  │                       └─► GaussDB (2019)                      │    │
│  │                                                                 │    │
│  │  独立 NewSQL 路线:                                              │    │
│  │    VoltDB (2010) ── 内存确定性执行                             │    │
│  │    FoundationDB (2013) ── 分层架构 (Apple)                     │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─ 云原生数据库 ─────────────────────────────────────────────────┐    │
│  │                                                                 │    │
│  │  Aurora (2015) ── 存算分离，日志即数据库                        │    │
│  │     └─► PolarDB ── 中国对标                                    │    │
│  │     └─► TDSQL-C ── 腾讯对标                                    │    │
│  │     └─► ByteNDB/VeDB ── 字节对标                               │    │
│  │                                                                 │    │
│  │  Cloud Spanner (2017) ── 全球分布式 SQL 即服务                  │    │
│  │                                                                 │    │
│  │  融合趋势:                                                      │    │
│  │    云厂商 NewSQL 托管 (CockroachDB Cloud, TiDB Cloud)           │    │
│  │    HTAP (TiDB, SingleStore, OceanBase)                          │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 三、核心设计模式溯源

### 3.1 模式清单

| # | 设计模式 | 起源系统 | 传播路径 |
|---|---------|---------|---------|
| 1 | **关系模型 + SQL** | System R (1970s) | DB2, Oracle, PostgreSQL, MySQL → 所有现代 SQL 数据库 |
| 2 | **MVCC (多版本并发控制)** | Ingres / PostgreSQL | PostgreSQL, MySQL/InnoDB, Oracle, TiDB |
| 3 | **主从复制** | 早期 RDBMS | MySQL, PostgreSQL → Aurora (增强为共享存储日志) |
| 4 | **SSTable + LSM-Tree** | Bigtable (2006) | HBase, Cassandra, RocksDB → TiKV, FoundationDB |
| 5 | **Tablet/Region 分裂** | Bigtable (2006) | HBase, Bigtable → TiKV Region, Spanner Split |
| 6 | **一致性哈希** | Dynamo (2007) | Cassandra, Riak, Voldemort → 分布式缓存系统 |
| 7 | **Vector Clocks** | Dynamo (2007) | Riak, Cassandra (早期版本) → 后被 Hybrid Logical Clocks 替代 |
| 8 | **Quorum NWR** | Dynamo (2007) | Cassandra, Riak → 可配置一致性级别 |
| 9 | **Paxos 共识** | Spanner (2012) | OceanBase, Cloud Spanner → CockroachDB 改用 Raft |
| 10 | **Raft 共识** | Raft 论文 (2013) | etcd, Consul, TiKV, CockroachDB, YugabyteDB → 成为 NewSQL 标准 |
| 11 | **TrueTime / 外部一致性** | Spanner (2012) | Cloud Spanner → CockroachDB 用 HLC 模拟 |
| 12 | **Percolator 分布式事务** | Google Percolator (2010) | TiDB (通过 TiKV 实现) |
| 13 | **存算分离 (日志即数据库)** | Aurora (2015) | PolarDB, TDSQL-C, ByteNDB → 云原生数据库标准架构 |
| 14 | **分层事务架构** | FoundationDB (2013) | TiDB (TiKV + TiDB 分离) → 后续 NewSQL 参考 |
| 15 | **确定性执行** | VoltDB (2010) | 影响单分片内无锁设计思路 |
| 16 | **Shared-Storage 云架构** | Aurora (2015) | 几乎所有云厂商的关系型数据库服务 |

---

## 四、模式复用路径详解

### 4.1 复制与共识模式演进

```
        主从异步复制
        (MySQL, PostgreSQL)
              │
              ▼  需要更强一致性
        半同步复制 / 同步复制
        (MySQL Semi-Sync, PostgreSQL Synchronous)
              │
              ▼  需要自动故障切换
        Paxos / Raft 多副本共识
        (Spanner/Paxos, TiKV/Raft, CockroachDB/Raft)
              │
              ▼  需要跨地域低延迟
        Multi-Raft / Multi-Paxos 分片化共识
        (TiKV Multi-Raft, CockroachDB Range-level Raft)
              │
              ▼  需要更低延迟写入
        Pipeline Paxos / Fast Paxos
        (部分系统内部优化)
```

### 4.2 数据分片模式演进

```
        手动分库分表
        (早期互联网应用)
              │
              ▼  自动化需求
        一致性哈希 (自动分片)
        (Dynamo, Cassandra)
              │
              ▼  需要范围查询
        范围分片 (Range Sharding)
        (Bigtable Tablet, Spanner Split)
              │
              ▼  动态自适应
        自动分裂 + 负载均衡
        (TiKV Region, CockroachDB Range, Spanner Split)
              │
              ▼  精细化调度
        PD/调度器驱动的智能分裂
        (TiKV + PD, YugabyteDB Master)
```

### 4.3 一致性模型演进

```
        强一致 (ACID, 2PC)
        (传统 RDBMS)
              │
              ▼  互联网规模下不可行
        最终一致性
        (Dynamo, Cassandra)
              │
              ▼  开发者抱怨
        可调一致性 (Tunable Consistency)
        (Cassandra: ONE/QUORUM/ALL)
              │
              ▼  Spanner 突破
        外部一致性 + 全局时钟
        (Spanner TrueTime)
              │
              ▼  开源替代
        Hybrid Logical Clocks
        (CockroachDB, YugabyteDB)
              │
              ▼  实用折中
        快照隔离 + 乐观并发
        (TiDB, FoundationDB)
```

### 4.4 存储引擎模式演进

```
        B+Tree
        (传统 RDBMS: InnoDB, PostgreSQL)
              │
              ▼  写放大 + 大规模写入瓶颈
        LSM-Tree (SSTable)
        (Bigtable → RocksDB)
              │
              ├──► 纯 LSM: HBase, Cassandra, TiKV, FoundationDB
              │
              └──► B+Tree 优化: WiredTiger (MongoDB), MyRocks (MySQL)
```

### 4.5 云原生架构模式演进

```
        传统部署 (计算+存储同机)
        (MySQL, PostgreSQL 本地部署)
              │
              ▼  弹性需求
        存算分离 (Shared-Storage)
        (Amazon Aurora, 2015)
              │
              ├──► 日志即数据库 (Log is the Database)
              │    (Aurone, PolarDB, TDSQL-C)
              │
              └──► 存算共享 + 分布式存储
                   (PolarDB 共享存储架构)
              │
              ▼  全分布式
        Shared-Nothing 云原生
        (CockroachDB Cloud, TiDB Cloud, Cloud Spanner)
              │
              ▼  Serverless
        Serverless 数据库
        (Aurora Serverless, TiDB Serverless, Neon)
```

---

## 五、跨代影响分析

### 5.1 Bigtable × Dynamo 杂交：Cassandra

Cassandra 是分布式数据库史上最重要的"混血儿"：

- **数据模型** 继承 Bigtable：排序的列族、SSTable、MemTable + Flush
- **一致性模型** 继承 Dynamo：一致性哈希、Vector Clocks（早期）、Quorum NWR、最终一致性
- **额外创新**：去中心化的 Gossip 协议、无单点设计

这个杂交证明了**不同论文的设计模式可以组合**，开创了 NoSQL 时代最持久的分支。

### 5.2 Spanner 的"开源替代"竞赛

Spanner 论文发表后，开源社区出现了多个"开源 Spanner"项目，但选择了不同实现路径：

| 系统 | 共识 | 时钟 | SQL 兼容 | 特点 |
|------|------|------|---------|------|
| CockroachDB | Raft | HLC (混合逻辑时钟) | PostgreSQL | 最接近 Spanner 设计理念 |
| TiDB | Raft (TiKV) | Percolator TSO | MySQL | 分层架构，HTAP 能力 |
| YugabyteDB | Raft | HLC | PostgreSQL | DocDB 存储引擎 |
| OceanBase | Paxos | 原子钟+GPS (自研) | MySQL/Oracle | 与 Spanner 几乎同期独立研发 |

### 5.3 Aurora 模式的复制潮

Aurora 的 "Log is the Database" 架构引发了中国云厂商的集体跟进：

- **阿里云 PolarDB** (2017)：存算分离架构，基于共享分布式存储 (Pangu)，设计理念与 Aurora 相似
- **腾讯 TDSQL-C** (原 CynosDB, 2018)：存算分离架构，MySQL/PG 兼容
- **字节 ByteNDB/VeDB** (2020+)：Aurora-like 架构，适配字节内部场景

这一模式的核心洞察是：**将计算节点无状态化，存储层独立扩展**，从而实现秒级弹性伸缩。

### 5.4 分层架构的复兴

FoundationDB (2013) 开创的分层架构在 NewSQL 时代被广泛复用：

```
FoundationDB 分层模型:
  ┌─────────────────────┐
  │   SQL / API 层       │  ← 无状态，可水平扩展
  ├─────────────────────┤
  │   事务协调层          │  ← ACID 保证
  ├─────────────────────┤
  │   有序 KV 存储层      │  ← Raft/Paxos 共识
  └─────────────────────┘

TiDB 的分层 (受此启发):
  ┌─────────────────────┐
  │   TiDB (SQL 层)      │  ← 无状态，MySQL 协议
  ├─────────────────────┤
  │   PD (调度 + TSO)    │  ← 元数据 + 事务协调
  ├─────────────────────┤
  │   TiKV (KV 存储层)   │  ← Raft + RocksDB
  └─────────────────────┘
```

---

## 六、关键技术转折点

### 6.1 三大论文时代 (2003–2007)

| 论文 | 年份 | 解决的问题 | 开启的时代 |
|------|------|-----------|-----------|
| GFS | 2003 | 海量数据如何可靠存储 | 分布式文件系统 |
| MapReduce | 2004 | 海量数据如何并行计算 | 大数据计算 |
| Bigtable | 2006 | 结构化数据如何分布式存储 | 列式 NoSQL |
| Dynamo | 2007 | 高可用 KV 存储如何设计 | 去中心化 NoSQL |

这四篇论文定义了 2006–2012 年的 NoSQL 黄金时代。

### 6.2 Spanner 时刻 (2012)

Spanner 论文的出现标志着一个转折：**Google 证明了全球规模的强一致性是可行的**，前提是拥有精确的全局时钟（TrueTime）。

这直接引发了 NewSQL 运动——在 NoSQL 的扩展性和 RDBMS 的 ACID 之间找到平衡。

### 6.3 云原生时刻 (2015–2017)

Aurora 的发布标志着数据库架构从"Shared-Nothing"向"Shared-Storage"的转变。这个转变的驱动力是：

- 云厂商拥有可靠的分布式存储基础设施（EBS, Pangu）
- 计算节点无状态化带来极致的弹性
- 日志前移（Log-Ahead）消除了传统 RDBMS 的 redo/undo 双写开销

### 6.4 开源 NewSQL 成熟 (2018–present)

2018 年后，多个 NewSQL 系统进入成熟期：

- CockroachDB GA (2018)
- FoundationDB 开源 (2018)
- OceanBase 开源 (2021)
- YugabyteDB 2.0 (2020)
- TiDB 大规模生产验证 (2018+)

同时，HTAP 融合成为新方向：TiDB (TiFlash), SingleStore, OceanBase 都在尝试同时支持 OLTP 和 OLAP。

---

## 七、设计模式跨系统复用矩阵

| 设计模式 | Spanner | CockroachDB | TiDB | YugabyteDB | OceanBase | Cassandra | HBase | DynamoDB |
|---------|:-------:|:-----------:|:----:|:----------:|:---------:|:---------:|:-----:|:--------:|
| Raft | ✗ | ✓ | ✓ (TiKV) | ✓ | ✗ (Paxos) | ✗ | ✗ | ✗ |
| Paxos | ✓ | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ |
| 全局时钟 | TrueTime | HLC | TSO | HLC | 自研时钟 | ✗ | ✗ | ✗ |
| 范围分片 | ✓ | ✓ | ✓ (Region) | ✓ | ✓ | ✗ (哈希) | ✓ (Tablet) | ✗ |
| MVCC | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ | ✗ |
| LSM-Tree | ✓ | ✓ | ✓ (RocksDB) | ✓ (RocksDB) | ✓ | ✓ | ✓ (HFile) | ✓ |
| 多副本强一致 | ✓ | ✓ | ✓ | ✓ | ✓ | 可调 | 可调 | 可调 |
| SQL 兼容 | 自研 | PostgreSQL | MySQL | PostgreSQL | MySQL/Oracle | CQL | 有限 | 有限 |
| 分层架构 | ✗ | ✗ | ✓ | 部分 | ✗ | ✗ | ✗ | ✗ |
| 存算分离 | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |

---

## 八、当前趋势与未来方向

### 8.1 融合趋势

- **HTAP**：TiDB、OceanBase、SingleStore 尝试在单一系统中同时支持事务和分析
- **Serverless 数据库**：Aurora Serverless、TiDB Serverless、Neon 推动按需计费模式
- **多模型数据库**：Lindorm (宽列 + 时序 + 搜索)、DocumentDB (文档 + 图)

### 8.2 硬件驱动的架构变化

- **NVMe/SCM**：DAOS、3FS 展示字节级存储如何改变数据库 I/O 路径
- **RDMA**：降低节点间通信延迟，使分布式共识更高效
- **CXL**：内存池化可能改变存算分离的成本模型

### 8.3 AI 时代的影响

- **向量数据库**：Milvus、Pinecone、Weaviate 为 LLM 应用提供语义搜索
- **AI 训练数据管道**：JuiceFS、3FS 针对 AI 训练的吞吐优化
- **AI 辅助查询优化**：ML-driven query optimization 成为 RDBMS 新方向

---

## 九、参考资料

### 核心论文

1. **System R**: Astrahan et al., "System R: Relational Approach to Database Management", ACM TODS, 1976.
2. **Bigtable**: Chang et al., "Bigtable: A Distributed Storage System for Structured Data", OSDI 2006.
3. **Dynamo**: DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store", SOSP 2007.
4. **Spanner**: Corbett et al., "Spanner: Google's Globally-Distributed Database", OSDI 2012.
5. **Percolator**: Peng & Dabek, "Large-scale Incremental Processing Using Distributed Transactions and Notifications", OSDI 2010.
6. **Raft**: Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm", USENIX ATC 2014.
7. **FoundationDB**: Zhou et al., "FoundationDB: A Distributed Key-Value Store", SIGMOD 2021.

### 技术博客与文档

- Amazon Aurora Architecture Overview (AWS re:Invent)
- CockroachDB Architecture Documentation
- TiDB Architecture Documentation (PingCAP)
- Google Cloud Spanner Architecture
- OceanBase 技术白皮书

---

*文档版本: 1.0 | 最后更新: 2026-06-11 | 遵循 architect 技能 5 维分析框架*
