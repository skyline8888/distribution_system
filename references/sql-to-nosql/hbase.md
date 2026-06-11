# Apache HBase

## 基本信息

| 维度 | 内容 |
|------|------|
| **发布时间** | 2008 年（Google Bigtable 论文 2006） |
| **开发方** | Apache Software Foundation（PowerSet 工程师 Jim Kellerman 发起，Powerset → Microsoft） |
| **开源协议** | Apache License 2.0 |
| **核心论文** | [Bigtable: A Distributed Storage System for Structured Data](https://research.google/pubs/pub27898/) (Google, OSDI 2006) |
| **定位** | Bigtable 的开源实现，Hadoop 生态的分布式宽列存储 |

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

2006 年 Google 发表 Bigtable 论文，描述了一个面向结构化数据的分布式存储系统：稀疏排序映射（sorted map）、列族组织、SSTable 存储、GFS 持久化。但 Bigtable 是闭源内部系统，开源社区没有等价物。

HBase 的诞生动机很直接：**把 Bigtable 的设计搬到开源 Hadoop 生态**。HDFS 已有 GFS 的开源实现，但缺少 Bigtable 层的结构化存储。HBase 填补了这个空白——为海量数据（亿级行、TB/PB 级）提供随机实时读写能力，这是 Hadoop MapReduce 批处理无法做到的。

### 前代系统局限

- **HDFS**：只能追加写入，不支持随机读写、单行级别操作。
- **传统 RDBMS**：在十亿行级别分库分表极其复杂，水平扩展困难。
- **MySQL 分片**：需要应用层手动路由、一致性维护困难。

### 硬件/网络背景

- 2008 年：廉价 x86 服务器集群 + 1Gbps 以太网成为数据中心标配
- Hadoop 生态已成熟（HDFS + MapReduce），但缺少在线读写层
- ZooKeeper（Yahoo 2007 开源）提供了可靠的分布式协调基础

---

## 2. Architecture（架构设计）

### 系统拓扑

HBase 采用 **Master-Slave 分层架构**：

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                                │
│  (HBase Java API / Thrift / REST / Phoenix SQL)             │
└──────────┬──────────────────────────┬───────────────────────┘
           │                          │
           ▼                          ▼
┌──────────────────┐      ┌──────────────────────────────────┐
│   ZooKeeper      │      │          HMaster                  │
│  (Coordination)  │◄────┤  • Region 分配/负载均衡            │
│  • Master 选举   │      │  • DDL (create/alter/drop table)  │
│  • Region 注册   │      │  • Schema 管理                    │
│  • -ROOT- /      │      │  • 不转发数据请求 (故障时可无Master)│
│    meta. 位置    │      └────────────────┬─────────────────┘
└──────────────────┘                       │ (RPC)
                                           ▼
              ┌────────────────────────────────────────────────┐
              │             Region Servers (多台)               │
              │                                                │
              │  ┌────────────────┐  ┌────────────────────┐   │
              │  │   Region A     │  │     Region B       │   │
              │  │ (row 001-100)  │  │ (row 101-200)      │   │
              │  │                │  │                    │   │
              │  │  ┌──────────┐  │  │  ┌──────────────┐  │   │
              │  │  │ MemStore │  │  │  │  MemStore    │  │   │
              │  │  │ (写缓冲)  │  │  │  │  (写缓冲)     │  │   │
              │  │  └────┬─────┘  │  │  └──────┬───────┘  │   │
              │  │       │ Flush │  │         │ Flush    │   │
              │  │  ┌────▼─────┐  │  │  ┌──────▼───────┐  │   │
              │  │  │ HFile #1  │  │  │  │  HFile #1    │  │   │
              │  │  │ HFile #2  │  │  │  │  HFile #2    │  │   │
              │  │  └──────────┘  │  │  └──────────────┘  │   │
              │  │                │  │                    │   │
              │  │  ┌──────────┐  │  │  ┌──────────────┐  │   │
              │  │  │   WAL    │  │  │  │    WAL       │  │   │
              │  │  │ (HLog)   │  │  │  │   (HLog)     │  │   │
              │  │  └──────────┘  │  │  └──────────────┘  │   │
              │  └────────────────┘  └────────────────────┘   │
              │                                                │
              └────────────────────────┬───────────────────────┘
                                       │
                                       ▼
              ┌────────────────────────────────────────────────┐
              │                    HDFS                         │
              │  (NameNode + DataNodes, 3 副本)                 │
              │  HFile 持久化存储于此                            │
              └────────────────────────────────────────────────┘
```

### 核心组件

| 组件 | 角色 | 说明 |
|------|------|------|
| **HMaster** | 管理节点 | 负责 Region 分配、负载均衡、DDL 操作。**不转发数据面请求**，所以 HMaster 挂掉不影响已运行的读写（只要不触发 Region 重新分配） |
| **RegionServer** | 工作节点 | 托管 Regions，处理所有读写请求。每个 RegionServer 管理多个 Region |
| **ZooKeeper** | 协调服务 | Master 选举、RegionServer 存活检测、`hbase:meta` Region 位置存储、集群状态维护 |
| **Region** | 数据分片 | 一个连续 rowkey 范围的行集合，是 HBase 的最小分布单元 |

### 数据模型与命名空间

HBase 的数据模型是一个**稀疏的、排序的、多维映射**：

```
Table
  └── Row (由 RowKey 唯一标识, 按字典序排序)
       └── Column Family (建表时定义, 通常 1-3 个)
            └── Column Qualifier (动态添加, 无需预定义)
                 └── Cell (Value + Timestamp)
                      └── 可存储多个版本 (由 Timestamp 区分)
```

关键特征：
- **RowKey**：字节数组，按字典序排序——设计 RowKey 是 HBase 性能调优的核心
- **Column Family**：物理存储单元，建表时必须定义；同一 CF 的列存储在同一个 HFile 中
- **Column Qualifier**：动态列，无需预定义，可随数据自由扩展
- **Cell**：由 (RowKey, ColumnFamily, ColumnQualifier, Timestamp) 四维唯一确定
- **Timestamp**：每个 cell 自带版本戳，默认取写入时间；可保留多个历史版本

### 元数据架构归类：**集中式 + 分布式协调**

HBase 的元数据架构融合了两种模式：

1. **集中式**：HMaster 是集群的集中式管理节点，负责 Region 分配和 DDL
2. **算法/计算式 + 分布式协调**：ZooKeeper 用于 Master 选举和 `hbase:meta` 位置发现，通过 ZNode 路径实现去中心化的元数据查找

元数据查找路径（三层导航）：
```
ZooKeeper → hbase:meta Region 位置 → 目标 Region 所在 RegionServer
```

客户端首次访问时走完整路径，之后**缓存** RegionServer 地址，直接访问。

### 数据架构分析

#### 分块方式：层次化 Region 分裂

- **Region** 是最小分布单元，初始时一个 Table 只有一个 Region
- 当 Region 大小超过阈值（默认 10GB），自动**分裂**为两个子 Region
- 分裂是"半开"的：分裂后的两个 Region 各自服务一半的 rowkey 范围
- HMaster 将新 Region 重新分配到不同 RegionServer 实现负载均衡

#### 分布策略：范围分片（Range Partitioning）

- Region 按 **RowKey 字典序范围**划分
- `hbase:meta` 表记录了所有 Region 的 rowkey 范围 → RegionServer 映射
- 客户端根据目标 RowKey 二分查找 `hbase:meta` 定位 Region

#### 冗余方式：HDFS 副本复制

- 数据持久化在 HDFS 上，默认 **3 副本**
- Region 级别无额外复制——HDFS 副本提供底层容错

#### I/O 路径（写路径与读路径）

**写路径 (Write Path)：**
```
Client → RegionServer
  → 1. 写 WAL (HLog) → HDFS (持久化保障)
  → 2. 写 MemStore → 内存 (LSM 树的 active 层)
  → 返回 ACK
  → (后台) MemStore 满时 Flush → 生成 HFile → 写入 HDFS
```

**读路径 (Read Path)：**
```
Client → RegionServer
  → 1. 查 BlockCache (读缓存, LRU)
  → 2. 查 MemStore (内存)
  → 3. 查 HFile (磁盘, 使用 Bloom Filter 快速定位)
  → 合并结果 (最新 Timestamp 优先)
```

#### 存储引擎：LSM-Tree 变体

```
                    ┌─────────────┐
                    │   MemStore  │ ← 写入目标 (内存)
                    │  (Sorted)   │
                    └──────┬──────┘
                           │ Flush (顺序写入)
                    ┌──────▼──────┐
                    │   HFile 1   │ ← 不可变, 有序 (SSTable-like)
                    │   (L0)      │
                    └─────────────┘
                           │
                    ┌──────▼──────┐
                    │   HFile 2   │ ← 不可变, 有序
                    │   (L0)      │
                    └─────────────┘
                           │
                    ┌──────▼──────┐
                    │  Compaction │ ← Minor/Major 合并
                    └─────────────┘
```

#### 生命周期管理

- **Flush**：MemStore → HFile（内存满或达到阈值触发）
- **Minor Compaction**：选取少量小 HFile 合并，减少读时文件数
- **Major Compaction**：合并所有 HFile 为一个，清理过期/删除数据（TTL）
- **TTL**：可为 Column Family 设置存活时间，过期数据在 Major Compaction 时物理删除

---

## 3. Core Technical Innovations（核心技术创新）

### 一致性模型：**强一致性（线性可读）在 Region 级别**

- **同一 RowKey 的所有操作是原子的**：单行事务性
- 写操作先写 WAL 再写 MemStore，确保写持久化
- 读操作遵循"Read-Your-Writes"：写入后可立即读到
- 不同 Row 之间**无跨行事务**——这是与 RDBMS 的核心差异
- 跨 Region 的操作不保证原子性（除非使用 HBase 的多行事务扩展，如 Omid）

### WAL (Write-Ahead Log / HLog)

每个 RegionServer 有一个共享的 WAL（HLog），所有写入该 RegionServer 上任意 Region 的数据都先追加到 WAL，然后才写入 MemStore。WAL 存储在 HDFS 上（多副本）。

**恢复机制**：RegionServer 崩溃时，HMaster 检测到 ZNode 超时，将故障 RS 的 Regions 重新分配。新 RS 先回放 WAL 恢复 MemStore，然后继续服务。

### HFile：SSTable-like 存储格式

HFile 是 HBase 的底层存储格式，借鉴自 Bigtable 的 SSTable：

```
HFile Structure:
┌──────────────────────────────────┐
│  Data Block (Key-Value pairs)    │ ← 压缩存储, 可配置压缩算法
│  Data Block                      │
│  ...                             │
├──────────────────────────────────┤
│  File Info (metadata)            │ ← 压缩方式、Comparator 等
├──────────────────────────────────┤
│  Data Index (block offsets)      │ ← 加速随机读
│  Meta Index                      │
├──────────────────────────────────┤
│  Bloom Filter (可选)             │ ← 快速判断 rowkey 是否存在
├──────────────────────────────────┤
│  Trailer (fixed size)            │ ← 指向索引和元数据的指针
└──────────────────────────────────┘
```

关键设计：
- **不可变**：写入后不再修改，只有 Compaction 时重写
- **有序**：按 RowKey 字典序排列
- **压缩**：支持 Snappy、LZO、GZ 等
- **Bloom Filter**：快速判断行是否存在，减少不必要的磁盘 I/O

### MemStore + Flush

- MemStore 是内存中的有序缓冲区（ConcurrentSkipListMap）
- 每个 Region 的每个 Column Family 有一个独立的 MemStore
- 当 MemStore 达到阈值（默认 128MB），触发 Flush：
  - 先写一个 WAL marker（标记此点之前全部持久化）
  - 将 MemStore 内容排序后顺序写入新的 HFile
  - 删除旧的 WAL 条目

### Compaction 机制

HBase 有两种 Compaction：

| 类型 | 行为 | 影响 |
|------|------|------|
| **Minor Compaction** | 选取少量相邻小 HFile 合并为较大的 | 减少读时需扫描的文件数；不处理删除标记 |
| **Major Compaction** | 合并 Region 的所有 HFile 为一个 | 清理删除标记和过期版本、去重、重建 Bloom Filter；I/O 开销大 |

Compaction 是 HBase 性能调优的关键——过度 Compaction 浪费 I/O，不足 Compaction 导致读放大。

### Coprocessor 机制

从 HBase 0.92 开始引入 Coprocessor，允许在 RegionServer 上执行**服务端自定义逻辑**：

- **Observer Coprocessor**：类似触发器，在 Get/Put/Delete/Scan 前后执行
- **Endpoint Coprocessor**：类似存储过程，暴露自定义 RPC 端点
- **聚合操作**（COUNT/SUM/MAX）可直接在 RegionServer 端执行，减少网络传输

### 性能优化技术

- **BlockCache**：基于 LRU 的读缓存，默认堆内存的 40%
- **Bloom Filter**：行级/行+列级，减少不必要的 HFile 访问
- **预分区 (Pre-Splitting)**：建表时预设 Region 边界，避免热点和频繁分裂
- **RowKey 设计**：加盐 (salting)、反转 (reversing)、哈希前缀等打散热点

### 扩展方式：**Scale-Out**

- 通过增加 RegionServer 线性扩展吞吐
- 通过 Region 分裂自动应对数据增长
- HDFS 层通过增加 DataNode 扩展存储容量

---

## 4. Trade-offs (CAP / PACELC)

### CAP 定位：**CP 系统**

| 维度 | 选择 | 说明 |
|------|------|------|
| **Consistency** | ✅ | 同一 Region 内的读写强一致；WAL 保证持久化 |
| **Availability** | ❌（分区时） | RegionServer 故障后，其上的 Region 不可用直到被重新分配 |
| **Partition Tolerance** | ✅ | 依赖 HDFS 副本和 ZooKeeper 集群容忍节点故障 |

**为什么是 CP 而非 AP？**

- RegionServer 崩溃后，其 Region **立即不可服务**，直到 HMaster 通过 ZooKeeper 检测到故障并重新分配（通常 1-3 分钟）
- 在此期间，对该 Region 的读写请求被拒绝或排队
- 这与 AP 系统（如 Cassandra）形成对比——Cassandra 允许在节点故障时读写（可能读到旧数据）

### PACELC 定位

| 场景 | 选择 | 说明 |
|------|------|------|
| **Partition 时** | **C** (Consistency) | 牺牲可用性，保证一致性——Region 重新分配期间不可服务 |
| **Else (正常时)** | **L** (Latency) 还是 **E** (Efficiency)? | 倾向于 **E**：通过 LSM-Tree 的批量写、后台 Compaction、HDFS 批量 I/O 优化吞吐量而非极致低延迟 |

简写：**CP-EL**（分区时选一致性，正常时选效率）

### 复杂度 vs 运维

| 维度 | 评价 |
|------|------|
| **部署复杂度** | 高——依赖 HDFS + ZooKeeper + RegionServer 多层组件 |
| **运维复杂度** | 中高——Compaction 调优、Region 均衡、GC 调优（MemStore 堆内存） |
| **故障排查** | 复杂——需要理解 Region 分裂、Compaction、WAL 回放等多层机制 |

### 成本 vs 性能

| 优势 | 劣势 |
|------|------|
| PB 级数据水平扩展 | 小数据量场景开销大（HDFS 三副本 + ZooKeeper 集群） |
| 随机读写优于 HDFS | 延迟通常 10-100ms，不如 Redis 等内存存储 |
| 与 Hadoop 生态深度集成 | 强依赖 Hadoop 生态，独立部署成本不低 |

### 与同代系统的 CAP 对比

```
CAP 三角:

         Consistency
            ▲
            │
     HBase  │  Spanner
    (CP)    │
            │
            │
Availability┼───────────Partition Tolerance
            │
     Dynamo │  Cassandra
    (AP)    │
```

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统

| 系统 | 关系 | 说明 |
|------|------|------|
| **Apache Phoenix** | SQL on HBase | 在 HBase 之上提供 SQL 层、二级索引 |
| **Apache Omid** | 事务扩展 | 为 HBase 提供多行 ACID 事务 |
| **Facebook CorL** | 内部变体 | Facebook 基于 HBase 构建的内部系统 |
| **Apache Cassandra** | 思想竞争 | 同样受 Bigtable 启发，但走去中心化路线 |
| **TiDB (TiKV)** | 演进替代 | 同样 range partitioning + LSM，但用 Raft 替代 HDFS |
| **Lindorm (阿里云)** | 商业演进 | 兼容 HBase API 的云原生多模数据库 |

### 成为标准的设计模式

1. **LSM-Tree 存储引擎**：MemStore → HFile → Compaction 的写路径成为 NoSQL 标准模式，后被 RocksDB、Cassandra、TiKV 等广泛采用
2. **Region/Range Partitioning**：按排序键范围分片的模式，后续被 TiKV、Spanner 等采用
3. **三层元数据导航**（ZK → Meta → Region）：多级元数据发现模式
4. **Coprocessor**：服务端自定义计算的思想，影响了后续数据库的用户定义函数设计
5. **WAL + Flush + Compaction** 的持久化策略：成为 LSM 系统的标准范式

### 技术谱系位置

```
Bigtable (Google, 2006)
    │
    ├── 闭源/内部 ──→ Google Cloud Bigtable (2015, 托管服务)
    │
    └── 开源实现 ──→ Apache HBase (2008)
                        │
                        ├── Apache Phoenix (SQL 层)
                        ├── Apache Omid (事务)
                        └── 云厂商 HBase 服务 (阿里云 Lindorm, 腾讯云 CHBase)

Bigtable (Google, 2006)
    │
    └── 思想融合 ──→ Apache Cassandra (2008)
                        Bigtable 的数据模型 + Dynamo 的去中心化
```

### 当前相关性和使用场景

**依然活跃的场景：**
- 大数据平台的历史数据存储（日志、时序、事件流）
- Hadoop 生态中的在线查询层
- 消息存储（如 Apache Kafka 的早期替代方案）
- 国内互联网公司的用户画像/标签系统

**面临的挑战：**
- 云原生时代，对象存储 + 计算分离架构兴起（如 Snowflake、BigQuery）
- TiDB/CockroachDB 等 NewSQL 系统在需要 SQL + 水平扩展的场景中替代 HBase
- RocksDB 作为嵌入式 LSM 引擎在单机场景取代 HBase 的部分用途
- HBase 本身也在演进：HBase 2.0+ 引入离线数据导入、异步 replication 等改进

### 历史地位总结

HBase 是 **Bigtable 思想在开源世界的第一个完整实现**。它把 Google 论文中的抽象设计落地为可运行的工程系统，证明了列式宽表模型 + LSM-Tree + HDFS 的可行性。虽然云原生时代出现了更轻量、更易运维的替代方案，但 HBase 奠定的设计模式——Region 分裂、MemStore+HFile、WAL 恢复、Compaction 策略——已成为现代分布式存储系统的通用语言。

理解 HBase，就是理解 LSM-Tree 在分布式场景下如何工作。

---

## 参考资料

1. Google Bigtable Paper: *Bigtable: A Distributed Storage System for Structured Data* (OSDI 2006)
2. Apache HBase Reference Guide: https://hbase.apache.org/book.html
3. HBase 架构分析: *HBase: The Definitive Guide* (O'Reilly, 2011)
4. Lars George: *HBase in Action* (Manning, 2012)
5. HBase Source Code: https://github.com/apache/hbase
