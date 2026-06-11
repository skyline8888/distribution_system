# ClickHouse

## 基本信息
- **发布时间:** 2016 年 6 月（开源发布），内部研发始于 2009 年
- **开发方:** Yandex（俄罗斯搜索引擎巨头），后由 ClickHouse Inc. 独立运营
- **开源协议:** Apache 2.0
- **核心论文/文档:** 无传统学术论文，但以 Yandex.Metrica 官方博客和文档为核心参考资料
- **语言:** C++
- **许可证:** Apache 2.0

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

Yandex 拥有全球最大的 Web 分析平台之一 **Yandex.Metrica**（对标 Google Analytics），每天处理数十亿级事件。2009 年前后，他们面临一个核心痛点：

> **如何对海量事件数据进行亚秒级实时交互式分析？**

当时的 OLAP 方案存在明显不足：
- **传统 MPP（Teradata、Greenplum）：** 按列存储但基于行式执行引擎，无法充分利用 CPU 向量能力；扩展成本高
- **Hadoop/Hive：** 批处理延迟高（分钟到小时级），无法支持交互式查询
- **Druid/Impala：** 要么侧重实时性牺牲灵活性，要么依赖 Hadoop 生态

### 前代系统的局限性

| 系统 | 局限 |
|------|------|
| Hive/MapReduce | 批处理范式，延迟过高 |
| MySQL/PostgreSQL | 行式存储，列查询需全表扫描 |
| MonetDB | 列存但面向单机，缺乏分布式能力 |
| InfiniDB | 列存但扩展性和性能不足 |

### 硬件与网络背景

- **多核 CPU 普及：** SIMD 指令集（SSE/AVX）成为标配，但多数数据库未充分利用
- **SSD 出现：** 顺序读带宽大幅提升，列式存储的收益被放大
- **10GbE 网络：** 节点间数据传输不再是瓶颈，分布式架构可行

### 设计哲学

ClickHouse 的设计哲学非常明确：

> **OLAP 场景专用，不为 OLTP 妥协。** 不支持高频点查、不支持行级 UPDATE/DELETE、不支持事务。一切设计决策服务于一个目标：在宽表上做极高速度的聚合分析。

---

## 2. Architecture（架构设计）

### 系统拓扑

ClickHouse 本质上是一个 **Shared-Nothing 架构**的分布式列式数据库。每个节点独立存储和处理数据，通过分布式表协调查询。

```
┌─────────────────────────────────────────────────────────┐
│                    ClickHouse Cluster                    │
│                                                          │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   Shard 1   │    │   Shard 2   │    │   Shard N   │  │
│  │             │    │             │    │             │  │
│  │ ┌─────────┐ │    │ ┌─────────┐ │    │ ┌─────────┐ │  │
│  │ │ Replica │ │    │ │ Replica │ │    │ │ Replica │ │  │
│  │ │   A     │ │    │ │   A     │ │    │ │   A     │ │  │
│  │ └─────────┘ │    │ └─────────┘ │    │ └─────────┘ │  │
│  │ ┌─────────┐ │    │ ┌─────────┐ │    │ ┌─────────┐ │  │
│  │ │ Replica │ │    │ │ Replica │ │    │ │ Replica │ │  │
│  │ │   B     │ │    │ │   B     │ │    │ │   B     │ │  │
│  │ └─────────┘ │    │ └─────────┘ │    │ └─────────┘ │  │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘  │
│         │                  │                  │          │
│         └──────────────────┼──────────────────┘          │
│                            │                             │
│              ┌─────────────▼─────────────┐               │
│              │   ZooKeeper / Keeper      │               │
│              │  (Replication Coordination│               │
│              │   + Distributed DDL)      │               │
│              └───────────────────────────┘               │
└─────────────────────────────────────────────────────────┘
         │
         │ SQL (HTTP / Native TCP)
         ▼
┌─────────────────────┐
│   Client / BI Tool  │
│   (SELECT query)    │
└─────────────────────┘
```

### 内部存储架构（单机视角）

```
┌──────────────────────────────────────────────────┐
│                   ClickHouse Server               │
│                                                    │
│  ┌────────────────────────────────────────────┐   │
│  │              SQL Parser & Planner           │   │
│  └──────────────────┬─────────────────────────┘   │
│                     │                              │
│  ┌──────────────────▼─────────────────────────┐   │
│  │        Expression & Aggregation            │   │
│  │        Pipeline (Query Plan)               │   │
│  └──────────────────┬─────────────────────────┘   │
│                     │                              │
│  ┌──────────────────▼─────────────────────────┐   │
│  │     Vectorized Query Execution Engine       │   │
│  │     (SIMD: AVX2/AVX-512, batch processing)  │   │
│  └──────────────────┬─────────────────────────┘   │
│                     │                              │
│  ┌──────────────────▼─────────────────────────┐   │
│  │            MergeTree Storage               │   │
│  │                                             │   │
│  │  ┌─────────────────────────────────────┐   │   │
│  │  │        Table / Partition             │   │   │
│  │  │                                      │   │   │
│  │  │  ┌────┐ ┌────┐ ┌────┐ ┌────┐        │   │   │
│  │  │  │Part│ │Part│ │Part│ │Part│ ...    │   │   │
│  │  │  │ 1  │ │ 2  │ │ 3  │ │ N  │        │   │   │
│  │  │  └────┘ └────┘ └────┘ └────┘        │   │   │
│  │  │   ↑        ↑        ↑                │   │   │
│  │  │   │ Sparse Index (Primary Key)       │   │   │
│  │  │   │ Data Skipping Indexes            │   │   │
│  │  │   │ Projections (Materialized)       │   │   │
│  │  └─────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────┘   │
│                     │                              │
│  ┌──────────────────▼─────────────────────────┐   │
│  │              Disk Storage                   │   │
│  │  (Column files .bin, marks.mrk, minmax.idx) │   │
│  └─────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────┘
```

### 数据模型与命名空间

```
Database
  └── Table (ENGINE = MergeTree family)
        ├── Partition (按 PARTITION BY 表达式划分)
        │     └── Data Part (immutable, generated per INSERT)
        │           ├── Column files (*.bin) — 每个列独立文件
        │           ├── Primary key index (sparse, marks file)
        │           ├── Data skipping indexes (可选)
        │           └── Metadata (checksums, columns, primary key)
        └── Primary Key Index (稀疏索引, 每 8192 行一个 mark)
```

### 元数据架构归类

**集中式（单节点元数据）** — 单机模式下元数据完全存在于本地文件系统；集群模式下通过 ZooKeeper/ClickHouse Keeper 协调副本和分布式 DDL。

ClickHouse 不维护全局统一的元数据服务。每个节点管理自己的本地元数据（表定义、parts 信息），集群级元数据（副本同步状态、分布式 DDL 队列）依赖 ZooKeeper。这是一种 **混合模式：本地集中 + 集群依赖外部协调**。

### 数据架构分析

#### 分块方式（Data Chunking）

- **最小单元：Data Part（数据分片/片段）**
  - 每次 `INSERT` 生成一个新的 Part（在 MergeTree 中）
  - Part 是 **不可变的（immutable）**，写入后即冻结
  - Part 大小由 `min_bytes_for_wide_part` 等参数控制
  - Parts 按 Partition 分组，Partition 由 `PARTITION BY` 表达式定义（常用 `toYYYYMM(date)`）

#### 列式存储格式

```
Part 目录结构示例:
202401_1_5_2/          ← Part 名称 (partition_minBlock_maxBlock_level)
  checksums.txt        ← 校验和
  columns.txt          ← 列定义
  count.txt            ← 行数
  primary.idx          ← 稀疏主键索引
  date.bin             ← date 列数据 (列压缩)
  date.mrk2            ← date 列 mark 位置
  user_id.bin          ← user_id 列数据
  user_id.mrk2
  event_count.bin
  event_count.mrk2
  ...
```

每个 `.bin` 文件包含一个列的所有值，使用适合该类型的编码（LowCardinality、Delta、Gorilla、LZ4/ZSTD 压缩）。

#### 稀疏索引（Sparse Primary Key Index）

```
主键索引 = 每 8192 行（granule）记录主键值

Index marks:
  mark 0  → page 1,   primary_key_value = "Alice"
  mark 1  → page 42,  primary_key_value = "Charlie"
  mark 2  → page 89,  primary_key_value = "Eve"
  ...

查询 WHERE pk BETWEEN "Bob" AND "David":
  → 二分查找 index → 命中 mark 1 ~ mark 2
  → 只读 page 42 ~ page 89 的数据
  → 跳过 mark 0 (Alice) 及之后的无关 parts
```

**关键设计：** 索引本身很小（只存主键的 granule 边界值），常驻内存。索引不指向精确行，而是指向一个 granule 范围。

#### 数据分布（Data Distribution）— 分布式层

- **Sharding（分片）：** 通过 `Distributed` 表引擎 + `sharding_key` 实现
  - INSERT 时按 `sharding_key` 的 hash 路由到目标 shard
  - SELECT 时在 Distributed 表下推查询到所有 shard，结果在发起节点合并
- **Replication（副本）：** 通过 `ReplicatedMergeTree` 系列引擎实现
  - 基于 ZooKeeper/Keeper 的日志复制
  - 每个 Part 的创建/删除/合并操作通过 ZK 协调

#### 数据冗余

- **ReplicatedMergeTree:** 多副本同步，通过 ZK 的 log 队列协调
  - Leader 写入 → 写入本地 part → 在 ZK 创建 log 条目 → 其他副本从 ZK 拉取并 replay
- **S3/Disk 级冗余:** 支持将 parts 存储在 S3 兼容对象存储上（S3 引擎）

#### I/O 路径

- 直接文件系统 I/O（Linux: `O_DIRECT` 可选）
- 列级按需读取（只读取查询涉及的列）
- 顺序读优化（Part 内数据按主键排序，range scan 极高效）

#### 数据生命周期管理

```
INSERT → New Part (wide part)
              │
              ▼
    Background Merge Thread
    (合并多个小 parts → 大 part)
              │
              ▼
    Old parts marked for deletion
              │
              ▼
    TTL cleanup (可选按时间自动清理)
```

- **TTL 支持：** 按时间自动过期删除数据
- **Mutation：** `ALTER TABLE ... DELETE/UPDATE` 是异步的（实际是重写 parts）
- **Projection：** 预计算的排序/聚合投影，类似物化索引

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 列式存储 + 稀疏索引

ClickHouse 的核心存储引擎是 **MergeTree** 家族。每个 Part 内部：

1. **列独立存储：** 每个列单独存放在 `.bin` 文件中，查询只读需要的列
2. **主键排序：** Part 内数据按 `ORDER BY` 表达式排序，使得 range scan 可以跳过大量无关数据
3. **稀疏索引：** 每 8192 行（`index_granularity`）一个 mark，索引体积通常是数据的 0.1%~1%
4. **数据跳过索引（Data Skipping Indexes）：**
   - `minmax` — 存储值的 min/max，类似分区裁剪
   - `set` — 存储唯一值集合
   - `bloom_filter` — 概率性过滤
   - `tokenbf_v1 / ngrambf_v1` — 文本搜索

```
查询优化路径:
SELECT * WHERE date = '2024-01-01' AND user_id = 42

1. Partition pruning   → 只查 202401 partition
2. Sparse index lookup → 只读 user_id 命中范围的 granules
3. Skipping index      → bloom_filter 排除不含 42 的 granules
4. Column read         → 只读 SELECT 指定的列
5. Decompress in-memory→ LZ4/ZSTD 解压到内存

→ 从 TB 级数据中只读 MB 级
```

### 3.2 MergeTree 日志结构存储

MergeTree 的存储模型借鉴了 LSM-Tree 的思想，但做了 OLAP 场景的适配：

| 特性 | LSM-Tree (KV) | MergeTree (OLAP) |
|------|---------------|-------------------|
| 写入单元 | SSTable | Data Part |
| 不可变 | ✅ | ✅ |
| 合并 | Compaction | Merge |
| 排序 | Key | Primary Key (ORDER BY) |
| 粒度 | KV 记录 | 列式 granule (8192 行) |
| 目的 | 写优化 | 读优化 + 写可接受 |

**Merge 过程：**
- 后台线程持续合并小 Parts → 大 Parts
- 合并时重新排序（保持 ORDER BY 有序）
- 合并后旧 Parts 标记删除，异步清理
- Merge 策略可配置（`merge_selector`）

### 3.3 向量化查询执行（SIMD）

这是 ClickHouse 性能的核心秘密武器。

```
传统执行模型（一行一行处理）:
  for each row:
    result = col_a[row] + col_b[row]
    if result > threshold:
      output(row)

向量化执行模型（批量处理，SIMD）:
  for each batch (8192 rows):
    // AVX-512: 一次处理 16 个 double (512-bit / 64-bit)
    vec_result = _mm512_add_pd(vec_a, vec_b)
    vec_mask   = _mm512_cmpgt_pd_mask(vec_result, vec_threshold)
    output.select(vec_mask)

→ 利用 CPU 的 SIMD 指令，单条指令处理多条数据
→ L1/L2 缓存命中率大幅提升（连续内存访问）
→ 分支预测更准确（无 per-row 分支）
```

**关键优化技术：**
- **Batch processing：** 按 granule（8192 行）批量处理
- **SIMD 函数：** 算术运算、字符串匹配、哈希计算全部向量化
- **Late materialization：** 尽量推迟列的合并，减少中间结果
- **Pipeline execution：** 查询计划编译为 pipeline，每一步产出 batch 给下一步

### 3.4 物化视图与预聚合

```sql
-- 源表：存储原始事件
CREATE TABLE events (
    date Date,
    user_id UInt64,
    event_type String,
    duration UInt32
) ENGINE = MergeTree()
ORDER BY (date, user_id);

-- 物化视图：自动预聚合
CREATE MATERIALIZED VIEW daily_stats
ENGINE = SummingMergeTree()
ORDER BY (date, event_type)
AS SELECT
    date,
    event_type,
    sum(duration) AS total_duration,
    count()         AS event_count
FROM events
GROUP BY date, event_type;

-- 写入 events 时，自动触发写入 daily_stats
INSERT INTO events VALUES (...);
-- → daily_stats 自动更新

-- 查询直接读物化视图（快几个数量级）
SELECT * FROM daily_stats WHERE date = '2024-01-01';
```

**物化视图的本质：** 是一个独立的表，在源表 INSERT 时通过触发器机制自动写入。`SummingMergeTree`、`AggregatingMergeTree` 等特殊引擎在 merge 时自动聚合。

### 3.5 一致性模型与共识协议

#### 单节点

- **强一致性：** 单节点内数据写入即持久化（`fsync` 可配置）
- 无 MVCC：读取的是当前最新 committed 数据

#### 分布式复制

- **ZooKeeper/ClickHouse Keeper 协调：**
  - 副本同步基于 ZK 的分布式日志队列
  - INSERT → leader 写入 part → 在 ZK 创建 log entry → followers 从 ZK 消费并 replay
  - **同步复制：** 可配置 `number_of_replicas` 确认数
  - **副本选择：** 读取时可选择任意副本（最终一致性读）或最新副本

- **ClickHouse Keeper：** Yandex 自研的 ZooKeeper 替代
  - Raft 协议实现
  - C++ 编写，性能优于 JVM 版 ZooKeeper
  - 更低的内存占用和更好的 P99 延迟

#### 分布式查询

- `Distributed` 表引擎将查询分发到所有 shard
- 每个 shard 独立执行，结果在发起节点 merge
- **最终一致性：** 跨 shard 的读取不保证全局一致（shard 间无分布式事务）

### 3.6 容错机制

| 故障场景 | 处理方式 |
|----------|----------|
| 节点宕机 | 副本自动接管（通过 ZK 检测） |
| Part 损坏 | checksums.txt 校验，可从不健康副本恢复 |
| 网络分区 | 分区期间写入可能 diverge，恢复后通过 ZK 协调 |
| ZooKeeper 故障 | 只读模式继续，写入阻塞直到 ZK 恢复 |
| 磁盘故障 | 多磁盘配置，parts 可分布到多个磁盘 |

### 3.7 扩展方式

- **Scale-out（水平扩展）：** 增加 shard 数量
  - 需要在集群定义中声明新 shard
  - 历史数据需手动 re-shard（无自动 rebalance）
- **Scale-up（垂直扩展）：** 增加单节点 CPU/内存/磁盘
  - 向量化执行充分利用多核（每个 CPU 核心独立处理一个 batch）
  - 内存主要用于排序、聚合的中间结果

---

## 4. Trade-offs（CAP / PACELC 权衡）

### CAP 定位：**CP 系统**

```
        Consistency
             ▲
             │
             │    ClickHouse
             │    (CP: 分区时保一致)
             │
   ┌─────────┼─────────┐
   │         │         │
   │   CP    │    AP   │
   │         │         │
   ├─────────┼─────────┤
   │         │         │
   │   CA    │    CP   │
   │         │         │
   └─────────┼─────────┘
             │
             │
             ▼
        Partition Tolerance
```

- **在分片内（Replica 组）：** 强一致性（CP）
  - 通过 ZooKeeper/Keeper 保证副本间的一致性
  - 网络分区时，无法写入的副本停止服务
- **跨分片：** 无分布式事务，最终一致性
  - `Distributed` 表的查询不保证跨 shard 的 snapshot 一致性
  - 可通过 `GLOBAL IN/JOIN` 做近似，但不等同于分布式事务

### PACELC 定位

| 场景 | 选择 | 说明 |
|------|------|------|
| **P（Partition）时** | **C（Consistency）** | 副本间通过 ZK 保证一致，分区时写入阻塞 |
| **E（Else，正常时）** | **L（Latency）** | 正常操作下优先低延迟，向量化执行追求极致吞吐 |

```
PACELC:  P→C, E→L
```

### 核心权衡矩阵

| 维度 | 选择 | 代价 |
|------|------|------|
| OLAP vs OLTP | **极致 OLAP** | 不支持高频点查、行级更新、事务 |
| 写入 vs 读取 | **读优化** | 写入需批量（建议 ≥ 1000 行/批），mutation 是异步重写 |
| 一致性 vs 可用性 | **一致性优先** | 副本写入需要 ZK 确认，ZK 故障时写入阻塞 |
| 灵活性 vs 性能 | **性能优先** | 表引擎选择影响语义，Schema 变更需要重写 |
| 自动 vs 手动 | **手动调优** | 无自动 rebalance、无自动查询优化器（基于规则） |
| SQL 标准兼容 | **部分兼容** | 不支持完整 JOIN、子查询有约束、窗口函数逐步完善 |

### 复杂度 vs 运维

- **简单：** 单机部署零依赖，一个二进制文件即可运行
- **复杂：** 生产级分布式部署需要 ZooKeeper/Keeper 集群 + 手动分片规划 + 监控 parts 数量 + 调优 merge 参数
- **Parts 爆炸问题：** 频繁小批量 INSERT 会导致 Part 数量激增，merge 压力增大，需要合理控制写入频率或使用 `Buffer` 引擎做批处理

### 成本 vs 性能

- **极致的读取性能：** 在合适场景下比 Hive 快 100-1000 倍
- **存储效率：** 列压缩比通常 3-10x（LZ4/ZSTD）
- **硬件友好：** 对 CPU 核心数和内存带宽敏感，但不需要昂贵 SSD（HDD 也能跑，SSD 更好）
- **运维成本：** 手动分片管理增加了运维复杂度

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统

```
ClickHouse (2016)
    │
    ├── ByteHouse (字节跳动, 2021)
    │     └── 基于 ClickHouse 架构的云服务版本
    │         增加存算分离、弹性扩展、S3 后端
    │
    ├── Altinity (2017)
    │     └── ClickHouse 商业支持 + Kubernetes Operator
    │
    ├── ClickHouse Cloud (ClickHouse Inc., 2022)
    │     └── 官方云服务，存算分离架构
    │         计算节点无状态，存储节点 S3 + 本地缓存
    │
    ├── 众多 OLAP 产品的参考设计
    │     ├── StarRocks — 部分借鉴列存 + 向量化执行理念
    │     ├── Doris — 类似的列式存储 + 向量化执行
    │     └── 各类实时 OLAP 选型的事实参考基准
    │
    └── 开源生态
          ├── clickhouse-jdbc / clickhouse-go / clickhouse-python
          ├── Grafana / Superset / Metabase 原生支持
          └── Airbyte / Debezium CDC 集成
```

### 成为标准的设计模式

1. **MergeTree 日志结构列存：** 已成为实时 OLAP 的存储范式
   - 不可变 Part + 后台 Merge + 稀疏索引
   - 被多个后续 OLAP 系统借鉴

2. **向量化执行引擎：** SIMD batch processing 成为现代 OLAP 的标准配置
   - StarRocks、Doris、Databend 等均采用向量化执行

3. **物化视图做预聚合：** 通过 `*MergeTree` 系列引擎实现增量聚合
   - 比传统物化视图更灵活（merge 时聚合 vs 写入时聚合）

4. **数据跳过索引：** 在不建 B-Tree 索引的前提下实现高效数据裁剪

### 技术谱系位置

```
                    Column Store Evolution
                    ══════════════════════════

     MonetDB (1990s)
         │
         ▼
     C-Store (2005) ──────▶ Vertica (2005)
         │
         ▼
     Infobright (2006)
         │
         ▼
     Columnar in BigQuery/Redshift (2010s)
         │
         ├──────────────────────────────┐
         ▼                              ▼
     ClickHouse (2016)            Druid (2011)
         │                              │
         ▼                              ▼
     ByteHouse (2021)              Imply / Pivot
     ClickHouse Cloud (2022)
     ───▶ Real-Time OLAP Standard
```

### 当前相关性和使用场景

#### 最适合的场景 ✅

- **日志/事件分析：** Web 分析、用户行为、监控指标
- **时间序列数据：** IoT 传感器、系统监控
- **实时报表：** 业务 Dashboard、运营报表
- **宽表聚合：** 星型/雪花模型中的事实表分析
- **数据量级：** GB 到 PB 级（推荐 100M+ 行才有明显优势）

#### 不适合的场景 ❌

- **OLTP：** 高频点查、行级事务
- **频繁更新/删除：** mutation 成本高
- **高并发小查询：** 设计面向吞吐而非低延迟单查询
- **跨 shard 强一致事务：** 不支持分布式事务
- **小数据量：** 列存和向量化的 overhead 在小数据集上反而更慢

### 市场地位

- GitHub ⭐ 39k+（截至 2024 年），是增长最快的开源数据库之一
- DB-Engines 排名：列式 DBMS 类别 Top 5
- 被广泛用于云厂商的 OLAP 产品（ByteHouse、ClickHouse Cloud、Altinity.Cloud）
- 成为 **"实时 OLAP" 这一品类的代名词**，类似于 Redis 之于缓存、Kafka 之于消息队列

---

## 附录：核心配置参数参考

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `index_granularity` | 8192 | 稀疏索引粒度（行数/mark） |
| `max_threads` | CPU 核数 | 查询并行度 |
| `max_memory_usage` | 10GB | 单查询最大内存 |
| `max_insert_block_size` | 1,048,576 | INSERT 块大小 |
| `background_pool_size` | 16 | 后台 merge 线程池大小 |
| `parts_to_delay_insert` | 300 | Part 数延迟阈值 |

## 附录：MergeTree 家族引擎速查

| 引擎 | 特性 | 典型场景 |
|------|------|----------|
| `MergeTree` | 基础引擎，主键排序 + 分区 | 通用 OLAP |
| `ReplacingMergeTree` | 按版本列去重 | 幂等写入、去重 |
| `SummingMergeTree` | merge 时数值列求和 | 预聚合指标 |
| `AggregatingMergeTree` | merge 时执行聚合函数 | 精细预聚合 |
| `CollapsingMergeTree` | 折叠 sign 列的对 | 状态变更流 |
| `VersionedCollapsingMergeTree` | 带版本的折叠 | 带版本的状态流 |
| `ReplicatedMergeTree` | 多副本同步 | 高可用集群 |
| `Distributed` | 跨 shard 查询 | 分布式查询入口 |

---

## 参考资料

- [ClickHouse 官方文档](https://clickhouse.com/docs/)
- [ClickHouse Architecture Overview](https://clickhouse.com/docs/en/development/architecture)
- Yandex.Metrica Blog — [ClickHouse: 原始官方博客](https://habr.com/en/company/yandex/blog/323790/)
- [Altinity Knowledge Base](https://kb.altinity.com/)
- DB-Engines Ranking — [Column Store DBMS](https://db-engines.com/en/ranking/column-store+dbms)
