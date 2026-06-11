# Delta Lake

## 基本信息
- **发布时间：** 2019 年 4 月开源（Apache 2.0）
- **开发方：** Databricks（基于其内部存储系统的工程经验）
- **开源协议：** Apache 2.0，2019 年贡献给 Linux Foundation Delta Lake Project
- **核心论文/文档：** Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores（CIDR 2020）
- **定位：** 数据湖上的**开放表格式**（Open Table Format），为数据湖带来 ACID 事务、版本控制、Schema 治理等数据库能力

---

## 1. Context & Motivation（背景与动机）

### 1.1 解决的问题

2015–2018 年间，数据湖（HDFS / S3 + Parquet）成为主流存储范式。但原始数据湖面临几个致命缺陷：

| 痛点 | 表现 |
|------|------|
| **缺乏原子性** | 并发写入导致部分写入、脏数据；失败写入留下残缺文件 |
| **缺乏一致性** | 多个 Reader 和 Writer 同时访问，可能读到不一致的快照 |
| **无法回滚** | 写入错误后无版本恢复机制 |
| **Schema 漂移** | 列名/类型变更后无追踪，下游任务静默失败 |
| **Update/Delete 困难** | Parquet 是只追加格式，行级更新需要全表重写 |
| **小文件问题** | 流式写入产生海量小文件，读取性能急剧下降 |

**根本矛盾：** 数据湖的"廉价 + 灵活"是以牺牲**数据可靠性**为代价的。企业需要在湖上跑生产级分析/ML，但湖本身没有数据库级别的事务保障。

### 1.2 前代系统局限

- **Hive 表：** 只有目录级元数据，无事务；并发写入导致数据损坏
- **直接 Parquet：** 文件系统级别的操作，无原子性保证；`mv` 不是跨目录原子的
- **HBase/Kudu：** 支持更新但存储成本高，失去对象存储的廉价优势

### 1.3 硬件/网络趋势

- **对象存储成熟：** S3/ADLS 提供 EB 级廉价存储，但仅提供 Eventually Consistent 或 List-after-Write 语义
- **计算-存储分离：** Spark 等分布式计算引擎天然需要共享存储，但存储层无法保证一致性
- **SSD/NVMe 普及：** 使 Parquet 列存格式的性能优势充分释放

---

## 2. Architecture（架构设计）

### 2.1 系统拓扑

Delta Lake **不是独立的分布式系统**，而是运行在计算引擎（Spark/Databricks Runtime）之上的**表格式层**：

```
┌─────────────────────────────────────────────────────────┐
│                    计算层 (Compute)                       │
│  Spark / Databricks Runtime / SQL 引擎                    │
│         Delta Lake API (DataFrame / SQL / Python)        │
├─────────────────────────────────────────────────────────┤
│                  Delta Lake 表格式层                       │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ Transaction  │  │   Schema     │  │   Time        │  │
│  │   Log        │  │  Enforcement │  │   Travel      │  │
│  └──────┬───────┘  └──────┬───────┘  └───────┬───────┘  │
│         │                  │                   │         │
│  ┌──────┴──────────────────┴───────────────────┴──────┐  │
│  │              元数据管理：_delta_log                  │  │
│  │     (JSON commit files + CRC checksums)             │  │
│  └──────────────────────┬─────────────────────────────┘  │
├─────────────────────────┼─────────────────────────────────┤
│                         │       存储层 (Storage)           │
│                  ┌──────┴──────┐                           │
│                  │ Parquet 数据 │                           │
│                  │ 文件 (S3/   │                           │
│                  │  ADLS/HDFS) │                           │
│                  └─────────────┘                           │
└─────────────────────────────────────────────────────────┘
```

### 2.2 元数据架构归类

Delta Lake 属于 **算法/计算式 + 集中式日志** 混合模式：

- **核心元数据：** `_delta_log/` 目录下的 JSON 事务日志文件（00000000000000000000.json, 00000000000000000001.json, ...）
- **元数据即日志：** 每个 commit 是一个原子操作，包含 add/remove/setTransaction/checkpoint 等 action
- **Checkpoint 机制：** 每 10 个 commit（默认）生成一个 `_last_checkpoint` 文件 + Parquet 格式的 checkpoint，加速启动时的元数据加载
- **Checksum 文件：** 每个 commit 对应 `.crc` 校验文件，防止日志损坏

### 2.3 数据模型与命名空间

```
table_path/
├── _delta_log/
│   ├── 00000000000000000000.json          ← Commit 0（CREATE TABLE）
│   ├── 00000000000000000000.json.crc      ← CRC 校验
│   ├── 00000000000000000001.json          ← Commit 1（INSERT）
│   ├── 00000000000000000002.json          ← Commit 2（UPDATE）
│   ├── ...
│   ├── 00000000000000000010.checkpoint.parquet  ← Checkpoint
│   └── _last_checkpoint                    ← 指向最新 checkpoint
├── part-00000-xxx.c000.snappy.parquet     ← 数据文件
├── part-00001-xxx.c000.snappy.parquet
└── ...
```

**命名空间设计：**
- 表 = 目录路径（目录级命名空间）
- 版本 = 单调递增的 commit 版本号
- 数据文件 = 标准 Parquet 文件，无特殊格式

### 2.4 数据架构分析

| 维度 | Delta Lake 设计 |
|------|----------------|
| **数据格式** | Parquet（列存、压缩、谓词下推） |
| **数据分块** | 由上游计算引擎决定（Spark partition write） |
| **数据分布** | 无内置分布，依赖底层存储；通过 Z-ORDER/Liquid Clustering 实现数据局部性优化 |
| **数据冗余** | 依赖底层存储（S3 多 AZ、ADLS ZRS 等） |
| **I/O 路径** | 通过云存储 SDK / Hadoop FileSystem API |
| **生命周期** | VACUUM 命令清理过期文件（默认保留 7 天） |

### 2.5 核心组件：事务日志（Transaction Log）

每个 JSON commit 文件包含一组 **Action**：

```json
// 00000000000000000000.json 示例
{"commitInfo":{"timestamp":1718000000000,"operation":"WRITE","isolationLevel":"Serializable"}}
{"metaData":{"id":"uuid","name":"users","format":{"provider":"parquet"},"schemaString":"{...}","partitionColumns":[]}}
{"add":{"path":"part-00000-xxx.snappy.parquet","size":12345,"modificationTime":1718000000000,"dataChange":true}}
```

**Action 类型：**

| Action | 说明 |
|--------|------|
| `add` | 添加数据文件 |
| `remove` | 标记文件删除（带时间戳，延迟物理删除） |
| `metaData` | 表元数据变更（Schema、分区、配置） |
| `protocol` | 读写协议版本升级 |
| `txn` | 应用级事务 ID（流式写入 exactly-once） |
| `commitInfo` | 提交信息（操作类型、隔离级别） |
| `cdc` | Change Data Feed 记录（3.0+） |

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 ACID 事务 — 乐观并发控制（Optimistic Concurrency Control, OCC）

Delta Lake 的 ACID 实现基于 **Serializable Snapshot Isolation (SSI)** + **OCC**：

```
Writer A: 读取当前版本 v → 准备写入 → CAS 提交 v→v+1 ✅
Writer B: 读取当前版本 v → 准备写入 → CAS 提交 v→v+1 ❌（冲突）
         → 重试：读取新版本 v+1 → 重新验证冲突 → 提交 v→v+2 ✅
```

**OCC 三阶段：**

1. **Read Phase：** 读取当前表快照，记录读取的数据文件集合
2. **Write Phase：** 在本地准备新数据文件和新的 commit
3. **Validation & Commit Phase：** 尝试原子地追加 JSON 文件到 `_delta_log/`
   - 如果当前版本号与读取时一致 → 提交成功
   - 如果版本号已被其他 Writer 推进 → **冲突**
     - **可自动解决：** 无重叠文件、append-only 操作
     - **不可自动解决：** 需要应用层重试

**原子性保证：**
- 对象存储的 **单文件原子写入**（S3 PUT、ADLS 写入）
- 如果 commit 文件存在，则该 commit 完整有效
- 如果 commit 文件不存在，则该 commit 从未发生

**隔离级别：**
- `Serializable`（默认）：最高隔离，完全防止并发冲突
- `WriteSerializable`：允许并发读和写（读不需要看到最新写入）
- `SnapshotIsolation`：最宽松，只保证读自身一致

### 3.2 Time Travel（时间旅行）

```sql
-- 按版本号查询
SELECT * FROM users VERSION AS OF 5

-- 按时间戳查询
SELECT * FROM users TIMESTAMP AS OF '2024-06-01 00:00:00'

-- DataFrame API
spark.read.format("delta").option("versionAsOf", 5).load("/path/to/table")
```

**实现原理：**
- 每个 commit 版本号 = 时间线上的一个快照点
- 查询指定版本时，重放事务日志到该版本，得到当时的文件集合
- Checkpoint 加速：从最近的 checkpoint 开始重放，而非从 0 开始
- 历史文件在 VACUUM 之前保留（默认 7 天），可恢复任意历史版本

### 3.3 Schema Enforcement & Evolution

```sql
-- Schema Enforcement（默认开启）
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
-- 如果 age 列不存在或类型不匹配 → 写入失败

-- Schema Evolution（显式开启）
spark.conf.set("spark.databricks.delta.schema.autoMerge", "true")
-- 自动添加新列，类型不兼容的列仍会拒绝
```

**Schema 治理模型：**

| 场景 | 默认行为 | 可配置 |
|------|----------|--------|
| 插入新列 | ❌ 拒绝 | `autoMerge` → ✅ 自动添加 |
| 删除列 | ✅ 保留旧 Schema | 需显式 `replaceWhere` |
| 类型变更 | ❌ 拒绝 | 不可自动转换 |
| 列名大小写 | 取决于配置 | `delta.caseSensitive` |

Schema 本身存储在 `metaData.action` 中，每次变更作为新的 commit 记录，因此 **Schema 变更历史可通过 Time Travel 回溯**。

### 3.4 MERGE / UPSERT（合并/更新插入）

```sql
MERGE INTO users AS target
USING updates AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name, age = source.age
WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (source.id, source.name, source.age)
```

**实现机制：**

```
1. 读取目标表和源数据
2. 根据 ON 条件执行 Hash Join / Sort Merge Join
3. 对匹配的行生成 UPDATE 版本
4. 对未匹配的行保持原样或生成 INSERT
5. 将所有结果写入新的 Parquet 文件
6. 原子提交：add 新文件 + remove 旧文件（同一 commit）
```

**COPY INTO（幂等批量加载）：**
```sql
COPY INTO users
FROM '/path/to/new-data/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true')
```
- 自动跳过已加载的文件（通过文件路径/大小哈希去重）
- 适合批量数据摄入场景

### 3.5 Z-ORDER Optimization（数据跳过优化）

```sql
OPTIMIZE users ZORDER BY (country, date)
```

**原理：**
- Z-Order Curve（希尔伯特曲线的 2D 近似）将多维数据映射到 1D
- 同一 Z-Order 值的行被写入同一个 Parquet 文件
- 查询时利用 Parquet 的 **min/max 统计信息** 跳过不相关的文件
- **Data Skipping：** Delta 自动收集每列的 min/max/null_count，存储在事务日志中

```
原始数据:                    Z-ORDER 后:
文件1: [CN,2024] [US,2023]    文件1: [CN,2024] [CN,2025] [CN,2023]
文件2: [CN,2025] [JP,2024]    文件2: [US,2023] [US,2024] [JP,2024]
文件3: [US,2024] [JP,2023]    文件3: [JP,2023] [JP,2024] [US,2025]

查询 country='CN':             只需扫描文件1（跳过了文件2和3）
```

**限制：** Z-ORDER 是静态的；数据变更后需要重新 OPTIMIZE

### 3.6 Liquid Clustering（液体聚类 — 2023 年新特性）

```sql
CREATE TABLE events (
  event_id STRING,
  event_time TIMESTAMP,
  user_id STRING,
  event_type STRING
) USING DELTA
CLUSTER BY (event_type, user_id)
```

**与 Z-ORDER 的对比：**

| 维度 | Z-ORDER | Liquid Clustering |
|------|---------|-------------------|
| 排序时机 | OPTIMIZE 时一次性排序 | 写入时持续聚类 |
| 维护成本 | 需要定期 OPTIMIZE | 自动维护 |
| 数据覆盖 | 全表重排 | 仅影响新写入/变更的数据 |
| 灵活性 | 固定聚类列 | 可动态调整聚类列 |
| 适用场景 | 静态数据集 | 持续更新的流式/增量数据 |

**核心创新：** 将"数据排序"从离线维护任务转变为**写入时的持续过程**，消除了 Z-ORDER 的维护窗口需求。

### 3.7 Unified Batch & Streaming（统一批流）

```python
# 流式写入 Delta 表
streamingDF.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/checkpoint") \
  .start("/path/to/delta-table")

# 流式读取 Delta 表（Change Data Feed）
spark.readStream \
  .format("delta") \
  .option("readChangeFeed", "true") \
  .load("/path/to/delta-table")
```

**统一性体现在：**
- 同一表格式同时支持批处理和流处理
- Spark Structured Streaming 与 Delta 深度集成
- 流式写入通过 `txn` action 实现 **exactly-once** 语义
- Change Data Feed（3.0+）支持下游消费者订阅变更流

### 3.8 元数据架构 ASCII 图

```
┌──────────────────────────────────────────────────────────┐
│                    Writer Process                         │
│                                                          │
│  1. Read current snapshot (version = N)                   │
│     ↓                                                     │
│  2. Write new Parquet files to table/                     │
│     ↓                                                     │
│  3. Prepare commit JSON (add/remove/metaData actions)     │
│     ↓                                                     │
│  4. Atomic CAS: table/_delta_log/N+1.json                 │
│     ├── Success → commit valid                            │
│     └── Conflict → OCC retry (re-read, re-validate)       │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│                    Reader Process                         │
│                                                          │
│  1. Read _last_checkpoint → find latest checkpoint       │
│     ↓                                                     │
│  2. Load checkpoint (Parquet format, fast)               │
│     ↓                                                     │
│  3. Replay commits from checkpoint to target version     │
│     ↓                                                     │
│  4. Resolve file set → read Parquet files                │
│     └── Apply data skipping (min/max stats from log)      │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│                 _delta_log/ 目录结构                       │
│                                                          │
│  table/_delta_log/                                       │
│  ├── 00000000000000000000.json          ← Commit 0       │
│  ├── 00000000000000000000.json.crc      ← Checksum       │
│  ├── 00000000000000000001.json          ← Commit 1       │
│  ├── ...                                                 │
│  ├── 00000000000000000009.checkpoint.parquet              │
│  ├── 00000000000000000010.json          ← Commit 10      │
│  ├── ...                                                 │
│  ├── 00000000000000000019.checkpoint.parquet              │
│  └── _last_checkpoint                  ← Pointer         │
│                                                          │
│  每 10 个 commit 生成一次 checkpoint (Parquet 格式)       │
│  Checkpoint = 到该版本为止的完整表状态快照                  │
└──────────────────────────────────────────────────────────┘
```

---

## 4. Trade-offs（CAP / PACELC）

### 4.1 CAP 定位

Delta Lake **本身不提供 CAP 中的 C 或 A**——它依赖底层存储系统的保证：

| 底层存储 | 一致性模型 | Delta Lake 的 ACID |
|----------|-----------|-------------------|
| **AWS S3** | 强一致（2020-12 起） | ✅ 完整 ACID |
| **Azure ADLS Gen2** | 强一致 | ✅ 完整 ACID |
| **HDFS** | 强一致 | ✅ 完整 ACID |
| **GCS** | 强一致 | ✅ 完整 ACID |
| **早期 S3（2020 前）** | 最终一致（List-after-Write） | ⚠️ 需要额外处理 |

**结论：Delta Lake 是存储层之上的"一致性叠加层"。** 它的 ACID 保证在底层存储提供原子单文件写入的前提下成立。

### 4.2 PACELC 分析

Delta Lake 作为表格式层，其 PACELC 行为受计算引擎和存储层共同影响：

| 场景 | 选择 | 说明 |
|------|------|------|
| **分区时 (P)** | 取决于存储 | S3/ADLS 本身是分区容忍的；Delta 日志的原子写入保证了分区期间的一致性 |
| **正常时 (EL)** | **E（一致性）优先** | Serializable 隔离级别确保读者看到一致的快照；写入通过 OCC 保证线性化 |
| **写入冲突** | 重试 > 快速失败 | OCC 冲突时自动重试，而非返回错误（在一定次数内） |

**本质选择：** Delta Lake 在 CAP 中偏向 **CP**（当底层存储是强一致时），在 PACELC 中正常时选 **E**（一致性），牺牲部分写入延迟（OCC 重试）。

### 4.3 复杂度 vs 运维简洁性

| 优势 | 代价 |
|------|------|
| 表格式层简化了 ACID 实现 | 需要定期运行 OPTIMIZE / VACUUM |
| Checkpoint 自动管理元数据膨胀 | 大表的 checkpoint 可能很大 |
| Schema 演化自动记录 | Schema 合并冲突需人工处理 |
| 与 Spark 深度集成 | 非 Spark 引擎支持有限（需 Delta Kernel / Delta Standalone Reader） |
| 事务日志即完整历史 | 日志增长需要定期清理 |

### 4.4 成本 vs 性能

| 维度 | 成本影响 | 性能影响 |
|------|----------|----------|
| **事务日志** | 极小（JSON 文件，KB 级） | 读取时 checkpoint 加速启动 |
| **Parquet 数据文件** | 压缩比高，存储成本低 | 列存 + 谓词下推 = 高效扫描 |
| **Z-ORDER / Liquid Clustering** | 额外存储（重写数据时） | 显著减少扫描数据量 |
| **VACUUM 保留期** | 历史文件占用额外存储 | 支持 Time Travel / 恢复 |
| **多引擎支持** | Delta Kernel 减少引擎锁定 | 非 Databricks 引擎性能可能较低 |

---

## 5. Influence & Legacy（影响与遗产）

### 5.1 启发的后续系统与技术

| 后续发展 | 关系 |
|----------|------|
| **Databricks Lakehouse** | Delta Lake 是 Lakehouse 架构的核心存储层 |
| **Delta Lake 3.0+** | Change Data Feed、Liquid Clustering、Unity Catalog 集成 |
| **Delta Kernel** | 轻量级嵌入式连接器，支持非 JVM 引擎（Python、Rust、C++）读取 Delta |
| **UniForm** | Delta 表自动暴露为 Iceberg 格式，实现多格式互操作 |
| **Delta Sharing** | 开放协议，跨组织安全共享 Delta 表数据 |

### 5.2 成为标准的设计模式

1. **事务日志即真相源：** _delta_log 模式被广泛采纳（类似 etcd/WAL 的思想应用于数据湖）
2. **乐观并发控制用于表级操作：** 区别于传统数据库的行级锁，Delta 证明了表级 OCC 在实际场景中的可行性
3. **数据即文件 + 元数据即日志：** 分离数据和元数据管理，元数据轻量、数据可并行
4. **Time Travel 作为一等公民：** 版本化查询成为湖格式的标准能力
5. **Data Skipping 统计内嵌元数据：** 在事务日志中存储 min/max/null_count，使查询优化无需读取数据

### 5.3 技术谱系位置（Technology Genealogy）

```
                    Google File System (2003)
                          │
                    Hadoop HDFS (2006)
                          │
              ┌───────────┴────────────┐
              │                        │
         Hive Tables               Parquet (2013)
       (无事务元数据)              (列式存储格式)
              │                        │
              └───────────┬────────────┘
                          │
              ┌───────────┼──────────────────┐
              │           │                   │
         Apache Hudi   Delta Lake       Apache Iceberg
          (Uber, 2017)  (Databricks,     (Netflix, 2017)
                         2019 开源)
              │           │                   │
              │     Databricks            Snowflake
              │       Lakehouse           Iceberg 支持
              │           │
              │     Delta Kernel
              │     UniForm
              │     Delta Sharing
```

**思想传承路径：**
- **GFS/HDFS** → 共享存储理念 → 数据湖基础
- **Parquet** → 高效列存 → Delta 的数据文件格式
- **传统数据库 ACID** → OCC 适配到湖场景 → Delta 的事务模型
- **Git 的版本控制思想** → commit 日志 + checkpoint → Delta 的时间旅行

### 5.4 三大表格式竞争格局（2024 视角）

| 维度 | Delta Lake | Apache Iceberg | Apache Hudi |
|------|------------|----------------|-------------|
| **发起方** | Databricks | Netflix | Uber |
| **治理** | Linux Foundation | Apache Software Foundation | Apache Software Foundation |
| **引擎支持** | Spark, Databricks, Trino, Presto, Flink (via Delta Kernel) | Spark, Flink, Trino, Presto, StarRocks, Doris | Spark, Flink, Trino, Presto |
| **默认存储格式** | Parquet | Parquet/ORC | Parquet/HBase |
| **写入模型** | CoW + MoR (通过流式) | 快照隔离 + Manifest 树 | CoW + MoR (原生) |
| **CDC** | Change Data Feed (3.0+) | 有限（通过 CDC extension） | 原生支持 |
| **流式写入** | ✅ 与 Structured Streaming 深度集成 | 有限（Flink 支持） | ✅ 原生流式摄入 |
| **云厂商集成** | Databricks 原生，AWS/Azure/GCS 支持 | AWS Glue, Snowflake, BigQuery, Databricks | AWS EMR, Databricks |
| **社区活跃度** | 高（Databricks 驱动） | 极高（多厂商中立） | 中（Uber 主导） |
| **中国市场** | 较少直接使用 | 阿里云、火山引擎原生支持 | 字节跳动内部使用较多 |

### 5.5 当前相关性和使用场景

**Delta Lake 在以下场景是首选：**

1. **Databricks 生态用户：** Lakehouse 架构的自然选择
2. **Spark 重度用户：** 与 Spark 集成最深，API 最自然
3. **需要 ACID + Time Travel：** 金融/医疗等需要审计追踪的行业
4. **ML 数据管道：** 版本化数据集支持模型复现

**正在演化的方向：**

- **Delta Kernel：** 打破 Spark 锁定，让任何引擎都能读取 Delta
- **UniForm：** 通过双写机制同时暴露 Delta + Iceberg 格式，减少格式锁定
- **Liquid Clustering：** 逐步替代 Z-ORDER，成为默认数据组织方式
- **Delta Live Tables：** 声明式 ETL 管道，构建在 Delta 之上

---

## 参考资料

- Armbrust, M. et al. "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores." CIDR 2020.
- Delta Lake Documentation: https://docs.delta.io/
- Delta Lake GitHub: https://github.com/delta-io/delta
- Databricks Lakehouse Platform Whitepaper
- Delta Lake 3.0 Release Notes (Change Data Feed)
- Liquid Clustering Documentation (Databricks, 2023)
