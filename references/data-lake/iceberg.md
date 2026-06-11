# Apache Iceberg

## 基本信息

| 维度 | 详情 |
|------|------|
| **发布时间** | 2018 年（开源）|
| **发起方** | Netflix |
| **开源协议** | Apache 2.0 |
| **基金会** | Apache Software Foundation（2020 年进入孵化器，2021 年成为顶级项目）|
| **类型** | 开放表格式（Open Table Format），非数据库 |
| **核心论文** | *Iceberg: High-Performance Table Format for Data Lakes*（非学术论文，以设计文档/规范形式发布）|
| **规范版本** | v1（2018）、v2（2021，加入行级删除）|
| **GitHub** | https://github.com/apache/iceberg |

**一句话定义：** Iceberg 是构建在数据湖对象存储之上的**开放表格式（Table Format）**，通过引入独立的元数据层，为大规模数据集提供 ACID 事务、快照隔离、时间旅行、模式演进等数据库级能力。

---

## 1. Context & Motivation（背景与动机）

### 1.1 解决的问题

2018 年前后，数据湖（Data Lake）架构已成为企业主流：原始数据以 Parquet/ORC 格式批量存储在 HDFS 或 Amazon S3 上，通过 Hive Metastore、Presto、Spark SQL 等引擎查询。然而，Hive 表格式存在一系列根本性缺陷：

| Hive 表格式缺陷 | 影响 |
|----------------|------|
| **无原子写入** | 写入过程中查询可能读到不完整数据 |
| **无并发写入支持** | 多个写入者可能互相覆盖，无冲突检测 |
| **无快照隔离** | 查询无法获得一致性视图 |
| **Schema 演化需重写** | 添加列需重写整个表，代价高昂 |
| **分区耦合路径** | 分区信息编码在目录路径中（`dt=2018-06-01/`），改分区需重写数据 |
| **无时间旅行** | 无法查询历史数据版本 |
| **无行级删除** | 删除数据只能重写整个分区 |

Netflix 的数据规模（PB 级、日均数千次查询、数百名分析师）使这些问题成为实际生产瓶颈。

### 1.2 前代系统局限

- **Hive Metastore + HDFS**：元数据与存储紧耦合，缺乏事务语义。
- **HDFS Append 模型**：Hadoop 早期不支持随机写，后来追加写也不具备 ACID 保证。
- **对象存储限制**：S3 等对象存储是最终一致的（当时），`LIST` 操作昂贵，无法作为事务日志使用。

### 1.3 硬件/网络趋势

- **对象存储成本急剧下降**：S3 成为事实上的数据湖后端，但其最终一致性和高 `LIST` 延迟使传统 Hive 方式不可行。
- **列式存储成熟**：Parquet 成为标准，Iceberg 天然绑定 Parquet/ORC。
- **计算存储分离成为趋势**：Spark/Presto 等无状态计算引擎与无状态对象存储结合，中间需要一层"表语义"。

---

## 2. Architecture（架构设计）

### 2.1 系统拓扑

Iceberg 不是独立运行的服务，而是一个**客户端侧的表格式库（Library）**，嵌入在查询引擎中：

```
┌─────────────────────────────────────────────────────────┐
│                    Query Engine                          │
│  Spark │ Flink │ Presto/Trino │ StarRocks │ Hive │ etc. │
└────┬────────┬───────┬──────────────┬──────────┬──────┬───┘
     │        │       │              │          │      │
     ▼        ▼       ▼              ▼          ▼      ▼
┌─────────────────────────────────────────────────────────┐
│              Iceberg Runtime Library                     │
│  (Java)                                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐ │
│  │Catalog API│ │Snapshot  │ │Scan Plan │ │Write Commit│ │
│  └──────────┘ └──────────┘ └──────────┘ └────────────┘ │
└──────────────────────┬──────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
┌───────────────────┐     ┌───────────────────┐
│  Object Storage   │     │   Catalog Store   │
│  (S3/GCS/ADLS/    │     │  (Hive Metastore, │
│   HDFS/Local FS)  │     │   JDBC, REST,     │
│                   │     │   Glue, Nessie)   │
│  data files       │     └───────────────────┘
│  metadata files   │
└───────────────────┘
```

**关键特征：** Iceberg 是**无服务器组件**的——没有独立的 Iceberg Master、Iceberg RegionServer。所有逻辑在查询引擎进程中执行，元数据文件和数据文件全部持久化在对象存储上。

### 2.2 元数据架构——四层树形结构

Iceberg 的元数据采用**自顶向下的四层树形结构**，属于**外部引擎（文件即元数据）**模式。所有元数据以 JSON/Avro 格式文件持久化在对象存储中：

```
┌──────────────────────────────────────────────────────────────┐
│                     metadata.json (L0)                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ 表级元数据：                                           │   │
│  │  • schema (当前 + 历史)                                │   │
│  │  • partition spec (当前 + 历史)                        │   │
│  │  • sort order                                         │   │
│  │  • properties                                         │   │
│  │  • current-snapshot-id                                │   │
│  │  • snapshots[] (快照列表，含 snapshot-id,             │   │
│  │    timestamp, manifest-list 路径)                     │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────┬───────────────────────────────────────┘
                       │ references
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                manifest list (L1) — Avro                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ 快照级清单：                                           │   │
│  │  每个快照对应一个 manifest list 文件                     │   │
│  │  包含该快照下所有 manifest file 的条目：                 │   │
│  │  • manifest file 路径                                 │   │
│  │  • 分区摘要 (partition summary)                       │   │
│  │  • added/deleted/existing 行数                       │   │
│  │  • data file 数量                                    │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────┬───────────────────────────────────────┘
                       │ references
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              manifest file (L2) — Avro                       │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ 数据文件清单：                                         │   │
│  │  每个 manifest file 包含一组 data file 的条目：        │   │
│  │  • data file 路径                                     │   │
│  │  • partition 值                                       │   │
│  │  • 列级统计：min/max/null count/NaN count            │   │
│  │  • 文件大小、记录数                                   │   │
│  │  • content type: DATA / POSITION_DELETES /           │   │
│  │    EQUALITY_DELETES                                  │   │
│  │  • content type 的 column ids (equality delete)      │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────┬───────────────────────────────────────┘
                       │ references
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                data files (L3) — Parquet/ORC                 │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ 实际数据：                                             │   │
│  │  • Parquet 或 ORC 格式                                │   │
│  │  • 按 manifest 中记录的 partition spec 组织            │   │
│  │  • 路径与分区解耦（隐藏分区）                           │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### 2.3 元数据架构归类

| 维度 | Iceberg 的选择 |
|------|---------------|
| **元数据模式** | 外部引擎——**文件即元数据**（File-as-Metadata）。所有元数据以不可变文件形式持久化在对象存储中 |
| **元数据一致性** | 通过**乐观并发控制（Optimistic Concurrency Control, OCC）**实现。写入时检查 `metadata.json` 版本，冲突则重试 |
| **Catalog 层** | 可插拔：Hive Metastore、JDBC、REST Catalog、AWS Glue、Nessie、DynamoDB 等。Catalog 仅存储**指向最新 metadata.json 的指针** |
| **变更日志** | 无独立 WAL。每次写入产生新 snapshot，通过 manifest 差异体现变更 |

### 2.4 数据架构

#### 2.4.1 数据分块

- Iceberg 不控制数据分块。数据以 **Parquet/ORC 文件** 粒度组织。
- 写入时由写入引擎（Spark/Flink）决定文件大小（`target-file-size-bytes`，默认 ~512MB）。
- 支持 compaction（合并小文件）以优化读取性能。

#### 2.4.2 数据分布

- 通过 **Partition Spec** 定义分区策略，但分区值**不编码在文件路径中**（隐藏分区）。
- 分区信息存储在 manifest file 中，允许在运行时修改分区策略而无需重写数据。
- 支持 **Bucket 分区**、**Truncate 分区**、**Year/Month/Hour 分区** 等多种分区变换函数。

#### 2.4.3 数据冗余

- Iceberg 不负责数据冗余。冗余由底层对象存储保障（S3 默认 3 AZ 冗余、11 个 9 的持久性）。

#### 2.4.4 数据生命周期

- **元数据清理**：通过 `expireSnapshots()` 清理过期快照及其关联的 manifest 和 data file。
- **数据保留**：可通过 `metadata-history-max-entries` 控制保留的快照数量。
- **垃圾回收**：过期快照的 manifest 和 data file 可安全删除（因为不可变）。

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 快照隔离（Snapshot Isolation）

Iceberg 的核心抽象是**快照（Snapshot）**：

```
时间线 ─────────────────────────────────────────────────────────────▶

         写入 #1         写入 #2            写入 #3
         (snapshot A)    (snapshot B)       (snapshot C)
              │               │                 │
              ▼               ▼                 ▼
         metadata.json ───> metadata.json ───> metadata.json
         → snapshot A      → snapshot B       → snapshot C
                            │
                            ├─ manifest list B
                            │   ├─ manifest B1 → data file 1, 2
                            │   └─ manifest B2 → data file 3, 4
                            │
                            └─ (snapshot A 的 metadata 仍然可读)
```

**关键特性：**
- 每次写入（Append/Overwrite/Delete/Update）创建一个**新快照**。
- 快照是**不可变的**——一旦创建，永远不会修改。
- `metadata.json` 始终指向**当前快照**，但历史快照数据仍然完整保留。
- 查询始终在**单一快照**上执行，提供读一致性。
- 快照之间共享 manifest 和 data file——只有变更部分写入新文件，未变更的文件被复用。

### 3.2 隐藏分区（Hidden Partitioning）

Iceberg 的分区系统与 Hive 的根本区别：

```
Hive 分区（路径耦合）：
  /table/dt=2024-01-01/country=US/data.parquet
  /table/dt=2024-01-02/country=CN/data.parquet
  → 分区值硬编码在路径中，改分区 = 重写数据

Iceberg 隐藏分区（元数据解耦）：
  /table/data/0001-abcd.parquet
  /table/data/0002-efgh.parquet
  → 分区值存储在 manifest file 中
  → 可修改分区策略，数据文件无需移动
  → 可自动分区（partition evolution）
```

### 3.3 Schema 演化（Schema Evolution）

Iceberg 通过**列 ID（Column ID）**而非列名来引用数据列，实现无重写的 Schema 演化：

```json
// schema 定义
{
  "type": "struct",
  "fields": [
    { "id": 1, "name": "order_id",   "type": "long"   },
    { "id": 2, "name": "user_id",    "type": "long"   },
    { "id": 3, "name": "amount",     "type": "double" },
    { "id": 4, "name": "status",     "type": "string" }  // ← 新加列
  ],
  "last-column-id": 4
}
```

**支持的演化操作：**

| 操作 | 是否需重写数据 | 说明 |
|------|--------------|------|
| 添加列 | ❌ 不需要 | 新列对新写入的数据有效，历史数据中为 NULL |
| 删除列 | ❌ 不需要 | 逻辑删除，元数据中标记为 dropped |
| 重命名列 | ❌ 不需要 | 仅修改元数据中的 name 映射 |
| 修改列类型（安全转换）| ❌ 不需要 | 如 int → long, float → double |
| 调整列顺序 | ❌ 不需要 | 列 ID 不变，顺序仅影响展示 |

### 3.4 时间旅行（Time Travel）

基于快照不可变性，Iceberg 支持查询任意历史版本：

```sql
-- 按 snapshot ID 查询
SELECT * FROM orders TIMESTAMP AS OF 5981230475924;

-- 按时间戳查询
SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00';

-- 查询快照元数据
SELECT * FROM orders.snapshots;

-- 查看快照变更历史
SELECT * FROM orders.history;
```

### 3.5 行级删除（Row-Level Deletes）

v2 规范引入两种删除模式：

#### 3.5.1 位置删除（Position Deletes）

- manifest file 中为每个 data file 分配一个**位置 ID（ordinal position）**。
- 删除操作将 `(data_file_path, position)` 写入专门的 position delete 文件。
- 读取时合并 data file 和 position delete file，过滤已删除的行。

```
Position Delete 文件：
┌─────────────────────────────────────┐
│ file_path              │ position   │
├────────────────────────┼────────────┤
│ s3://bucket/0001.parq  │ 42         │  ← 删除第 42 行
│ s3://bucket/0001.parq  │ 105        │  ← 删除第 105 行
│ s3://bucket/0003.parq  │ 7          │  ← 删除第 7 行
└─────────────────────────────────────┘
```

#### 3.5.2 等值删除（Equality Deletes）

- 指定一列或多列作为**等值键**。
- 等值删除文件包含待删除行的键值。
- 读取时，若 data file 中某行的键值匹配等值删除文件中的键值，则该行被过滤。

```
Equality Delete 文件（等值键 = order_id）：
┌─────────────────────────────────────┐
│ equality_delete_file_id │ order_id  │
├─────────────────────────┼───────────┤
│ 5                       │ 1001      │  ← 删除 order_id=1001 的所有行
│ 5                       │ 1042      │  ← 删除 order_id=1042 的所有行
└─────────────────────────────────────┘
```

**两种模式对比：**

| 维度 | Position Deletes | Equality Deletes |
|------|-----------------|------------------|
| 删除粒度 | 精确到文件内行位置 | 按列值匹配 |
| 适用场景 | UPDATE/DELETE after INSERT | MERGE INTO / CDC / 基于主键的删除 |
| 读取开销 | 需扫描 delete file | 需 build hash set |
| 写入开销 | 低（追加即可） | 需扫描待删除数据 |
| v1 支持 | ❌ | ❌ |
| v2 支持 | ✅ | ✅ |

### 3.6 并发控制——乐观并发控制（OCC）

Iceberg 不使用分布式锁或共识协议。写入采用 **OCC（Optimistic Concurrency Control）**：

```
Writer A:                              Writer B:
  │                                      │
  ├─ 读取当前 metadata.json (v1)          │
  ├─ 写入新数据文件                        │
  ├─ 生成新 manifest                       ├─ 读取当前 metadata.json (v1)
  ├─ 生成新 snapshot                       ├─ 写入新数据文件
  ├─ CAS: metadata.json v1 → v2 ✓         ├─ 生成新 manifest
  │                                        ├─ 生成新 snapshot
  │                                        ├─ CAS: metadata.json v1 → v2 ✗ (冲突!)
  │                                        ├─ 重试: 读取 metadata.json v2
  │                                        ├─ 合并变更
  │                                        ├─ CAS: metadata.json v2 → v3 ✓
  │                                        │
                                         ▼
```

**CAS（Compare-And-Swap）依赖底层存储的原子操作：**
- S3：通过条件写入（conditional put）或 DynamoDB 锁表
- HDFS：通过 `rename()` 原子操作
- 多数 Catalog 实现通过外部存储的 CAS 语义保证

**局限性：** OCC 在高写入冲突场景下性能下降（重试风暴）。Iceberg 通过 **Row-Level Locking（v3 规范实验性支持）** 和外部锁服务（如 DynamoDB lock table）缓解。

### 3.7 扩展方式

- **读扩展**：Snapshot 查询是只读操作，无中心节点，理论上无限水平扩展。
- **写扩展**：受 OCC 限制，同一表的并发写入数有限。可通过**分区写入**（不同写入者操作不同分区）提升并发度。
- **元数据扩展**：元数据文件大小随数据量增长。Iceberg 通过**快照过期（expireSnapshots）** 和 **manifest 合并**控制元数据膨胀。

### 3.8 数据跳过（Data Skipping）

Iceberg 在 manifest file 中为每个 data file 记录**列级统计信息**，使查询引擎能在规划阶段跳过无关文件：

```
manifest file 条目：
┌──────────────────────────────────────────────────────────────────┐
│ file_path  │ partition │ record_count │ column_stats              │
├────────────┼───────────┼──────────────┼───────────────────────────┤
│ 0001.parq  │ dt=01-15  │ 5000000      │ amount: {min: 10, max: 99}│
│ 0002.parq  │ dt=01-15  │ 3000000      │ amount: {min: 5,  max: 50}│
│ 0003.parq  │ dt=01-16  │ 8000000      │ amount: {min: 20, max: 200}│
└──────────────────────────────────────────────────────────────────┘

查询: WHERE amount > 100 AND dt = '2024-01-15'
  → 0001.parq: max=99, 跳过 ✓
  → 0002.parq: max=50, 跳过 ✓
  → 0003.parq: dt 不匹配, 跳过 ✓
  → 全表跳过!
```

---

## 4. Trade-offs（权衡取舍）

### 4.1 CAP 定位

Iceberg 本身**不是分布式系统**，没有独立的节点集群。CAP 定理适用于底层存储：

| 组件 | CAP 行为 |
|------|---------|
| **对象存储（S3/GCS/ADLS）** | 通常为 **AP**（可用性 + 分区容忍）。S3 早期为最终一致（2020 年后改为强一致） |
| **Catalog** | 取决于实现：Hive Metastore → 单点，REST Catalog → 依赖后端存储 |
| **Iceberg 层** | **无独立 CAP 属性**。提供 snapshot 隔离，但不保证跨写入的线性一致性 |

**结论：** Iceberg 的 CAP 属性 = 底层存储 + Catalog 的 CAP 属性。在 S3 上，由于 S3 2020 年后提供强一致性，Iceberg + S3 可接近 **CP** 行为。

### 4.2 PACELC 定位

| 场景 | Iceberg 的选择 |
|------|---------------|
| **Partition（网络分区时）** | 选择 **Availability**——写入在本地完成，通过 OCC 解决冲突 |
| **Else（正常运行时）** | 选择 **Latency**——读操作直接扫描对象存储，无额外协调开销 |

### 4.3 复杂度 vs 运维

| 优势（降低运维复杂度） | 代价（增加复杂度） |
|----------------------|-------------------|
| 无服务器组件，无需管理 Iceberg 集群 | 需要管理元数据文件生命周期（expireSnapshots） |
| 与现有计算引擎集成，零基础设施变更 | 多写入者并发冲突时 OCC 重试可能影响吞吐 |
| 开放格式，厂商锁定风险低 | Schema 演化虽灵活，但需谨慎管理列 ID |
| 快照隔离提供一致性保障 | v2 行级删除增加读取开销（需合并 delete file） |

### 4.4 成本 vs 性能

| 维度 | 低成本方案 | 高性能方案 |
|------|-----------|-----------|
| 数据文件 | 大文件（~1GB），减少 LIST 开销 | 小文件（~128MB），提高并行度 |
| 元数据 | 少保留快照，减少存储成本 | 多保留快照，支持灵活时间旅行 |
| 删除模式 | 分区级删除（重写分区） | 行级删除（v2，需额外 delete file） |
| 格式 | Parquet（高压缩） | ORC（更高压缩，但生态较小） |

### 4.5 核心权衡总结

```
                    ACID 事务
                       ▲
                       │
    无服务器 ◄─────────┼─────────► 强一致性
     架构              │           (需外部锁)
                       │
                       │
    开放格式 ◄─────────┼─────────► 写入并发
     (低锁定)           │          (OCC 限制)
                       │
                       │
    元数据存储 ◄───────┼─────────► 元数据膨胀
     成本               │          (需定期清理)
```

---

## 5. Influence & Legacy（影响与遗产）

### 5.1 湖仓一体（Lakehouse）的基石

Iceberg 与 Delta Lake、Apache Hudi 并称为**三大开放表格式**，共同奠定了 Lakehouse 架构的基础：

```
湖仓一体演进时间线
─────────────────────────────────────────────────────────────────▶

2016     2017        2018        2019        2020        2021+
  │        │           │           │           │           │
  ▼        ▼           ▼           ▼           ▼           ▼
Hudi      Delta      Iceberg     多引擎       三大表     湖仓融合
(Uber)    (Databricks)(Netflix)   支持        格式       加速
  │        │           │           │        竞争期      │
  │        │           │           │           │        │
  │        │           ├───────────┼───────────┤        │
  │        │           │ Spark/Flink/Presto/ │        │
  │        │           │ Trino/StarRocks     │        │
  │        │           │ /Doris/RisingWave   │        │
  │        │           └─────────────────────┘        │
  │                                                    │
  ▼                                                    ▼
流式数据湖          统一开放表格式成为行业事实标准
```

### 5.2 启发的后续系统与设计

| 系统/概念 | 关系 |
|-----------|------|
| **Apache Paimon** | 流式湖仓表格式，借鉴 Iceberg 快照模型 |
| **Apache Polaris Catalog** | 开源 Iceberg REST Catalog 实现（Snowflake 发起） |
| **Apache Gravitino** | 元数据联邦，支持 Iceberg + Hudi + Delta 统一管理 |
| **Tabular** | Iceberg 商业化公司（Iceberg 创始人 Ryan Blue 创立）|
| **Snowflake Iceberg Tables** | Snowflake 原生支持查询/写入 Iceberg 表 |
| **Databricks Unity Catalog + Iceberg** | Databricks 从 Delta 扩展支持 Iceberg |
| **AWS Athena + Iceberg** | AWS 一等公民支持 |

### 5.3 成为标准的设计模式

1. **快照不可变性** → 成为湖仓表格式的标准抽象
2. **元数据分层树** → manifest → manifest list → data file 的三层结构被广泛借鉴
3. **隐藏分区** → 解耦分区策略与数据存储
4. **列 ID 独立于列名** → Schema 演化的标准做法
5. **开放表格式（非厂商锁定）** → 行业趋势，推动计算存储彻底分离

### 5.4 三大表格式对比

| 维度 | Apache Iceberg | Apache Hudi | Delta Lake |
|------|---------------|-------------|------------|
| **发起方** | Netflix (2018) | Uber (2016) | Databricks (2019) |
| **设计目标** | 大规模 OLAP 查询 | 流式数据摄入 + UPSERT | 湖仓一体统一格式 |
| **快照模型** | 不可变快照树 | 时间线 + Compaction | 事务日志（Delta Log） |
| **元数据存储** | JSON/Avro 文件 | HoodieMetadata + HBase RocksDB | JSON Delta Log 文件 |
| **行级删除** | v2 支持（position + equality） | 原生支持 | 原生支持 |
| **Schema 演化** | ✅ 最完善 | 有限 | ✅ |
| **流式写入** | 有限（v3 增强中） | ✅ 最强 | ✅ |
| **CDC** | 有限 | ✅ 原生 | ✅ |
| **引擎支持** | ✅ 最广（Spark/Flink/Presto/Trino/StarRocks/Doris/Hive/ClickHouse/RisingWave） | Spark/Flink | Spark/Databricks |
| **厂商中立** | ✅ 最高 | ✅ | 较低（Databricks 主导） |

### 5.5 技术谱系位置

```
                    Hive 表格式 (2006)
                         │
                         │  痛点：无 ACID、无快照、Schema 演化困难
                         │
            ┌────────────┼────────────┐
            ▼            ▼            ▼
        Hudi (2016)  Iceberg (2018)  Delta (2019)
        Uber        Netflix         Databricks
            │            │            │
            │            ├────────────┤
            │            │            │
            ▼            ▼            ▼
        流式湖仓     开放表格式标准   Lakehouse 平台
        (Paimon)    (Polaris/Tabular) (Databricks)
```

### 5.6 当前相关性

- **行业采用**：Iceberg 已成为开放表格式中**引擎支持最广泛**的选择。Netflix、Apple、Adobe、Capital One、Snowflake、Databricks 等均为主要用户。
- **云厂商支持**：AWS（Glue Catalog + Athena）、GCP（BigLake）、Azure（Synapse）均提供一等公民支持。
- **社区活跃度**：Apache Iceberg 是 ASF 最活跃的项目之一，2024 年发布 v1.6+，v3 规范（row-level locking、position delete 优化）正在推进。
- **未来方向**：
  - **v3 规范**：行级锁（Row-Level Locking）、优化位置删除存储格式
  - **REST Catalog 标准化**（Apache Polaris）
  - **Iceberg on Streaming**：增强流式写入和 CDC 支持
  - **Iceberg REST API**：统一元数据访问协议

---

## 参考资料

1. Apache Iceberg 官方文档 — https://iceberg.apache.org/
2. Iceberg Spec v1 — https://iceberg.apache.org/spec/
3. Iceberg Spec v2 (Row-level Deletes) — https://iceberg.apache.org/spec/#deletes
4. Netflix Tech Blog: "Iceberg—Open Table Format for Huge Analytic Datasets" — https://netflixtechblog.com/
5. "Apache Iceberg: The Definitive Guide" (O'Reilly) — Daniel Weeks & Jason Dai
6. Iceberg GitHub — https://github.com/apache/iceberg
7. "Delta Lake vs Apache Hudi vs Apache Iceberg" 对比分析 — Databricks / Onehouse / Tabular 官方文档
8. AWS Big Data Blog: "How Apache Iceberg transforms data lakes on Amazon S3"
9. S3 Strong Consistency Announcement (2020-12) — https://aws.amazon.com/blogs/aws/
