# Apache Hudi

## 基本信息

| 维度 | 详情 |
|------|------|
| **初始发布** | 2019 年开源（Uber 内部 2018 年启动） |
| **开发方** | Uber Technologies, Inc. → Apache Software Foundation |
| **开源协议** | Apache 2.0 |
| **顶级项目** | 2020 年晋升 Apache TLP |
| **核心论文** | *Hudi: A Cloud-Native Data Lakehouse Platform* (CIDR 2023) |
| **定位** | Table Format for Data Lakes — 在对象存储之上提供 ACID 事务、upsert、CDC 能力 |
| **名称来源** | **H**adoop **U**pdates, **D**eletes, and **I**ncrements |

---

## 1. Context & Motivation（背景与动机）

### 1.1 解决的问题

Uber 在 2018 年面临的核心痛点：

- **数据湖无法更新** — 基于 HDFS/S3 + Hive 的传统数据湖架构只支持 append-only，业务需要行级 upsert/delete（司机/乘客资料修正、订单状态更新、合规删除/GDPR）。
- **CDC 管道断裂** — 业务数据库（MySQL/PostgreSQL）的变更数据捕获流（Debezium/Canal 类工具）落入数据湖后无法 merge，只能全量覆盖或维护额外的 KV 层（HBase）。
- **增量计算困难** — 下游 ETL 每次都要全量扫描 PB 级数据，无法只读取「自上次处理以来新增/变更」的数据。
- **多引擎一致性** — Spark 批处理、Flink 流处理、Presto/Hive 查询需要共享同一份数据，且各自看到一致的快照。

### 1.2 前代系统的局限

| 前代方案 | 局限 |
|----------|------|
| Hive + INSERT OVERWRITE | 全表重写，O(全表) 成本，无法行级更新 |
| HBase 做更新层 | 双写（Hive + HBase），数据不一致，运维复杂 |
| Sqoop 全量导入 | 无法处理增量，延迟高 |
| Kafka → Consumer | 只有流式层，缺少持久化表语义 |

### 1.3 硬件/网络背景使其可能

- **对象存储成熟**（S3、OSS、COS）：提供高持久度、低成本的海量存储，但本身只提供 put/get，无事务语义。
- **计算/存储分离**成为云原生范式：需要一种"智能文件格式"在不改变底层存储的前提下增加表级别语义。
- **流处理引擎成熟**：Flink/Spark Structured Streaming 提供了持续处理的运行时，Hudi 在此之上叠加表格式。

---

## 2. Architecture（架构设计）

### 2.1 系统拓扑

Hudi 是 **嵌入式表格式库**（embedded table format library），不是独立的服务进程。它嵌入在计算引擎（Spark/Flink）中运行，直接操作底层对象存储上的文件。

```
┌─────────────────────────────────────────────────────────────────┐
│                        Computation Engine                        │
│                   (Spark / Flink / Java App)                     │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                    Hudi Client API                          │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────────┐ │ │
│  │  │  Writer   │ │  Index   │ │ Timeline │ │  HoodieTable  │ │ │
│  │  │(Upsert/  │ │(Bloom/   │ │(Commit/  │ │  Abstraction  │ │ │
│  │  │ Insert/  │ │ HBase/   │ │ Clean/   │ │               │ │ │
│  │  │ Delete)  │ │ Simple)  │ │ Rollback)│ │               │ │ │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └───────┬────────┘ │ │
│  │       │             │             │               │          │ │
│  └───────┼─────────────┼─────────────┼───────────────┼──────────┘ │
└──────────┼─────────────┼─────────────┼───────────────┼────────────┘
           │             │             │               │
           ▼             ▼             ▼               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Storage Layer                               │
│              (S3 / HDFS / OSS / GCS / ADLS)                      │
│                                                                  │
│  <table-base-path>/                                             │
│  ├── .hoodie/              ← 元数据目录                          │
│  │   ├── hoodie.properties  ← 表配置                            │
│  │   ├── *.commit           ← 提交时间线                         │
│  │   ├── *.clean            ← 清理记录                           │
│  │   ├── *.rollback         ← 回滚记录                           │
│  │   └── *.deltacommit      ← MoR delta 提交                      │
│  ├── <partition-1>/         ← 分区目录                            │
│  │   ├── *.parquet           ← 基列文件 (Base File)              │
│  │   └── *.log               ← 增量日志 (Log File, MoR only)     │
│  └── <partition-2>/         ← 分区目录                            │
│       └── ...                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 数据模型与核心概念

#### HoodieRecord + HoodieKey

每条记录在 Hudi 中被封装为 `HoodieRecord`，其唯一标识是 `HoodieKey`：

```
HoodieRecord
├── HoodieKey
│   ├── recordKey: String    ← 业务主键（如 user_id, order_id）
│   └── partitionPath: String ← 分区路径（如 dt=2024-01-15）
├── data: HoodieRecordPayload  ← 实际数据（GenericRecord/Avro/Row）
└── currentLocation / newLocation  ← 文件内位置（索引使用）
```

`recordKey` 是行级 upsert/delete 的核心：Hudi 通过索引定位到 `recordKey` 所在的物理文件，然后决定是追加还是原地修改。

#### 两种存储模型

**Copy-on-Write (CoW)**

```
Update(record R):
  1. 找到 R 所在的 Parquet 文件 F
  2. 读取 F 到内存
  3. 更新 R 的记录
  4. 写入全新的 Parquet 文件 F'
  5. 在 Timeline 中记录 commit，指向 F'
  6. 清理时删除 F
```

- 每次更新**重写整个 Parquet 文件**（包含未变更的行）
- 读路径简单：直接读最新的 Parquet 文件
- **适合读多写少的场景**（报表、BI 查询）

**Merge-on-Read (MoR)**

```
Update(record R):
  1. 将 R 的新版本追加到 delta log 文件（.log，Avro 格式）
  2. 基列文件（.parquet）不变
  3. Timeline 记录 deltacommit

Read(record R):
  1. 读取基列文件中 R 的旧版本
  2. 读取所有 delta log 中 R 的新版本
  3. 在内存中 merge（log 覆盖 base）
  4. 返回最终结果

Compaction（异步/同步）:
  1. 将 delta log 合并回 Parquet 基列文件
  2. 产生新的 commit
```

- 更新只写增量日志（append-only），**不重写基列文件**
- 读时需要 merge base + delta，有额外 CPU 开销
- **适合写多读少的场景**（实时 CDC、流式摄入）
- 支持异步压缩（compaction），将 delta 合并回 base

**对比矩阵**

| 维度 | Copy-on-Write | Merge-on-Read |
|------|--------------|---------------|
| 写入放大 | 高（重写整个文件） | 低（仅写 delta log） |
| 读取延迟 | 低（直接读 Parquet） | 中（需 merge base + log） |
| 文件大小 | 大（频繁重写产生新快照） | 小（delta 追加） |
| 适用场景 | 读多写少、批处理 | 写多读少、流式/CDC |
| 存储成本 | 较高 | 较低（压缩后） |

### 2.3 时间线（Timeline）— 元数据架构

Hudi 的元数据核心是 **Timeline**：按时间顺序排列的所有操作记录，保存在 `.hoodie/` 目录下。

```
Timeline（时间轴，从早到晚）:

  t=20240115100000    t=20240115100500    t=20240115101000    t=20240115101500
  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │   COMMIT     │ →  │   COMMIT     │ →  │  DELTACOMMIT │ →  │    CLEAN     │
  │ (CoW batch   │    │ (CoW batch   │    │ (MoR delta   │    │ (remove old  │
  │  ingestion)  │    │  upsert)     │    │  append)     │    │  files)      │
  └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                                                                      
  额外动作类型：
  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │  COMPACTION  │    │   ROLLBACK   │    │   REPLACE    │
  │ (MoR base    │    │ (撤销指定    │    │ (Clustering, │
  │  file merge) │    │  commit)     │    │  Optimizing) │
  └──────────────┘    └──────────────┘    └──────────────┘
```

每个 Timeline 动作都是一个带时间戳的文件，记录：
- 操作类型（commit/deltacommit/clean/rollback/compaction/replace）
- 涉及的文件列表（新增/删除）
- 统计信息（写入行数、文件大小等）
- 校验和（用于完整性验证）

**元数据架构归类**：**外部引擎式**（元数据以文件格式存储在对象存储/HDFS 上，计算引擎读取这些文件来重建表状态）。Hudi 0.11+ 引入了 **Metadata Table**（基于 Hudi 自身格式存储的元数据表），将文件列表、列统计等从 Timeline 扫描中加速出来，避免了列出大目录的开销。

### 2.4 索引机制

Hudi 需要高效地将 `HoodieKey(recordKey, partitionPath)` 映射到物理文件位置。支持多种索引：

| 索引类型 | 原理 | 适用场景 |
|----------|------|----------|
| **Bloom Filter Index** | 每个 Parquet 文件脚注嵌入 Bloom Filter，`recordKey` 做 membership 测试 | 默认索引，内存开销小，适合大表 |
| **Simple Index** | 在写入时直接扫描文件定位 recordKey | 小表、测试用 |
| **HBase Index** | 将 recordKey → 文件位置映射存入 HBase | 需要精确快速查找的场景 |
| **Bucket Index** (0.10+) | 按 recordKey 哈希分桶，每个桶一个文件 | 大规模表，替代 Bloom |
| **RockDB Index** (0.12+) | 本地 RocksDB 做键值映射 | 写入加速，避免分布式查找 |

索引是 Hudi 写入性能的关键瓶颈：一次 upsert 需要先通过索引确定记录是否已存在、在哪个文件。

### 2.5 数据架构分析

| 维度 | Hudi 的设计 |
|------|------------|
| **分块方式** | 以 Parquet/ORC 文件为基本单位（~128MB-1GB），非固定大小块 |
| **分布策略** | 依赖 Hive 风格分区（`partitionKey=value`），由用户定义分区键 |
| **冗余方式** | 取决于底层存储（S3 三副本、HDFS 三副本等），Hudi 自身不管理副本 |
| **I/O 路径** | 通过 Hadoop FileSystem / S3 SDK 直接读写对象存储 |
| **生命周期** | Timeline + Clean 操作自动清理过期文件快照；支持保留 N 个最新提交 |

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 ACID 事务语义

Hudi 在**无事务的对象存储之上**实现了表级 ACID：

- **原子性**：commit 文件是原子写入的（通过文件系统原子 rename 或 S3 条件写入）。读引擎只承认已完成的 commit。
- **一致性**：每次 commit 记录完整的文件变更集（新增文件 + 删除文件），保证快照一致性。
- **隔离性**：基于 Timeline 快照隔离（Snapshot Isolation），读者看到某一时刻的完整快照，不受并发写入干扰。
- **持久性**：文件写入对象存储即持久化（S3/3FS/HDFS 各有其持久性保证）。

### 3.2 增量查询 API（Incremental Query）

Hudi 最具创新性的能力之一：

```sql
-- Spark SQL: 读取自指定 commit 以来的所有变更
SELECT * FROM hudi_table
WHERE _hoodie_commit_time > '20240115100000'

-- 等价于 CDC 的 binlog 订阅
-- 返回的是"变更后的最终状态"，而非变更事件本身
```

这使得下游 ETL 可以只处理增量数据，避免全表扫描。配合 Flink 可以实现**流式 CDC 管道**：

```
MySQL Binlog → Debezium → Kafka → Flink + Hudi → S3/HDFS
                                            ↓
                                    Presto/Hive 查询
```

### 3.3 并发写入控制

- **乐观锁**（Optimistic Concurrency Control）：多个 writer 同时写入时，通过 Timeline 文件检查冲突，失败的 writer 重试。
- **写入者 ID**：每个 writer 有唯一标识，避免自我冲突。
- **分布式锁**（可选）：基于 ZooKeeper/Hadoop Lock/元数据表实现 writer 间的互斥。

### 3.4 数据聚类（Clustering / Optimization）

后台异步优化操作：

- **小文件合并**：将大量小 Parquet 文件合并为大文件，提升查询效率
- **数据排序**：按特定列对数据重新排序，优化谓词下推（Predicate Pushdown）
- **文件布局优化**：根据查询模式重新组织数据物理布局

### 3.5 Schema 演进

支持有限的 Schema 变更：
- 添加列（add column）
- 删除列（drop column）
- 列类型变更（有限制）
- Schema 变更记录在 Timeline 中

### 3.6 流式写入（Flink Integration）

Hudi 提供了 Flink Source 和 Sink：
- **Flink Sink**：接收 ChangeLog Stream（INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE），自动映射为 Hudi 的 upsert/delete 操作
- **Flink Source**：作为流数据源，订阅 Hudi 表的增量变更
- **Exactly-Once**：利用 Flink Checkpoint + Hudi Timeline 实现端到端精确一次语义

---

## 4. Trade-offs（CAP / PACELC）

### 4.1 CAP 定位

**Hudi 本身不直接参与 CAP 三角**，因为 Hudi 是表格式库而非分布式服务。CAP 的选择取决于底层存储：

| 底层存储 | CAP 行为 |
|----------|----------|
| **HDFS** | CP（NameNode 单点时偏向一致性） |
| **S3** | AP（高可用，最终一致性 → 现已改进为强一致性） |
| **OSS/COS** | 类似 S3，取决于具体实现 |

Hudi 自身的**快照隔离**设计偏向 **Consistency**：读者始终看到某个完整 commit 的一致性快照，不会读到半写入的数据。

### 4.2 PACELC 定位

| 场景 | 选择 | 说明 |
|------|------|------|
| **分区时（P）** | 选 **A** | 写入不阻塞读取；旧快照仍可查询 |
| **正常时（E）** | 选 **L** | 读取最新快照有轻微延迟（需等待当前 commit 完成） |

**PACELC 总结：AP / EL**

### 4.3 关键权衡

| 权衡 | 取舍 | 说明 |
|------|------|------|
| **CoW 写入放大 vs 读取延迟** | 高写入成本 ↔ 低读取成本 | 选 MoR 可降低写入放大，但增加读取时 merge 开销 |
| **Bloom Filter 精度 vs 内存** | 假阳性率 ↔ Bloom 大小 | 假阳性导致不必要的文件扫描 |
| **增量查询 vs 全量查询** | 灵活性 ↔ 存储开销 | 需要保留历史快照才能支持增量查询 |
| **异步 Compaction vs 读取一致性** | 写入吞吐 ↔ 读取延迟 | 未压缩的 delta 文件累积会拖慢读取 |
| **索引查找 vs 写入吞吐** | 定位精度 ↔ 写入速度 | Bloom 是概率性，HBase 精确但有网络开销 |

### 4.4 复杂度 vs 运维

- **部署简单**：无需额外服务进程，随计算引擎部署
- **运维复杂**：需要管理 Timeline 清理、Compaction 调度、Clustering 策略
- **调优参数多**：50+ 配置参数，涉及文件大小、索引、压缩、并发等

### 4.5 成本 vs 性能

- **低成本存储**：直接使用对象存储，存储成本极低
- **计算成本**：Compaction、Clustering、Index 查找都需要额外计算资源
- **存储放大**：CoW 模式下更新产生的文件冗余会增加存储成本；MoR 模式下 delta 文件也会累积

---

## 5. Influence & Legacy（影响与遗产）

### 5.1 开创性贡献

Apache Hudi 是**数据湖表格式的先驱**（与 Delta Lake 几乎同期）：

1. **首个在数据湖上实现行级 Upsert/Delete** — 打破了数据湖只能 append-only 的限制
2. **首个将 CDC 语义引入数据湖** — 使 binlog 流可以直接落地到数据湖并查询
3. **增量查询 API** — 启发了后续表格式（Iceberg 的 Incremental Scan、Delta 的 Change Data Feed）

### 5.2 技术谱系

```
                    Google File System (2003)
                            │
                    Apache Hadoop / HDFS (2006)
                            │
                    Apache Hive (2010)
                            │
              ┌─────────────┴─────────────┐
              │  Append-only 数据湖架构    │
              │  无法行级更新               │
              └─────────────┬─────────────┘
                            │
              Uber 内部需求 (2018):
              行级 upsert + CDC + 增量查询
                            │
                    ┌───────┴───────┐
                    │  Apache Hudi  │ ← 2019 开源, 2020 Apache TLP
                    │  (CoW + MoR)  │
                    └───────┬───────┘
                            │
            ┌───────────────┼───────────────┐
            │               │               │
      启发 Delta Lake   启发 Iceberg   启发 Paimon
      (Databricks)    (Netflix)     (Apache/阿里)
      (2019)          (2020)        (2023)
            │               │               │
            └───────────────┼───────────────┘
                            │
                    Lakehouse 架构
                    (统一批流 + ACID)
```

### 5.3 成为标准的设计模式

| 模式 | 源自 Hudi | 后续采用者 |
|------|-----------|-----------|
| Timeline-based 版本管理 | ✅ | Delta (Transaction Log), Iceberg (Metadata JSON) |
| CoW / MoR 双模式 | ✅ | Delta (CoW), Paimon (CoW + MoR) |
| 增量查询 API | ✅ | Iceberg (Incremental Scan), Delta (CDF) |
| 后台 Compaction | ✅ | Delta (Optimize), Paimon (Compaction) |
| 小文件自动合并 | ✅ | Delta (Auto Optimize), Iceberg (Rewrite) |
| 嵌入式表格式（无独立服务） | ✅ | Iceberg, Delta, Paimon |

### 5.4 生态集成

Hudi 拥有最广泛的查询引擎支持：

```
┌──────────────────────────────────────────────────┐
│                计算引擎层                          │
│                                                  │
│  ┌────────┐ ┌────────┐ ┌──────┐ ┌─────┐        │
│  │ Spark  │ │ Flink  │ │ Java │ │  SDK │        │
│  │(读写)  │ │(读写)  │ │ App  │ │     │        │
│  └───┬────┘ └───┬────┘ └──┬───┘ └──┬──┘        │
│      │          │          │         │           │
│      └──────────┴────┬─────┴─────────┘           │
│                      │                            │
│              ┌───────┴───────┐                     │
│              │  Apache Hudi  │  ← Table Format     │
│              │  (CoW / MoR)  │                     │
│              └───────┬───────┘                     │
│                      │                             │
└──────────────────────┼─────────────────────────────┘
                       │
┌──────────────────────┼─────────────────────────────┐
│              查询引擎层                              │
│                                                  │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────────┐ │
│  │  Hive  │ │ Presto │ │ Trino  │ │  ClickHouse│ │
│  │(读/写) │ │  (读)  │ │  (读)  │ │  (读)      │ │
│  └────────┘ └────────┘ └────────┘ └────────────┘ │
│  ┌────────┐ ┌────────┐ ┌────────┐                 │
│  │  StarR │ │ Doris  │ │ Athena │                 │
│  │  ocks  │ │        │ │        │                 │
│  └────────┘ └────────┘ └────────┘                 │
└──────────────────────────────────────────────────┘
                       │
┌──────────────────────┼─────────────────────────────┐
│              存储层                                  │
│                                                  │
│   S3  │  HDFS  │  OSS  │  COS  │  ADLS  │  GCS    │
│                                                  │
└──────────────────────────────────────────────────┘
```

### 5.5 当前相关性（2024-2026）

- **Apache Hudi 持续活跃开发**：最新版本 1.0+（2024 发布），引入 Row-based 存储格式（Hudi Row Format）、Secondary Index、Time Travel 等
- **Flink 集成是差异化优势**：在三表格式中，Hudi 的 Flink 支持最成熟（原生 Flink CDC 集成）
- **与 Iceberg 的竞争**：Iceberg 在查询引擎支持和社区增长上领先，Hudi 在流式写入和 CDC 场景保持优势
- **企业采用**：Uber、Twitter、AWS、Tencent、Alibaba 等在生产环境大规模使用
- **湖仓一体趋势**：Hudi 是 Lakehouse 架构的核心组件之一，与 Iceberg、Delta Lake 共同推动数据湖从"数据沼泽"向"事务性数据平台"演进

### 5.6 与 Delta Lake / Iceberg 的核心差异

| 维度 | Hudi | Delta Lake | Iceberg |
|------|------|------------|---------|
| **发起方** | Uber | Databricks | Netflix |
| **核心优势** | CDC + 流式写入 | Databricks 生态 + 性能 | 引擎无关 + 规范清晰 |
| **存储模型** | CoW + MoR | CoW（主）+ CDF | 快照隔离（无 CoW/MoR 概念） |
| **元数据格式** | Timeline 文件 | Delta Log (JSON + Parquet) | Metadata JSON + Manifest |
| **Flink 支持** | ✅ 原生成熟 | ✅ (Databricks) | ✅ |
| **索引** | Bloom/HBase/Bucket/RocksDB | 无内建索引 | 无内建索引（依赖 Manifest 过滤） |
| **Incremental Read** | ✅ 原生 | ✅ (Change Data Feed) | ✅ (Incremental Scan) |
| **社区治理** | Apache TLP | Linux Foundation | Apache TLP |

---

## 参考资料

1. *Hudi: A Cloud-Native Data Lakehouse Platform*, CIDR 2023 — [官方论文](https://www.cidrdb.org/cidr2023/papers/p83-balaraman.pdf)
2. Apache Hudi 官方文档 — [https://hudi.apache.org](https://hudi.apache.org)
3. Uber Engineering Blog: *Introducing Hudi* (2019) — [uber.engineering/hudi](https://www.uber.com/blog/introducing-hudi/)
4. Hudi 架构详解 — *How Hudi Works* (官方文档)
5. 三大表格式对比分析 — Delta vs Iceberg vs Hudi
