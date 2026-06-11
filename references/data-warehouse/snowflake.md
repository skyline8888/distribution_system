# Snowflake

## 基本信息
- **发布时间：** 2015 年（私有预览），2016 年 GA，2020 年纽交所上市（NYSE: SNOW）
- **开发方：** Snowflake Computing（联合创始人：Benoît Dageville、Thierry Cruanes、Marcin Zukowski，分别来自 Oracle、Google/Oracle、Vectorwise/CWI）
- **类型：** 商用 SaaS（Cloud-Native Data Warehouse），闭源
- **核心参考：** [Snowflake Architecture Overview (White Paper)](https://www.snowflake.com/blog/wp-content/uploads/2015/11/Snowflake-Architecture-Overview.pdf)
- **运行平台：** AWS、Azure、GCP（三云托管）

---

## 1. Context & Motivation（背景与动机）

### 解决的问题
- **传统数仓的存算耦合困境：** Teradata、Greenplum 等 MPP 数仓将存储与计算紧密绑定在本地磁盘上。扩展计算能力必须同时扩展存储（反之亦然），导致资源浪费和成本膨胀。
- **Hadoop 生态的运维负担：** Hive/Impala/Spark SQL 等方案虽然廉价，但需要大量运维投入（HDFS 管理、YARN 调度、版本兼容），且性能和 SQL 体验远不及商业数仓。
- **多租户干扰：** 传统 MPP 系统中，不同工作负载（ETL、BI 查询、数据科学）共享同一集群，互相争抢资源，导致性能不可预测。

### 前代系统的局限性
| 局限 | Teradata/Greenplum | Hadoop 生态 |
|------|-------------------|-------------|
| 存算耦合 | ✅ 紧耦合，扩展不灵活 | ✅ HDFS + 计算节点绑定 |
| 并发隔离 | ❌ 单一集群，工作负载互相干扰 | ❌ YARN 调度，QoS 有限 |
| 运维复杂度 | 中（一体机/专有硬件） | 高（集群管理、版本升级） |
| 弹性 | 差（硬件采购周期长） | 中（可扩缩，但有延迟） |
| 数据共享 | 困难（需 ETL 或导出） | 困难 |

### 硬件/网络趋势使其成为可能
- **对象存储成熟（S3，2006）：** AWS S3 提供了无限扩展、高持久（11 个 9）、低成本的存储层，为存算分离提供了可靠底座。
- **云网络带宽提升：** 10Gbps+ 云内网降低了计算节点远程读取存储的延迟开销。
- **SSD 价格下降 + 本地 NVMe 缓存：** 计算节点可利用本地 SSD 做缓存层，缓解远程读取延迟。
- **虚拟化成熟：** 公有云 VM 使得按需启动/销毁计算节点成为常态。

**核心洞察：** Snowflake 是第一个**从第一天就完全云原生设计**的数据仓库，而非将传统架构"搬到云上"。三位创始人来自 Oracle（数据库内核）、Google（大规模分布式系统）、Vectorwise（列式分析引擎），具备了构建新一代数仓的完整技术基因。

---

## 2. Architecture（架构设计）

### 系统拓扑：三层架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    CLOUD SERVICES LAYER                          │
│  ┌───────────┐  ┌──────────────┐  ┌──────────┐  ┌────────────┐  │
│  │  Metadata │  │ Query        │  │ Access   │  │ Transaction│  │
│  │  Manager  │  │ Optimizer    │  │ Control  │  │ Manager    │  │
│  └───────────┘  └──────────────┘  └──────────┘  └────────────┘  │
│  ┌───────────┐  ┌──────────────┐  ┌──────────┐  ┌────────────┐  │
│  │  Stats    │  │ Compaction   │  │ Security │  │ Result     │  │
│  │  Collector│  │ Service      │  │ Mgmt     │  │ Cache      │  │
│  └───────────┘  └──────────────┘  └──────────┘  └────────────┘  │
│         (Stateless, horizontally scaled, always-on)             │
└─────────────────────────────┬───────────────────────────────────┘
                              │ SQL Parse / Metadata
┌─────────────────────────────┴───────────────────────────────────┐
│                    COMPUTE LAYER                                 │
│              (Virtual Warehouses — Independent)                  │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   VW #1 (BI)    │  │   VW #2 (ETL)   │  │   VW #3 (DS)    │  │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │
│  │  │ Node A    │  │  │  │ Node X    │  │  │  │ Node M    │  │  │
│  │  │ + SSD Cache│  │  │  │ + SSD Cache│  │  │  │ + SSD Cache│  │  │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │  │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │  │
│  │  │ Node B    │  │  │  │ Node Y    │  │  │  │ Node N    │  │  │
│  │  │ + SSD Cache│  │  │  │ + SSD Cache│  │  │  │ + SSD Cache│  │  │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │  │
│  │  Shared-Nothing │  │  Shared-Nothing │  │  Shared-Nothing │  │
│  │  MPP within VW  │  │  MPP within VW  │  │  MPP within VW  │  │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘  │
│           │                    │                    │            │
│  Multi-Cluster (auto-scale)   │               Independent       │
│           │                    │               workload          │
└───────────┴────────────────────┴────────────────────┬───────────┘
              │                    │                  │
              ▼                    ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER                                  │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │              Cloud Object Storage                         │     │
│  │          (S3 / Azure Blob / GCS)                         │     │
│  │                                                         │     │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │     │
│  │  │Micro-Part│ │Micro-Part│ │Micro-Part│ │Micro-Part│ ...│     │
│  │  │  ~50MB   │ │  ~50MB   │ │  ~50MB   │ │  ~50MB   │   │     │
│  │  │Columnar  │ │Columnar  │ │Columnar  │ │Columnar  │   │     │
│  │  │Compressed│ │Compressed│ │Compressed│ │Compressed│   │     │
│  │  │+Metadata │ │+Metadata │ │+Metadata │ │+Metadata │   │     │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘   │     │
│  └─────────────────────────────────────────────────────────┘     │
│                                                                 │
│  Zero-copy clones · Time Travel · Fail-safe                     │
└─────────────────────────────────────────────────────────────────┘
```

### 数据模型与命名空间
- **三层命名空间：** `Database → Schema → Table/View`
- 支持半结构化数据（JSON、Avro、Parquet、ORC、XML）通过 `VARIANT` 类型原生存储和查询，无需预定义 schema。
- 表类型：永久表（Permanent）、临时表（Temporary）、瞬态表（Transient）。

### 元数据架构归类：**集中式（Cloud Services 层）**

Snowflake 的元数据管理归属于 **Cloud Services Layer**，采用**集中式元数据服务**模式：
- 所有表的 schema 定义、micro-partition 元数据（min/max、distinct 计数、null 计数）、列统计、访问控制策略全部存储在 Cloud Services 层。
- Cloud Services 层是无状态（stateless）的水平扩展集群，但**元数据本身是单一逻辑视图**（single source of truth）。
- 计算节点（Virtual Warehouse）**不存储元数据**，只从 Cloud Services 层获取查询计划和 partition pruning 信息。

这与传统 MPP（如 Greenplum 的 Master node）类似，但区别在于：
- Cloud Services 层本身水平扩展，不是单点瓶颈。
- 计算节点可随时启停，不持有持久化状态。

### 数据架构分析

#### 分块方式：Micro-Partitions（微分区）
- Snowflake 自动将表数据切分为 **micro-partitions**，每个大小约 **50–500 MB**（压缩前）。
- **列式存储：** 每个 micro-partition 内按列存储，适合分析型查询的列扫描。
- **元数据丰富：** 每个 micro-partition 携带丰富的元数据，包括每列的 min/max 值、distinct 计数、null 值数量。这使得查询引擎可以做高效的 **partition pruning**，仅扫描需要的 micro-partitions。
- **自动维护：** 用户无需手动定义分区键或分布键。系统根据数据加载顺序自动分区，并在后台执行自动 compaction（re-clustering）。
- **按排序键聚类：** 通过 `CLUSTER BY` 指令引导物理存储的排序，提升 pruning 效果。

#### 分布策略
- 存储层（S3）内 micro-partitions 均匀分布，通过 key-value 索引定位。
- 计算层内，每个 Virtual Warehouse 的节点通过 **shared-nothing MPP** 方式并行处理分配到的 micro-partitions。

#### 冗余方式
- 存储层依赖**云对象存储的内置冗余**（S3 的跨 AZ 复制、11 个 9 持久性）。
- 计算层无状态，节点故障不影响数据持久性。
- **Fail-safe**：在 Time Travel 窗口之外，还有 7 天的不可恢复保留期（Snowflake 内部可恢复）。

#### I/O 路径
- 计算节点从云对象存储**远程读取** micro-partitions。
- 每个计算节点有**本地 SSD 缓存**，缓存近期访问的 micro-partitions，降低远程 I/O 延迟。
- 结果集通过 Cloud Services 层返回给客户端。

#### 生命周期管理
- **自动压缩：** 数据自动以列式压缩格式存储（用户不可选择算法）。
- **自动 compaction：** 后台合并小 micro-partitions，消除碎片。
- **自动 clustering：** 后台重排数据以优化查询。
- **分层存储：** 支持外部表（External Tables）直接查询 S3 数据。

---

## 3. Core Technical Innovations（核心技术创新）

### 存算完全分离（Compute-Storage Separation）

Snowflake 是第一个**从架构第一天就实现存算完全分离**的商业数据仓库：
- **存储层** = 云对象存储（S3/Blob/GCS），无限扩展、按实际使用量付费。
- **计算层** = Virtual Warehouses，可独立按需启动、停止、扩展。
- 两者通过高速云内网连接，计算节点通过读取存储层的 micro-partitions 执行查询。

这意味着：
- 存储容量不足时，只增加存储（自动发生，用户无感）。
- 计算能力不足时，只增加/扩容 Virtual Warehouse。
- 两者计费完全独立。

### Virtual Warehouses（虚拟仓库）与多集群架构

- 每个 Virtual Warehouse 是一个**独立的 MPP 计算集群**，由一组计算节点组成。
- **Shared-Nothing：** VW 内部节点之间不共享内存或磁盘，通过高速网络交换中间结果。
- **多集群并发：** 可配置 multi-cluster warehouse，根据负载自动增减集群数量。
- **工作负载隔离：** 可以为 ETL、BI、数据科学分别创建独立的 VW，互不干扰。
- **弹性：** VW 可暂停（suspend）以节省成本，也可自动恢复（auto-resume）。

### Zero-Copy Cloning（零拷贝克隆）

- 通过**元数据层面的指针复用**，instant 创建表/数据库/ schema 的完整副本。
- **无数据移动：** 克隆不复制任何实际数据，只是创建新的 micro-partition 元数据引用。
- **写时复制（Copy-on-Write）：** 克隆后对原表或克隆的修改各自独立，不影响对方。
- 典型用途：开发/测试环境搭建、数据备份、实验性分析。

### Time Travel（时间旅行）

- 可查询**任意历史时刻**的表数据（默认 1 天，企业版最长 90 天）。
- 实现机制：micro-partitions **不可变（immutable）**。数据变更（UPDATE/DELETE/MERGE）不原地修改，而是写入新的 micro-partitions。旧的 micro-partitions 保留在 Time Travel 窗口内。
- 通过 `AT` 或 `BEFORE` 子句指定时间点：
  ```sql
  SELECT * FROM orders AT (TIMESTAMP => '2024-01-15 12:00:00'::TIMESTAMP);
  ```
- **Fail-safe：** Time Travel 窗口到期后，数据进入 7 天 Fail-safe 期（仅 Snowflake 可恢复，用于灾难恢复）。

### Cloud Services 层的集中式协调

Cloud Services Layer 是整个系统的"大脑"，提供：
- **查询解析与优化：** SQL 解析、基于成本的优化器（CBO）利用 micro-partition 元数据进行 partition pruning。
- **事务管理：** ACID 语义，支持并发读写的一致性。
- **访问控制：** RBAC、行级安全策略、列级掩码。
- **元数据管理：** 表 schema、统计信息、micro-partition 索引。
- **结果缓存：** 相同查询的结果缓存 24 小时，零计算开销返回。

### 半结构化数据原生支持

- `VARIANT` 类型可直接存储 JSON/Avro/Parquet 等半结构化数据。
- 查询时通过 `:` 操作符直接访问嵌套字段，无需提前展平。
- 自动推断 schema，支持 schema-on-read。

### 数据共享（Data Sharing / Snowflake Marketplace）

- 跨账户（甚至跨云区域）的安全数据共享，无需 ETL 或数据复制。
- 提供者授予访问权限，消费者直接查询提供者的数据（只读视图）。
- 底层仍是 zero-copy：只共享元数据指针，不移动实际数据。

---

## 4. Trade-offs (CAP / PACELC)

### CAP 定位：**CP（Consistency + Partition Tolerance）**

- **强一致性：** 在同一个 Snowflake 账户内，所有读写操作遵循 ACID 语义。查询始终看到最新的已提交数据。
- **实现方式：** Cloud Services 层维护全局事务日志和元数据，写入操作通过集中式协调确保顺序一致。
- **牺牲可用性？** 在 Cloud Services 层不可用时（极端情况），写入操作会被阻塞直到恢复。但读取操作可通过缓存和存储层的可用性继续服务。
- **注意：** 跨账户的数据共享是最终一致的（replication 有延迟），但账户内部是强一致的。

### PACELC 定位

| 场景 | 选择 | 说明 |
|------|------|------|
| **分区时 (P)** | **C** (Consistency) | 账户内强一致，宁可延迟写入也不返回旧数据 |
| **正常时 (EL)** | **L** (Latency) 为主，兼顾 E | 通过结果缓存、SSD 缓存层、分区裁剪降低延迟 |

**解读：**
- 分区时选 C：Snowflake 优先保证一致性。写入操作的可见性遵循线性化（linearizable）语义。
- 正常时偏向 L：通过多层缓存（结果缓存 24h、计算节点 SSD 缓存）、智能分区裁剪、向量化执行来降低查询延迟。但因为是集中式协调（Cloud Services），写路径的延迟高于完全分布式系统。

### 复杂度 vs 运维简易性

| 维度 | 取舍 |
|------|------|
| **对用户** | 极低运维复杂度。无需管理集群、分区、索引、vacuum。全托管 SaaS。 |
| **对 Snowflake** | 极高内部复杂度。需要构建完整的元数据服务、查询优化器、多租户隔离、跨云兼容。 |
| **透明度** | 闭源，用户无法深入调优内部机制（如压缩算法、缓存策略）。 |

### 成本 vs 性能

| 优势 | 劣势 |
|------|------|
| 按秒计费的计算资源，不运行不花钱 | 存储和计算分离增加了网络 I/O 开销 |
| 自动扩缩容，按需付费 | 大量冷数据查询可能较慢（相比本地 NVMe） |
| 多 VW 隔离避免资源争抢 | 跨云数据传输费用可能较高 |
| 结果缓存减少重复计算开销 | 高级功能（Time Travel 90 天、multi-cluster）需要 Enterprise+ 版本 |

**成本优化模式：**
- 自动挂起/恢复 Virtual Warehouse（默认 10 分钟空闲后挂起）。
- 利用结果缓存避免重复查询。
- 合理选择 VW size（X-Small 到 6X-Large）。
- 使用 Search Optimization Service 替代盲目加大 VW。

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统

Snowflake 的存算分离架构已成为**云数据仓库的标准范式**：

| 系统 | 与 Snowflake 的关系 |
|------|-------------------|
| **Google BigQuery** | 早期已有存算分离理念，但 Snowflake 的多 VW 隔离和 zero-copy cloning 影响了其后续的多租户优化 |
| **Databricks SQL / Lakehouse** | 借鉴了存算分离 + 多租户隔离的模式，结合 Delta Lake 实现湖仓一体 |
| **Amazon Redshift RA3** | 2020 年引入 Managed Storage，本质上是向 Snowflake 式存算分离演进 |
| **阿里云 AnalyticDB / Hologres** | 存算分离架构，多租户隔离 |
| **字节 ByteHouse** | 存算分离 + 弹性计算节点 |
| **StarRocks / Doris** | 部分版本引入存算分离部署模式 |
| **MotherDuck** | 将 DuckDB 的本地性能与 S3 的存算分离结合 |

### 成为行业标准的设计模式

1. **存算分离（Compute-Storage Separation）：** 从"可选架构"变为"云数仓默认架构"。几乎所有新数据仓库都采用此模式。
2. **Micro-Partition + 自动分区裁剪：** 用户无需手动分区，系统自动管理。Delta Lake、Iceberg 的 snapshot 机制也借鉴了类似思路。
3. **Zero-Copy Cloning：** 通过元数据指针实现 instant 克隆，成为数据治理/开发测试的标准模式。Databricks Delta Clone 直接对标此功能。
4. **Time Travel / Snapshot Isolation：** 不可变数据 + 时间回溯查询，Delta Lake 的 time travel、Iceberg 的 snapshot 都是同类思想。
5. **多租户工作负载隔离：** 独立的 Virtual Warehouse 隔离不同负载，避免了"一个慢查询拖垮整个集群"的问题。
6. **SaaS 化数仓：** "零运维"的数据仓库体验，重塑了企业对数仓的预期。

### 技术谱系位置

```
                    SQL 分析引擎
                    /          \
              Oracle        Vectorwise
           (Dageville)      (Zukowski)
                 \            /
                  \          /
              Snowflake Computing (2012)
                        │
                        │ 云原生三层架构
                        │ 存算分离
                        │ Micro-Partitions
                        │
            ┌───────────┼───────────┐
            ▼           ▼           ▼
        BigQuery    Redshift    Databricks
        (强化)      RA3 演进    Lakehouse SQL
            │           │           │
            ▼           ▼           ▼
      现代云数仓标准：存算分离 + 多租户隔离 + 自动优化
```

**思想传播路径：**
- Oracle（传统数仓内核经验）+ Vectorwise（列式分析引擎研究）+ Google（大规模分布式系统）→ Snowflake 三层架构。
- Snowflake → Redshift RA3（Amazon 被迫回应）→ 传统数仓的云原生转型。
- Snowflake 的 zero-copy / time travel → Delta Lake / Iceberg 的 snapshot 机制 → 湖仓一体表格式标准。
- Snowflake 的 Data Sharing → 数据市场（Data Marketplace）概念 → 数据协作网络。

### 当前相关性和使用场景

- **定位：** 中高端企业数据分析平台，尤其适合需要多工作负载隔离、弹性伸缩、低运维的场景。
- **竞争格局：** 与 Databricks（Lakehouse）、BigQuery（GCP 原生）、Redshift（AWS 原生）形成"云数仓四强"。
- **趋势：** Snowflake 正在从"数据仓库"向"数据云"（Data Cloud）扩展——增加 Snowpark（Python/Java/Scala UDF）、Streamlit（数据应用）、Iceberg Tables（开放表格式）、Unistore（HTAP）等功能，模糊数仓与数据湖的边界。
- **风险：** 计算层网络 I/O 瓶颈在极大规模数据集上仍然显著；对云厂商锁定（虽然是多云，但每个部署绑定单一云）；闭源导致无法自定义优化。

---

## 参考文档

- [Snowflake Architecture Overview (官方白皮书)](https://www.snowflake.com/blog/wp-content/uploads/2015/11/Snowflake-Architecture-Overview.pdf)
- [Snowflake Documentation — Architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts)
- [Snowflake Documentation — Micro-partitions](https://docs.snowflake.com/en/user-guide/tables-micro-partitions)
- [Snowflake Documentation — Time Travel](https://docs.snowflake.com/en/user-guide/data-time-travel)
- [Snowflake Documentation — Zero-Copy Cloning](https://docs.snowflake.com/en/user-guide/object-clone)
- [Dageville, B. et al. "The Snowflake Elastic Data Warehouse." SIGMOD 2016.](https://doi.org/10.1145/2882903.2903741)
