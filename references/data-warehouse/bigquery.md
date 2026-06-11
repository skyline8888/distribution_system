# Google BigQuery

## 基本信息
- **发布时间：** 2010 年（GA）
- **开发方：** Google
- **性质：** 商用 SaaS（serverless cloud data warehouse）
- **核心论文：**
  - *Dremel: Interactive Analysis of Web-Scale Datasets* (VLDB 2010)
  - *Colossus: Google's Next-Generation Storage System* (公开资料有限)
  - *Jupiter Evolving: Transforming Google's Datacenter Network Fabric* (SIGCOMM 2015)
- **底层论文：** Bigtable (OSDI 2006), MapReduce (OSDI 2004), Capacitor (Google 内部)

---

## 1. Context & Motivation（背景与动机）

### 解决的问题
在 BigQuery 之前，企业对 PB 级数据的分析面临三个核心痛点：
1. **运维成本极高**：传统数仓（Teradata、Greenplum）需要专职 DBA 团队管理集群、调优参数、做容量规划。
2. **弹性不足**：存储与计算耦合，扩容需要停机或手动加节点，无法应对波动的分析负载。
3. **查询延迟高**：基于 MapReduce 的批处理范式（Hive）动辄需要分钟到小时级响应，无法支持交互式探索。

### 前代系统的局限性
| 前代系统 | 局限性 |
|----------|--------|
| Teradata / Greenplum | 存算耦合、需预置容量、管理复杂 |
| Hadoop + Hive | 批处理延迟高、SQL 支持不完整、无交互式体验 |
| 早期 Redshift (2013) | 仍需选择实例规格、手动 VACUUM/ANALYZE |

### 硬件/网络背景
BigQuery 的设计得益于 Google 内部基础设施的成熟：
- **万兆数据中心网络**：Jupiter 网络 fabric 提供了跨机架 Tbps 级带宽，使得大规模数据 shuffle 成为可能。
- **廉价大容量磁盘**：HDD 成本持续下降，支持 PB 级列式存储。
- **多核 CPU 普及**：服务器从 4 核走向 48+ 核，为向量化执行和并行扫描提供了硬件基础。

Google 的动机很明确：**将 Dremel 的内部能力产品化**，让任何客户无需管理基础设施即可进行交互式分析——即「Serverless Analytics」的开创性愿景。

---

## 2. Architecture（架构设计）

### 系统拓扑

BigQuery 采用**完全分离的三层架构**：

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                        CLIENT / API LAYER                        │
  │  SQL Editor / BI Tools / REST API / Client Libraries             │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │ HTTPS/gRPC
  ┌──────────────────────────────▼───────────────────────────────────┐
  │                      CONTROL / PLAN LAYER                        │
  │  ┌────────────────────────────────────────────────────────────┐  │
  │  │  Query Frontend  →  SQL Parser  →  Optimizer  →  Planner   │  │
  │  └────────────────────────────────────────────────────────────┘  │
  │                                                                   │
  │  ┌────────────────────────────────────────────────────────────┐  │
  │  │                    Slot Manager                            │  │
  │  │  (Dynamic resource allocation per query/project/reservation)│  │
  │  └────────────────────────────────────────────────────────────┘  │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │ Execution Tree
  ┌──────────────────────────────▼───────────────────────────────────┐
  │                    EXECUTION LAYER (Dremel)                      │
  │                                                                  │
  │   ┌─────────────┐                                                │
  │   │   Root (0)   │  ← Aggregates final results                   │
  │   └──────┬───────┘                                                │
  │          │                                                        │
  │   ┌──────▼───────┐  ┌──────────────────┐                         │
  │   │  Mix Node    │  │    Mix Node      │  ← Intermediate          │
  │   │  (L1)        │  │    (L1)          │     aggregation         │
  │   └──┬───┬───┬──┘  └──┬───┬───┬──────┘                         │
  │      │   │   │         │   │   │                                 │
  │  ┌───▼┐ ┌▼┐ ┌▼┐   ┌───▼┐ ┌▼┐ ┌▼┐                               │
  │  │Leaf│ │L│ │L│   │Leaf│ │L│ │L│  ← Scans Capacitor columns     │
  │  │Node│ │ │ │ │   │Node│ │ │ │ │    & returns partial results   │
  │  └────┘ └─┘ └─┘   └────┘ └─┘ └─┘                               │
  │                                                                  │
  │  Multi-level serving tree (fan-out per level)                   │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │ Read blocks
  ┌──────────────────────────────▼───────────────────────────────────┐
  │                    STORAGE LAYER (Colossus/Capacitor)            │
  │                                                                  │
  │  ┌────────────────────────────────────────────────────────────┐  │
  │  │  Colossus: Distributed storage (replicated blocks)          │  │
  │  │  Capacitor: Columnar format with compression & encoding     │  │
  │  │  (record-level encoding, run-length, delta, dictionary)     │  │
  │  └────────────────────────────────────────────────────────────┘  │
  │                                                                  │
  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐   │
  │  │  Tablet A  │ │  Tablet B  │ │  Tablet C  │ │  Tablet D  │   │
  │  │ [col1..N]  │ │ [col1..N]  │ │ [col1..N]  │ │ [col1..N]  │   │
  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘   │
  └──────────────────────────────────────────────────────────────────┘
```

### 数据模型与命名空间
- **三层命名空间**：`Project → Dataset → Table`
- 支持标准 SQL (SQL:2011 子集)，包括嵌套/重复字段（STRUCT、ARRAY）——继承自 Bigtable/Protocol Buffers 的数据模型。
- 分区表（按日期/整数范围）和聚簇表（clustering）用于剪枝。

### 元数据架构归类
**集中式分区（Centralized Partitioned）**：
- 元数据（表 schema、分区信息、权限）由中央元数据服务管理。
- 元数据按 Dataset 分片，每个分片由专用元数据节点服务。
- 由于是 Serverless 产品，元数据管理对终端用户完全透明。

### 数据架构分析

| 维度 | BigQuery 设计 |
|------|--------------|
| **分块方式** | Capacitor 列式文件，按列存储；每个 block 含记录级编码（RLE、delta、bit-packing、dictionary） |
| **分布策略** | 数据自动分布在 Colossus 集群上，按 range/hash 分区；Leaf Node 就近读取本地/近端 block |
| **冗余方式** | Colossus 提供多副本复制（通常 3 副本），自动修复损坏块 |
| **I/O 路径** | Leaf Node → 本地文件系统缓存 → Colossus RPC → 磁盘/内存；支持预读和向量化扫描 |
| **生命周期** | 自动分层（热数据 SSD/内存，冷数据 HDD）、TTL 策略、长期存储折扣定价 |

### Shuffle 服务
对于涉及 JOIN、GROUP BY、窗口函数的复杂查询，BigQuery 使用**分布式 Shuffle Service**：
1. 中间结果写入暂存区（内存优先，溢出到 Colossus）。
2. 通过 Jupiter 网络 fabric 进行跨节点数据重分布。
3. Shuffle 对优化器透明，由执行引擎自动选择 hash-shuffle 或 sort-shuffle。

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 Dremel 多级服务树（Multi-Level Serving Tree）
BigQuery 的查询执行基于 Dremel 论文中的核心设计：

- **树形拓扑**：Root → Mix Nodes → Leaf Nodes 的多级树。每个 Mix Node 聚合子节点结果并向上汇报。
- **扇出扩展**：每层 fan-out 通常为几十到数百，树深度通常 2-3 层即可覆盖数千个 Leaf Node。
- **动态剪枝**：Leaf Node 只扫描与查询谓词相关的列 block（列存储 + 元数据 block 中的 min/max 统计实现块级剪枝）。
- **树内聚合**：聚合操作在 Mix Node 层逐步合并，减少向 Root 传输的数据量。

```
  查询执行流程：
  SQL → Parser → Optimizer (CBO) → Plan → 分配 Slots → 构建 Dremel Tree
       → 下发到 Leaf Nodes → 并行扫描 Capacitor columns → Mix 层聚合
       → Root 收集结果 → 返回客户端
```

### 3.2 Capacitor 列式存储格式
Capacitor 是 Google 内部的列式存储格式，针对分析型工作负载深度优化：

| 特性 | 说明 |
|------|------|
| **列式布局** | 每列独立存储，只读取查询需要的列，减少 I/O |
| **记录级编码** | RLE（重复值）、Delta Encoding（单调递增序列）、Bit Packing（小整数）、Dictionary Encoding（低基数） |
| **嵌套数据** | 原生支持 Protocol Buffers 的嵌套/重复结构，无需 flat 化 |
| **Block 元数据** | 每个 block 存储 min/max/count/null_count，支持快速块级剪枝 |
| **压缩** | 列级别自动选择最优压缩算法（Snappy / LZ4 / Zstandard） |

### 3.3 存算分离（Separation of Compute and Storage）
BigQuery 是业界最早实现存算分离的云数仓之一：

- **存储层（Colossus + Capacitor）**：独立扩展，自动管理数据分布和冗余。
- **计算层（Dremel Slots）**：按需分配，查询结束后释放。
- **优势**：
  - 存储和计算可独立弹性伸缩。
  - 多租户共享存储，避免数据孤岛。
  - 按需付费模型：存储按量计费，查询按扫描字节/Slot 计费。

### 3.4 Jupiter 网络 Fabric
Jupiter 是 Google 数据中心的 Clos 网络架构：

- **带宽**：单集群 Tbps 级别吞吐，支持大规模 Shuffle。
- **多路径路由**：ECMP（等价多路径）确保网络负载均衡。
- **对 BigQuery 的意义**：如果没有 Jupiter 级别的网络，Dremel 的跨节点 shuffle 和大规模并行扫描将受网络瓶颈制约。Jupiter 使得「把数据打散到数千节点上并行处理」在工程上可行。

### 3.5 Slot-Based 资源管理
BigQuery 使用 **Slot（槽位）** 作为基本计算资源单位：

```
  Slot 层级：
  Organization
    └── Reservation（预留 Slot 池）
          └── Assignment（绑定到 Project/Dataset）
                └── Query（动态消耗 Slot）

  定价模式：
  - On-Demand：$6.25/TB 扫描数据（自动分配 Slot）
  - Flat-Rate：固定月费获取专属 Slot 池（如 500/2000/10000+ Slots）
```

- **动态分配**：每个查询根据复杂度和数据量自动申请适量 Slot。
- **弹性共享**：On-Demand 模式下 Slot 在多租户间动态复用。
- **Reservation**：企业可购买专属 Slot 池，实现资源隔离和成本预测。
- **自动缩放**：Reservation 支持 autoscaling，在预留基础上按需借用。

### 3.6 BI Engine
BI Engine 是 BigQuery 的内存加速层，专为交互式 BI 场景设计：

- **内存列缓存**：热点数据缓存在内存中，亚秒级响应。
- **自动管理**：根据查询模式自动选择缓存内容，无需手动配置。
- **与 Tableau/Looker/Data Studio 集成**：拖拽式仪表板实现即时反馈。
- **定价**：按预留内存容量计费（GB/月）。

### 3.7 一致性模型
BigQuery 提供**强一致性（Strong Consistency）**：

- 数据写入（Streaming Inserts / Load Jobs）完成后，后续查询立即可见。
- Streaming buffer 中的数据有短暂的可见性延迟（通常几秒），但一旦提交即强一致。
- 表级别的一致性：对表的 DDL 操作（ALTER TABLE 等）在完成后对所有后续查询一致可见。
- 不支持多表事务或跨数据集的 ACID 操作——这是数仓而非 OLTP 系统的权衡。

### 3.8 容错机制
- **Colossus 副本**：数据多副本存储，单点故障不影响可用性。
- **查询重试**：Slot 级别的故障自动重试，对用户透明。
- **无状态执行**：Dremel 执行节点无状态，故障后快速替换。
- **Streaming Buffer 容错**：流式写入有 exactly-once 语义（基于 insertId 去重）。

---

## 4. Trade-offs（CAP / PACELC）

### CAP 定位：CP（Consistency + Partition Tolerance）

| 维度 | 选择 | 说明 |
|------|------|------|
| **Consistency** | ✅ 强一致 | 写入后立即可查询，Streaming Buffer 延迟极短 |
| **Availability** | ⚠️ 分区时可能降级 | 极端网络分区下，部分区域可能不可写，但已写入数据可查 |
| **Partition Tolerance** | ✅ 容忍 | 跨地域多副本，网络分区后自动恢复 |

BigQuery 作为分析型系统，**一致性优先于可用性**——用户不能接受查询返回过期或丢失的数据。但在实际运营中，Google 的基础设施（Jupiter + Colossus）使分区极为罕见，因此可用性在实际体验中也很高。

### PACELC 定位

| 场景 | 选择 | 说明 |
|------|------|------|
| **分区时（P）** | **C**（一致性） | 宁愿延迟也不返回不一致结果 |
| **正常时（E/L）** | **L**（低延迟） | 正常运营下优化查询延迟，通过 Dremel 树、列存、缓存实现亚秒-秒级响应 |

**简而言之：PA/C + EL → C/L**

### 复杂度 vs 运维简单性
- **用户侧零运维**：无需管理节点、无需 VACUUM、无需索引调优——真正的 Serverless。
- **Google 侧极度复杂**：内部需要管理 Dremel 树调度、Slot 分配、Colossus 复制、Capacitor 编码优化、Jupiter 网络拓扑——这些复杂度全部被平台吸收。

### 成本 vs 性能

| 维度 | 权衡 |
|------|------|
| On-Demand 定价 | 按扫描字节计费 → 鼓励用户使用分区/聚簇减少扫描量 |
| Flat-Rate 定价 | 固定 Slot 池 → 适合稳定负载，但闲置 Slot 浪费成本 |
| 长查询成本 | 复杂 JOIN 可能扫描大量数据 → BI Engine 可加速热点查询 |
| 存储成本 | 主动存储 vs 长期存储（90 天未访问自动降价 50%） |

**核心权衡**：BigQuery 的按需定价模型意味着**查询模式直接决定成本**。不优化的查询（全表扫描、无分区过滤）可能非常昂贵——这既是约束也是激励。

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统
BigQuery 作为 Serverless Analytics 的开创者，深刻影响了后续云数仓的设计：

| 后续系统 | 受影响的设计 |
|----------|-------------|
| **Snowflake (2015)** | 存算分离架构、虚拟仓库（类似 Slot 概念）、按需付费 |
| **Amazon Redshift Serverless (2022)** | 从预置实例走向 Serverless、按 RPU 计费 |
| **Azure Synapse Serverless** | 按需 SQL 查询、分离存储与计算 |
| **Databricks SQL** | Photon 引擎的向量化执行、Serverless 计算 |
| **阿里 MaxCompute / 华为 DWS** | 国内云数仓的存算分离演进 |

### 成为行业标准的设计模式
1. **Serverless Analytics**：无需管理基础设施、按需付费、自动扩展——现已成为云数仓的标准范式。
2. **存算分离**：存储和计算独立扩展，避免资源浪费。
3. **列式存储 + 自动编码**：Capacitor 的设计思路被 Parquet、ORC 等开放格式广泛采纳。
4. **Slot/Capacity-Based 资源管理**：Snowflake 的 Warehouse、Redshift 的 RPU 都采用了类似概念。
5. **向量化查询执行**：Dremel 的列式扫描 + 批处理执行是现代 OLAP 引擎的标准。

### 技术谱系（Technology Genealogy）

```
  Google 论文/系统谱系：

  MapReduce (2004)
      │
      ▼
  Bigtable (2006) ──→ Protocol Buffers ──→ 嵌套数据模型
      │
      ▼
  GFS (2003) ──→ Colossus ──→ BigQuery 存储层
      │
      ▼
  Dremel (2006 内部 / 2010 论文)
      │
      ▼
  BigQuery (2010 GA) ───┬──→ Snowflake (2015)
                        ├──→ Redshift Serverless (2022)
                        ├──→ Azure Synapse Serverless
                        └──→ Databricks SQL / Photon

  Capacitor ──→ 列式存储思想 ──→ Parquet / ORC / Arrow
  Jupiter ──→ 数据中心网络 ──→ 现代 Clos 架构（AWS Nitro、阿里神龙）
```

### 当前相关性和使用场景
- **企业数据仓库**：PB 级数据的历史分析和报表。
- **数据湖分析**：通过外部表查询 GCS 中的 Parquet/CSV/JSON 文件。
- **机器学习**：BigQuery ML 支持在数仓内直接训练和预测模型。
- **实时分析**：Streaming Inserts + 物化视图支持近实时场景。
- **多云/混合云**：通过 BigQuery Omni 支持在 AWS/Azure 上执行查询（数据不搬迁）。

### 总结：BigQuery 的历史地位

BigQuery 是 **Serverless 分析型数据库的开创者**。它在 2010 年提出了三个在当时极具前瞻性的设计决策：

1. **用户无需管理任何基础设施**——比 AWS Lambda（2014）还早 4 年。
2. **存储与计算完全分离**——比 Snowflake 的类似架构还早 5 年。
3. **按查询付费而非按实例付费**——重新定义了数据分析的成本模型。

这三个决策共同定义了现代云数仓的标准形态。今天，「Serverless Data Warehouse」已成为行业共识，而 BigQuery 依然是这一领域的标杆系统之一。

---

## 参考资料
1. Melnik, S. et al. "Dremel: Interactive Analysis of Web-Scale Datasets." *VLDB*, 2010.
2. Google Cloud. "BigQuery Architecture Overview." https://cloud.google.com/bigquery/docs
3. Singh, A. et al. "Jupiter Evolving: Transforming Google's Datacenter Network Fabric." *SIGCOMM*, 2015.
4. Chang, F. et al. "Bigtable: A Distributed Storage System for Structured Data." *OSDI*, 2006.
5. Dean, J. and Ghemawat, S. "MapReduce: Simplified Data Processing on Large Clusters." *OSDI*, 2004.
6. Google Cloud. "BI Engine Documentation." https://cloud.google.com/bi-engine
7. Google Cloud. "BigQuery Slot Management." https://cloud.google.com/bigquery/docs/slots-introduction
