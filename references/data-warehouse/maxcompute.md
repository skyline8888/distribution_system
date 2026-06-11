# Alibaba MaxCompute (ODPS) — 深度技术分析

## 基本信息
- **全称:** Alibaba MaxCompute（原名 ODPS，Open Data Processing Service）
- **开发方:** 阿里巴巴集团
- **类型:** 云原生 Serverless 数据仓库（专有云/公有云）
- **首版:** ~2012 年（内部使用），2016 年正式对外商业化
- **规模:** Exabyte（EB）级数据处理能力
- **定位:** 阿里云数据与智能板块的核心离线计算平台
- **核心论文/文档:** 阿里云官方技术白皮书、Apsara 大会技术分享、阿里巴巴技术博客

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

2010 年前后，阿里巴巴集团面临前所未有的数据爆炸：

- **电商交易日志:** 淘宝/天猫每天产生 TB 级点击流、交易日志
- **多业务线数据孤岛:** 天猫、聚划算、菜鸟、支付宝等各自有独立的 Hadoop 集群，数据无法统一分析
- **Hadoop 生态的运维瓶颈:** 数十个 Hadoop 集群需要独立运维，资源利用率极低（峰值 vs 谷值差异达 10 倍）
- **Hive 性能不足:** 对复杂 ETL 和超大规模 JOIN 的性能表现不满足业务 SLA

### 前代系统局限性

| 前代方案 | 局限性 |
|----------|--------|
| 自建 Hadoop/Hive 集群 | 运维成本高，资源隔离差，无法跨业务共享 |
| 商业 MPP (Teradata) | 扩展成本指数增长，无法支撑 EB 级 |
| 早期阿里云 Hadoop 服务 | 需要用户管理基础设施，非 Serverless |

### 硬件/网络时代背景

- 2010–2012 年：单节点 HDD 容量达到 3–4TB，但 IOPS 仍受限
- 10GbE 网络逐渐普及，但跨机柜带宽仍是瓶颈
- 摩尔定律放缓 → 转向水平扩展（scale-out）成为唯一路径
- 飞天（Apsara）操作系统成熟，为 MaxCompute 提供了底层分布式基础设施（盘古存储 + 伏羲调度）

---

## 2. Architecture（架构设计）

### 系统拓扑

MaxCompute 构建在阿里巴巴飞天（Apsara）分布式操作系统之上，采用 **Master-Slave + 多租户** 混合架构：

```
┌─────────────────────────────────────────────────────────────────┐
│                    Client Layer (接入层)                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────┐  │
│  │   SQL    │  │ MapReduce│  │   Graph  │  │   ML/Xflow     │  │
│  │ Console  │  │   SDK    │  │   SDK    │  │   SDK          │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └───────┬────────┘  │
│       │              │             │                │            │
│       └──────────────┴──────┬──────┴────────────────┘            │
│                             ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Gateway / API Proxy 层                       │   │
│  │   认证鉴权 · 请求路由 · 限流 · 负载均衡                   │   │
│  └────────────────────────┬─────────────────────────────────┘   │
│                           │                                     │
├───────────────────────────┼─────────────────────────────────────┤
│              MaxCompute Control Plane (控制平面)                  │
│                                                                   │
│  ┌────────────────────┐  ┌──────────────────────────────────┐    │
│  │  Access Controller  │  │   Metadata Service (元数据服务)  │    │
│  │  · ACL/RBAC        │  │   · Project/Table/Partition      │    │
│  │  · Data Masking    │  │   · Column Schema/Stats          │    │
│  │  · Label Security   │  │   · Job DAG/State                │    │
│  └────────────────────┘  └──────────────┬───────────────────┘    │
│                                         │                         │
│  ┌──────────────────────────────────────┴───────────────────┐    │
│  │              Neptune SQL Engine (SQL 引擎)                │    │
│  │  ┌────────────┐ ┌───────────┐ ┌────────────────────┐     │    │
│  │  │  Parser &  │ │ Optimizer │ │  Code Generator     │     │    │
│  │  │  Validator │ │ (CBO/RBO) │ │  (→ Fuxi Tasks)    │     │    │
│  │  └────────────┘ └───────────┘ └────────────────────┘     │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │         Fuxi Resource Manager (伏羲资源管理器)             │    │
│  │  · Task Scheduling (DAG 调度)                             │    │
│  │  · Resource Allocation (CPU/Memory/IO 配额)              │    │
│  │  · Fault Tolerance (Task 重试/重调度)                     │    │
│  │  · Multi-tenant Fair Sharing                              │    │
│  └──────────────────────────┬───────────────────────────────┘    │
│                             │                                     │
├─────────────────────────────┼─────────────────────────────────────┤
│              MaxCompute Data Plane (数据平面)                      │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │         Compute Workers (计算节点集群)                      │    │
│  │                                                           │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐    │    │
│  │  │ Worker 1 │ │ Worker 2 │ │ Worker 3 │ │ Worker N │    │    │
│  │  │ · SQL RT │ │ · SQL RT │ │ · MR RT  │ │ · ML RT  │    │    │
│  │  │ · Shuffle│ │ · Shuffle│ │ · Shuffle│ │ · Shuffle│    │    │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘    │    │
│  │       │             │            │              │          │    │
│  └───────┼─────────────┼────────────┼──────────────┼──────────┘    │
│          │             │            │              │                │
│          └─────────────┴──────┬─────┴──────────────┘                │
│                               ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │         Pangu Distributed File System (盘古存储)           │    │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐            │    │
│  │  │  Pangu CS  │ │  Pangu CS  │ │  Pangu CS  │  ···       │    │
│  │  │  Chunk Srv │ │  Chunk Srv │ │  Chunk Srv │            │    │
│  │  └────────────┘ └────────────┘ └────────────┘            │    │
│  │                                                           │    │
│  │  列式存储: Cangjie (仓颉) 格式                              │    │
│  │  · Column-oriented encoding                               │    │
│  │  · Dictionary / RLE / Bit-packing                         │    │
│  │  · Predicate pushdown                                     │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 数据模型与命名空间

- **Project:** 最高级别的隔离单元，类比数据库的 Schema/Namespace
- **Table:** 分区表（Partitioned）或非分区表，支持外部表（OSS 映射）
- **Partition:** 按时间/业务维度划分，提升查询裁剪效率
- **Resource:** 上传的 JAR/Python/数据文件，供 UDF/MapReduce 使用
- **Instance:** 一次计算任务的运行实例，可查询状态与日志

### 元数据架构归类

**模式: 集中式分区（分区化集中式元数据服务）**

MaxCompute 的元数据服务是集中式的 Metadata Server 集群，负责管理所有 Project 的元数据（Table/Partition/Schema/Job 状态）。元数据按 Project 进行分区，不同 Project 的元数据路由到不同的元数据分片，实现水平扩展。

- 元数据持久化在底层高可用存储上（类似 Paxos 复制）
- 跨 Project 的 JOIN 需要元数据服务协调
- Job DAG 和中间状态也由元数据服务管理

### 数据架构分析

| 维度 | 设计 |
|------|------|
| **分块方式** | 表按 Partition 划分 → Partition 内部按固定大小 Block 分块（类 GFS Chunk），存储于 Pangu |
| **分布策略** | 基于 Hash + 范围混合：数据按分区键范围分布，分区内按 Hash 分配到不同 Pangu ChunkServer |
| **冗余方式** | Pangu 层 3 副本复制（默认），关键数据可配置更多副本；冷热数据分层后可能使用纠删码 |
| **I/O 路径** | 计算节点通过 Pangu Client 直接读取 Chunk（用户态），支持列裁剪和谓词下推 |
| **生命周期** | 自动生命周期管理：TTL 过期 → 回收站 → 物理删除；冷热数据自动分层存储（热: SSD, 温: HDD, 冷: 归档） |

### 存储格式: Cangjie（仓颉）列式格式

阿里巴巴自研的列式存储格式，核心设计：

- **列式编码:** 按列存储，支持高效的列裁剪（只读取需要的列）
- **压缩策略:** Dictionary Encoding、Run-Length Encoding (RLE)、Bit-packing、Delta Encoding
- **索引结构:** 每个 Block 包含 Min/Max 统计信息，支持 Predicate Pushdown
- **向量化读取:** 配合 Neptune SQL Engine 的向量化执行，减少 CPU 指令开销

### 计算引擎: Neptune + Fuxi

| 组件 | 职责 |
|------|------|
| **Neptune SQL Engine** | SQL 解析 → 优化（CBO + RBO）→ 代码生成 → 物理执行计划 |
| **Fuxi（伏羲）资源管理器** | 将物理计划拆分为 Task DAG，负责调度、容错、资源分配、多租户隔离 |

Neptune 的关键优化技术：
- **CBO (Cost-Based Optimizer):** 基于表统计信息的代价模型
- **向量化执行 (Vectorized Execution):** SIMD 优化，批量处理数据
- **运行时动态优化 (Runtime Re-optimization):** 根据实际运行时统计信息调整执行计划
- **自适应 JOIN 策略:** 根据数据量在 Broadcast/Merge/Hash JOIN 间切换

---

## 3. Core Technical Innovations（核心技术创新）

### 一致性模型

**强一致性（Strong Consistency）**

- 元数据：所有元数据操作（创建表、修改 Schema、分区操作）通过 Paxos 协议保证强一致性
- 数据：写入完成后立即可读（Read-After-Write 强一致）
- 事务：支持 ACID 语义的事务性写入（Transactional Write），多表操作保证原子性

### 共识协议

- **Paxos:** 元数据服务使用 Paxos 协议实现多副本一致性
- **Pangu 层:** Pangu 分布式文件系统使用基于 Paxos 的副本同步协议，保证数据副本间的一致性

### 容错机制

| 层级 | 容错策略 |
|------|----------|
| **Task 级** | Task 失败自动重试（默认 3 次），Fuxi 重新调度到其他可用 Worker |
| **Worker 级** | Worker 心跳超时 → 标记不可用 → 重新调度其上的所有 Task |
| **数据级** | Pangu 3 副本 → 单节点/机架故障不丢失数据；副本自动修复 |
| **Job 级** | DAG 阶段（Stage）级容错：失败 Stage 可重算，已完成的 Stage 结果不重复计算 |

### 性能优化技术

1. **CBO + RBO 混合优化:** 先 RBO 做逻辑等价变换，再 CBO 选择最优物理计划
2. **列裁剪 (Column Pruning):** 只读取需要的列，减少 I/O
3. **谓词下推 (Predicate Pushdown):** 在存储层进行数据过滤，减少网络传输
4. **MapJoin / Broadcast Join:** 小表广播到大表，避免 Shuffle
5. **动态分区裁剪 (Dynamic Partition Pruning):** 运行时根据过滤条件跳过无关分区
6. **数据本地性优化:** 计算任务尽量调度到数据所在节点（Data Locality）
7. **倾斜处理 (Skew Handling):** 自动检测并处理数据倾斜（Key Splitting、双重聚合）
8. **向量化执行:** 利用 SIMD 指令批量处理列数据

### 扩展方式

**纯 Scale-Out 架构:**

- 计算: 无状态 Worker 节点可随时水平扩展，Fuxi 统一调度
- 存储: Pangu 通过增加 ChunkServer 节点实现容量和吞吐的线性扩展
- 多租户隔离: Quota Group + 资源池机制，不同业务线独占或共享资源池

---

## 4. Trade-offs (CAP / PACELC)

### CAP 定位

**MaxCompute 选择 CP（Consistency + Partition Tolerance）**

| CAP 维度 | 选择 | 理由 |
|----------|------|------|
| **Consistency (C)** | ✅ 强一致性 | 作为数据仓库，元数据和数据读写必须保证一致性；分析结果可复现是基本要求 |
| **Availability (A)** | ⚠️ 分区时牺牲可用性 | 在网络分区时，优先保证数据一致性而非可用性（写入可能被拒绝） |
| **Partition Tolerance (P)** | ✅ 容错 | 分布式系统必须处理网络分区，P 不可妥协 |

> **为什么选 CP 而不是 AP？** 数据仓库的核心价值是 "可信的分析结果"。如果写入后读到不一致的数据，分析结果就不可信。对于离线数仓场景，可用性可以适当牺牲（任务可以重试/延迟），但一致性不能妥协。

### PACELC 定位

| 场景 | 选择 | 解释 |
|------|------|------|
| **分区时 (P):** 选 **C** 而非 A | 网络分区时保证一致性 | |
| **正常时 (EL):** 选 **E** (Efficiency) 而非 L (Latency) | 作为离线数仓，吞吐优先于延迟 | |

> **离线数仓的典型取舍:** 一个运行 30 分钟的 ETL 任务，延迟 5 秒 vs 10 秒没有本质区别，但吞吐量（每小时处理多少 TB）直接决定业务价值。

### 复杂度 vs 运维

| 维度 | 复杂度来源 | 用户感知 |
|------|-----------|----------|
| **底层架构复杂度** | 极高（自研引擎 + 自研存储 + 自研调度） | ✅ 对用户透明 |
| **用户运维复杂度** | 极低（Serverless） | ✅ 用户无需关心基础设施 |

**Serverless 设计的核心价值:** 所有复杂性下沉到平台层，用户只需提交 SQL，无需管理任何集群、存储或资源。这是 MaxCompute 与传统 Hadoop 生态最大的差异化优势。

### 成本 vs 性能

| 维度 | 设计 | 效果 |
|------|------|------|
| **存储成本** | 冷热分层 + 列式压缩 | 压缩比通常 3–10 倍，冷数据自动迁移到低成本介质 |
| **计算成本** | Serverless 按需计费 | 按数据处理量（SQL 扫描量）和 CU 消耗计费，无闲置成本 |
| **数据倾斜优化** | 自动检测 + 处理 | 避免因个别 Key 导致全 Job 性能下降（减少资源浪费） |
| **共享计算资源** | 多租户 + 资源池 | 提高集群整体利用率，降低单位计算成本 |

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统 / 生态集成

| 系统/产品 | 关系 |
|-----------|------|
| **Hologres** | 同属阿里云，实时交互式分析，与 MaxCompute 形成离线+实时互补 |
| **AnalyticDB** | 同属阿里云，实时 OLAP，不同场景定位 |
| **DataWorks** | MaxCompute 的上层数据开发治理平台 |
| **DLF + MaxCompute** | 湖仓一体方案：DLF 数据湖 + MaxCompute 计算引擎 |
| **Flink + MaxCompute** | 流批一体：Flink 实时写入 MaxCompute 离线存储 |
| **开源生态兼容** | 支持 Hive SQL 语法子集、兼容 Hadoop 生态工具链 |

### 成为标准的设计模式

1. **Serverless 数据仓库:** MaxCompute 是国内最早实现完全 Serverless 化的大规模数据仓库之一，用户零运维的设计理念影响了后续多家云厂商
2. **计算存储分离架构:** 在盘古存储之上构建独立计算层，这一模式被后续众多云原生数据仓库采纳
3. **多租户强隔离:** Quota Group + 资源池 + ACL 的多层隔离机制，成为企业级数仓的标准配置
4. **列式存储 + 向量化执行:** 这一组合已成为现代 OLAP 引擎的事实标准

### 技术谱系位置

```
Google →  GFS          → Pangu (盘古存储)        → MaxCompute 底层存储
         MapReduce     → Fuxi (伏羲调度)          → MaxCompute 计算调度
         Bigtable      → Table Store              → 关联存储系统
         Dremel        → Neptune SQL Engine       → SQL 查询引擎

开源 →  Hadoop/Hive   ──────┐
                            ├──→ MaxCompute 设计理念对标
商业 → Teradata/Greenplum ───┘     但采用完全不同的 Serverless 架构

MaxCompute ──→ Hologres (实时分析)
            ──→ DataWorks (数据治理)
            ──→ DLF + MaxCompute (湖仓一体)
            ──→ Flink + MaxCompute (流批一体)
```

### 当前相关性

- **阿里巴巴内部:** MaxCompute 是阿里巴巴集团**所有离线数据分析的唯一平台**，每天处理 EB 级数据，运行数百万计算任务
- **阿里云公有云:** 是阿里云数据中台方案的核心组件，服务大量外部企业客户
- **行业地位:** 在中国云数据仓库市场占据主导地位，与 Snowflake、BigQuery 在国际市场对标
- **演进方向:**
  - 湖仓一体（Lakehouse）: 与 DLF 深度整合
  - AI/ML 集成: 内置机器学习能力（PAI 平台对接）
  - 实时化: 与 Hologres 形成 Lambda/Kappa 架构
  - Serverless 2.0: 更细粒度的资源计量和弹性扩缩容

### 关键对比: MaxCompute vs 国际同类产品

| 维度 | MaxCompute | Snowflake | BigQuery |
|------|-----------|-----------|----------|
| 架构 | Serverless | Serverless | Serverless |
| 底层存储 | 自研 Pangu + Cangjie | 云对象存储 + 微分区 | Borg + Colossus + Capacitor |
| 调度 | 自研 Fuxi | 自研 | 自研 |
| SQL 引擎 | Neptune | 自研 | Dremel 演进 |
| 多租户 | Quota Group + 资源池 | 虚拟仓库隔离 | Slot 资源管理 |
| 生态 | 阿里云全家桶 | Snowflake Marketplace | GCP 生态 |
| 主要市场 | 中国/亚太 | 全球（欧美主导） | 全球（GCP 用户） |
| 开源兼容 | Hive SQL 子集 | ANSI SQL + 扩展 | Standard SQL + Legacy SQL |

---

## 附录: 关键术语表

| 术语 | 说明 |
|------|------|
| **ODPS** | MaxCompute 原名，Open Data Processing Service |
| **Apsara (飞天)** | 阿里巴巴自研分布式操作系统，MaxCompute 的底层平台 |
| **Pangu (盘古)** | 飞天分布式文件系统，MaxCompute 的存储底层 |
| **Fuxi (伏羲)** | 飞天资源调度系统，MaxCompute 的任务调度器 |
| **Neptune** | MaxCompute 的 SQL 计算引擎 |
| **Cangjie (仓颉)** | MaxCompute 的列式存储编码格式 |
| **Project** | MaxCompute 中的命名空间/隔离单元 |
| **Instance** | 一次计算任务的运行实例 |
| **CU** | Compute Unit，MaxCompute 的计算资源计量单位 |
| **Tunnel** | MaxCompute 的数据上传下载通道服务 |

---

## 参考资料

1. 阿里云 MaxCompute 官方文档: https://help.aliyun.com/product/27797.html
2. 阿里云 MaxCompute 技术白皮书
3. Apsara Conference (云栖大会) MaxCompute 技术分享
4. Alibaba Group Technology Blog
5. Google Dremel / BigQuery 论文 (架构参考对比)
6. Snowflake Architecture 论文 (产品对比参考)
