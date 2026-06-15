# 中国分布式系统技术生态总览

> 系统性梳理中国核心技术厂商在分布式存储、数据库、中间件等领域的技术布局、开源贡献与市场格局。

---

## Table of Contents

1. [Ecosystem Map](#ecosystem-map)
2. [Major Players](#major-players)
3. [Cloud Providers](#cloud-providers)
4. [Database Ecosystem](#database-ecosystem)
5. [Storage Ecosystem](#storage-ecosystem)
6. [Message Queue & Middleware](#message-queue--middleware)
7. [Open-Source Contributions](#open-source-contributions)
8. [Microservices & RPC Frameworks](#microservices--rpc-frameworks)
9. [Government & Enterprise Adoption](#government--enterprise-adoption)
10. [Unique Characteristics](#unique-characteristics)
11. [Timeline of Key Milestones](#timeline-of-key-milestones)

---

## Ecosystem Map

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    CHINA DISTRIBUTED SYSTEMS ECOSYSTEM MAP                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                 │
│  │   Alibaba/Ant   │  │    Tencent      │  │    ByteDance    │                 │
│  │                 │  │                 │  │                 │                 │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │                 │
│  │  │  Aliyun   │  │  │  │Tencent Cl.│  │  │ Volcengine  │  │                 │
│  │  └─────┬─────┘  │  │  └─────┬─────┘  │  │  └─────┬─────┘  │                 │
│  │        │        │  │        │        │  │        │        │                 │
│  │  ┌─────┴─────┐  │  │  ┌─────┴─────┐  │  │  ┌─────┴─────┐  │                 │
│  │  │ PolarDB   │  │  │   TDSQL     │  │  │    N/A      │  │                 │
│  │  │ OceanBase │  │  │   TBase     │  │  │  ByteHouse  │  │                 │
│  │  │ Lindorm   │  │  │   CynosDB   │  │  │  HDFS-based │  │                 │
│  │  │ AnalyticDB│  │  │   TDSQL-C   │  │  │  Storage    │  │                 │
│  │  └───────────┘  │  │  └───────────┘  │  └───────────┘  │                 │
│  │                 │  │                 │  │                 │                 │
│  │  RocketMQ       │  │  CMQ/CKafka     │  │  Kafka-based    │                 │
│  │  Dubbo/HSF      │  │  TARS/TSeer     │  │  RPC internal   │                 │
│  │  Sentinel       │  │  PolarDB        │  │  Flink (core)   │                 │
│  │  Seata          │  │  OpenClaw       │  │                 │                 │
│  │  SOFAStack      │  │                 │  │                 │                 │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘                 │
│           │                    │                    │                          │
│  ┌────────┴────────────────────┴────────────────────┴────────┐                 │
│  │                  OPEN-SOURCE FOUNDATIONS                   │                 │
│  │                                                             │                 │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │                 │
│  │  │  TiDB    │ │RocketMQ  │ │  Dubbo   │ │Sharding  │      │                 │
│  │  │  TiKV    │ │(Apache)  │ │(Apache)  │ │Sphere    │      │                 │
│  │  │(PingCAP) │ │          │ │          │ │(Apache)  │      │                 │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘      │                 │
│  │       │            │            │            │              │                 │
│  └───────┼────────────┼────────────┼────────────┼──────────────┘                 │
│          │            │            │            │                                │
│  ┌───────┴────────────┴────────────┴────────────┴──────────────┐                 │
│  │               COMPUTE & STORAGE INFRASTRUCTURE               │                 │
│  │                                                               │                 │
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐    │                 │
│  │  │  ESSD     │ │    CBS    │ │    OBS    │ │   SFS     │    │                 │
│  │  │ (Aliyun)  │ │(Tencent)  │ │ (Huawei)  │ │  (Huawei) │    │                 │
│  │  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘    │                 │
│  │        │              │              │              │         │                 │
│  │  ┌─────┴─────┐ ┌──────┴──────┐ ┌────┴─────┐ ┌─────┴────┐    │                 │
│  │  │ PolarFS   │ │   CFS       │ │  EVS     │ │ NAS      │    │                 │
│  │  │ (Alibaba) │ │ (Tencent)   │ │(Huawei)  │ │ (Common) │    │                 │
│  │  └───────────┘ └─────────────┘ └──────────┘ └──────────┘    │                 │
│  │                                                               │                 │
│  └───────────────────────────┬───────────────────────────────────┘                 │
│                              │                                                    │
│  ┌───────────────────────────┴───────────────────────────────────┐                 │
│  │                   HUAWEI CLOUD ECOSYSTEM                      │                 │
│  │                                                               │                 │
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐    │                 │
│  │  │ GaussDB   │ │   DWS     │ │  DRS      │ │  DDS      │    │                 │
│  │  │(openGauss)│ │(MPP/OLAP) │ │           │ │(Redis)    │    │                 │
│  │  └───────────┘ └───────────┘ └───────────┘ └───────────┘    │                 │
│  │                                                               │                 │
│  │  ROMA Connect │ ServiceStage │ CSE │ CloudPond                │                 │
│  │  DCS (Cache)  │ GaussDB(for) │ OBS │ CBR                       │                 │
│  └───────────────────────────┬───────────────────────────────────┘                 │
│                              │                                                    │
│  ┌───────────────────────────┴───────────────────────────────────┐                 │
│  │                   BAIDU & JD CLOUD                            │                 │
│  │                                                               │                 │
│  │  Baidu: Palo/Doris → Apache Doris (OLAP)                     │                 │
│  │         Baidu Cloud: BCR, GaiaDB, CDS                         │                 │
│  │         PaddlePaddle → distributed training infra              │                 │
│  │                                                               │                 │
│  │  JD:   Jimu (分布式数据库), JD Cloud storage/compute           │                 │
│  │        ShardingSphere roots from JD middleware                  │                 │
│  └───────────────────────────────────────────────────────────────┘                 │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                  GOVERNMENT & ENTERPRISE LAYER                   │            │
│  │                                                                   │            │
│  │  Xinchuang (信创) │  State Cloud │  City Brain │  Smart Gov     │            │
│  │  国产化替代        │ 政务云       │ 城市大脑     │  数字政府      │            │
│  │                                                                   │            │
│  │  Key drivers: Data Security Law, PIPL, Cybersecurity Law         │            │
│  │  Push for domestic substitutes across all layers                  │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Major Players

### Alibaba Group / Ant Group

| Dimension | Details |
|-----------|---------|
| **Scale** | Operates one of the world's largest distributed systems, handling 580K+ TPS during Double 11 peak |
| **Flagship** | Aliyun (Alibaba Cloud) — #1 cloud provider in China and APAC |
| **Database** | PolarDB, OceanBase (spun to Ant), Lindorm, AnalyticDB |
| **Middleware** | RocketMQ (Apache), Dubbo (Apache), Sentinel, Seata, SOFAStack |
| **Storage** | ESSD (block), NAS, OSS (object), PolarFS (distributed file system) |
| **Compute** | Apsara (飞天) distributed OS,神龙 (X-Dragon) bare metal, ACK (K8s) |
| **Key Innovation** | Self-developed Apsara distributed OS underpins the entire stack; born from handling extreme e-commerce traffic spikes |

### Tencent

| Dimension | Details |
|-----------|---------|
| **Scale** | Supports WeChat's 1.3B+ users, gaming infrastructure, Tencent Video |
| **Flagship** | Tencent Cloud — #2 cloud provider in China |
| **Database** | TDSQL (distributed RDBMS), TBase (HTAP), CynosDB (cloud-native), TDSQL-C |
| **Middleware** | CMQ, CKafka (managed Kafka), TSeer (service mesh) |
| **Storage** | CBS (block), CFS (file), COS (object) |
| **Compute** | TKE (K8s), TCS (container service), StarDB (internal) |
| **Key Innovation** | Strong integration with social/gaming ecosystem; TDSQL used in financial sector widely |

### ByteDance

| Dimension | Details |
|-----------|---------|
| **Scale** | TikTok/Douyin handles massive video upload, recommendation, and real-time processing |
| **Flagship** | Volcano Engine (火山引擎) — cloud arm |
| **Database** | ByteHouse (OLAP, ClickHouse-based), internal distributed storage |
| **Middleware** | Heavily Kafka-based, internal RPC framework |
| **Compute** | K8s-based container platform, self-developed orchestrators |
| **Key Innovation** | World-leading Flink contributor (acquired Ververica team's expertise); extreme scale real-time recommendation pipelines |

### Huawei

| Dimension | Details |
|-----------|---------|
| **Scale** | Full-stack from silicon (Kunpeng, Ascend) to cloud to apps |
| **Flagship** | Huawei Cloud — #3 cloud provider, strongest in government/enterprise |
| **Database** | GaussDB (openGauss-based), GaussDB(DWS), DDS, DRS |
| **Middleware** | ROMA Connect (integration), DCS (Redis-compatible) |
| **Storage** | OBS (object), EVS (block), SFS (file), CBR (backup) |
| **Compute** | CCE (K8s), CCI (container instance), FunctionGraph (serverless) |
| **Key Innovation** | Most vertically integrated: chips → OS (openEuler) → DB (openGauss) → cloud → applications. Strong sovereign/government positioning |

### Baidu

| Dimension | Details |
|-----------|---------|
| **Scale** | Search, AI/cloud workloads, autonomous driving |
| **Flagship** | Baidu Cloud — #5 cloud provider |
| **Database** | GaiaDB, BCR, CDS; contributed Apache Doris (OLAP) |
| **Key Innovation** | PaddlePaddle distributed training; strong AI/ML infrastructure; Apache Doris is widely adopted OLAP |

### JD.com

| Dimension | Details |
|-----------|---------|
| **Scale** | E-commerce logistics, supply chain, fintech |
| **Flagship** | JD Cloud |
| **Database** | Jimu (distributed DB), ShardingSphere roots from JD's database middleware |
| **Key Innovation** | ShardingSphere originated here; strong logistics/supply-chain distributed systems |

---

## Cloud Providers

### Market Share (approximate, 2024-2025)

```
┌──────────────────────────────────────────────────────────────┐
│           China Cloud Infrastructure Market                    │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Aliyun      ████████████████████████████████░░░░░░  ~35%    │
│  Tencent     ██████████████████░░░░░░░░░░░░░░░░░░  ~19%    │
│  Huawei      ████████████████░░░░░░░░░░░░░░░░░░░░  ~17%    │
│  China Telecom ████████░░░░░░░░░░░░░░░░░░░░░░░░░░  ~9%     │
│  Baidu       ██████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  ~7%     │
│  Others      ████████████░░░░░░░░░░░░░░░░░░░░░░░░  ~13%    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### Comparative Overview

| Feature | Aliyun | Tencent Cloud | Huawei Cloud |
|---------|--------|---------------|--------------|
| **Strengths** | Broadest product portfolio, #1 market share, mature PaaS | Gaming/social ecosystem, TDSQL finance adoption | Government/enterprise, full-stack sovereignty |
| **Compute** | ECS, ACK, FC, X-Dragon | CVM, TKE, SCF | ECS, CCE, FunctionGraph |
| **Database** | PolarDB, OceanBase, Lindorm, RDS | TDSQL, CynosDB, TBase | GaussDB, DDS, DWS |
| **Storage** | OSS, ESSD, NAS, CPFS | COS, CBS, CFS | OBS, EVS, SFS |
| **Network** | VPC, SLB, Express Connect | VPC, CLB, Direct Connect | VPC, ELB, Direct Connect |
| **AI/ML** | PAI, Tongyi | TI Platform | ModelArts, Pangu |
| **Differentiator** | Apsara distributed OS, global footprint | WeChat/QQ ecosystem integration | Kunpeng + openEuler + openGauss stack |

---

## Database Ecosystem

### Landscape Overview

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        DISTRIBUTED DATABASE LANDSCAPE                       │
├──────────────────────────┬─────────────────────────────────────────────────┤
│  CATEGORY                │  PRODUCTS                                       │
├──────────────────────────┼─────────────────────────────────────────────────┤
│  Cloud-Native (Shared    │  PolarDB (Aliyun)                               │
│  Storage / Compute-      │  CynosDB (Tencent)                              │
│  Storage Decoupled)      │  TDSQL-C (Tencent)                              │
│                          │  GaussDB(for MySQL) (Huawei)                    │
├──────────────────────────┼─────────────────────────────────────────────────┤
│  Distributed (NewSQL /   │  OceanBase (Ant) — HTAP, MySQL/Oracle compat    │
│  Spanner-like)           │  TiDB (PingCAP) — MySQL compat, Raft-based      │
│                          │  TDSQL (Tencent) — MySQL/PG compat, finance     │
│                          │  TBase (Tencent) — HTAP, PG-based               │
│                          │  GaussDB (Huawei) — openGauss-based, PG-derived │
│                          │  GoldenDB (ZTE) — telecom/finance               │
├──────────────────────────┼─────────────────────────────────────────────────┤
│  Distributed KV / Wide   │  Lindorm (Aliyun) — multi-model                 │
│  Column                  │  TiKV (PingCAP) — Raft KV engine                │
│                          │  OBKV (OceanBase)                               │
│                          │  CynosDB (Tencent)                              │
├──────────────────────────┼─────────────────────────────────────────────────┤
│  OLAP / Data Warehouse   │  AnalyticDB (Aliyun)                            │
│                          │  GaussDB(DWS) (Huawei)                          │
│                          │  Apache Doris (Baidu-origin)                    │
│                          │  StarRocks (forked from Doris)                  │
│                          │  ClickHouse variants (ByteHouse, ByteDance)     │
│                          │  SelectDB (commercial Doris)                    │
├──────────────────────────┼─────────────────────────────────────────────────┤
│  Time-Series / IoT       │  Lindorm TSDB (Aliyun)                          │
│                          │  TDengine (TaosData)                            │
│                          │  IoTDB (Apache, Tsinghua)                       │
├──────────────────────────┼─────────────────────────────────────────────────┤
│  Graph                   │  NebulaGraph (vesoft)                          │
│                          │  GalaxyBase (Tencent)                           │
│                          │  HugeGraph (Baidu, Apache)                      │
├──────────────────────────┼─────────────────────────────────────────────────┤
│  Cache                   │  Tair (Aliyun)                                  │
│                          │  DCS (Huawei)                                   │
│                          │  Redis Enterprise variants                      │
└──────────────────────────┴─────────────────────────────────────────────────┘
```

### Deep Dives

#### OceanBase (Ant Group)
- **Architecture**: Shared-nothing distributed RDBMS with Paxos-based replication
- **Consensus**: Multi-Paxos, 3+1 zone deployment for cross-region DR
- **TPC-C**: Held world record (707M tpmC) before being surpassed
- **Compatibility**: MySQL and Oracle modes — critical for enterprise migration
- **Use Cases**: Alipay core banking, government financial systems
- **Licensing**: Mixed — core is proprietary, some components open-sourced

#### PolarDB (Alibaba Cloud)
- **Architecture**: Shared-storage cloud-native (like Aurora), compute-storage decoupled
- **Storage**: PolarFS distributed file system + RDMA networking
- **Features**: Parallel query, global database, serverless variant
- **Engines**: MySQL, PostgreSQL, Oracle-compatible (PolarDB-O)
- **Scale**: Single cluster up to 100TB, 16 read replicas
- **Adoption**: Default recommendation for Aliyun RDS workloads

#### TiDB (PingCAP)
- **Architecture**: Distributed NewSQL, HTAP, Raft-based (TiKV storage layer)
- **Components**: TiDB (SQL layer, stateless), TiKV (Raft KV store), TiFlash (columnar HTAP)
- **Compatibility**: MySQL protocol — drop-in replacement
- **Open Source**: Apache 2.0, one of China's most successful open-source database exports
- **Ecosystem**: TiCDC (change data capture), TiDB Operator (K8s), BR (backup/restore)
- **Adoption**: Used by Tencent, JD, Xiaomi, and many startups; strong international presence

#### GaussDB (Huawei)
- **Architecture**: Derived from PostgreSQL (openGauss open-source core)
- **Variants**: GaussDB(for MySQL), GaussDB(for Redis), GaussDB(DWS) for OLAP
- **Positioning**: Core of Huawei's sovereign cloud stack, government mandate driver
- **openGauss**: Open-source base, growing community, compatible with PostgreSQL
- **Use Cases**: Government, banking, telecom — strong in Xinchuang (domestic substitution) programs

#### TDSQL (Tencent)
- **Architecture**: Distributed RDBMS based on MariaDB/PostgreSQL
- **Variants**: TDSQL (MySQL-based), TDSQL for PostgreSQL, TDSQL-C (cloud-native)
- **Features**: Strong consistency, auto-sharding, cross-AZ HA
- **Adoption**: Widely used in financial sector (banks, securities, insurance)
- **Scale**: Supports Tencent's own financial services and external banking clients

---

## Storage Ecosystem

### Block Storage

| Provider | Product | Features |
|----------|---------|----------|
| Aliyun | ESSD (Enhanced SSD) | Up to 1M IOPS, NVMe-based, PL0-PL5 tiers |
| Tencent | CBS (Cloud Block Storage) | NVMe SSD, auto-scaling, snapshot support |
| Huawei | EVS (Elastic Volume Service) | Ultra-high IOPS, encryption, multi-AZ |

### Object Storage

| Provider | Product | Scale | Features |
|----------|---------|-------|----------|
| Aliyun | OSS (Object Storage Service) | Zettabyte-scale | Lifecycle, versioning, cross-region replication, image processing |
| Tencent | COS (Cloud Object Storage) | Zettabyte-scale | Data lifecycle, CDN integration, inventory |
| Huawei | OBS (Object Storage Service) | Enterprise-scale | WORM, lifecycle, cross-region replication |

### File Storage

| Provider | Product | Protocol | Use Cases |
|----------|---------|----------|-----------|
| Aliyun | NAS, CPFS | NFS/SMB, Lustre-based CPFS | HPC, media rendering, AI training |
| Tencent | CFS | NFS/SMB | Container storage, shared workspace |
| Huawei | SFS, SFS Turbo | NFS/SMB, parallel | HPC, big data, container storage |

### Distributed File Systems (Internal/Underlying)

```
┌──────────────────────────────────────────────────────────┐
│          UNDERLYING DISTRIBUTED FILE SYSTEMS              │
├────────────────────┬─────────────────────────────────────┤
│  System            │  Provider / Status                  │
├────────────────────┼─────────────────────────────────────┤
│  PolarFS           │  Alibaba — RDMA + NVM, for PolarDB  │
│ 盘古 (Pangu)       │  Alibaba — Apsara storage foundation│
│  TFS               │  Taobao — internal file system      │
│  ChubaoFS (CFS)    │  Open-source (JuicedFS)             │
│  JuiceFS           │  Open-source — cloud-native POSIX   │
│  openGauss storage │  Huawei — storage engine for GaussDB│
└────────────────────┴─────────────────────────────────────┘
```

---

## Message Queue & Middleware

| Product | Origin | Status | Key Features |
|---------|--------|--------|--------------|
| **RocketMQ** | Alibaba | Apache TLP | High throughput, transaction messages, delay messages |
| **Apache Pulsar** | Diverse adoption | Apache TLP | Tencent Cloud uses heavily; multi-tenant, tiered storage |
| **CMQ** | Tencent | Internal/Cloud | Queue + topic model, managed service |
| **CKafka** | Tencent | Managed Kafka | Kafka-compatible, auto-scaling |
| **EventBridge** | Aliyun | Cloud | Event-driven architecture, SaaS connectors |

### RocketMQ Deep Dive
- Originally developed by Alibaba for Double 11 traffic
- Donated to Apache, graduated to Top-Level Project
- Architecture: NameServer (lightweight routing), Broker (storage/delivery), Producer/Consumer
- Features: Transactional messages, scheduled/delay messages, ordered messages, message tracing
- Used by: Alibaba, Ant, many Chinese enterprises, also growing internationally

---

## Open-Source Contributions

### Major Projects with Global Impact

```
┌─────────────────────────────────────────────────────────────────────────┐
│              CHINA-ORIGINATED OPEN-SOURCE DISTRIBUTED SYSTEMS            │
├──────────────────┬──────────────┬───────────────┬───────────────────────┤
│  Project         │  Origin      │  Foundation   │  Status/Impact        │
├──────────────────┼──────────────┼───────────────┼───────────────────────┤
│  TiDB / TiKV     │  PingCAP     │  CNCF         │  ★ 38K+, global use   │
│  RocketMQ        │  Alibaba     │  Apache       │  TLP, ★ 21K+          │
│  Dubbo           │  Alibaba     │  Apache       │  TLP, ★ 40K+          │
│  ShardingSphere  │  JD/Apache   │  Apache       │  TLP, ★ 7.6K+         │
│  Sentinel        │  Alibaba     │  CNCF         │  Sandbox, ★ 23K+      │
│  Seata           │  Alibaba     │  CNCF         │  Sandbox, ★ 12K+      │
│  Nacos           │  Alibaba     │  CNCF         │  Sandbox, ★ 30K+      │
│  SOFAJRaft       │  Ant Group   │  —            │  Internal/open-source │
│  Apache Doris    │  Baidu       │  Apache       │  TLP, ★ 13K+          │
│  StarRocks       │  Doris fork  │  —            │  ★ 13K+, commercial   │
│  TDengine        │  TaosData    │  LinuxFdn     │  ★ 26K+               │
│  Apache IoTDB    │  Tsinghua    │  Apache       │  TLP, ★ 4.5K+         │
│  NebulaGraph     │  vesoft      │  LF           │  ★ 7.5K+              │
│  HugeGraph       │  Baidu       │  Apache       │  Sandbox, ★ 2.7K+     │
│  OpenGauss       │  Huawei      │  —            │  ★ 3K+, PG-derived    │
│  OpenEuler       │  Huawei      │  OpenAtom     │  OS, ★ 4K+            │
│  PaddlePaddle    │  Baidu       │  LF AI        │  ★ 22K+, DL framework │
│  Dragonfly       │  Alibaba     │  CNCF         │  Graduated, ★ 5K+     │
│  OpenKruise      │  Alibaba     │  CNCF         │  Sandbox, ★ 3.5K+     │
│  ChaosBlade      │  Alibaba     │  CNCF         │  Sandbox, ★ 8.5K+     │
│  Flink (major)   │  ByteDance   │  Apache       │  TLP, top contributor │
│  JuiceFS         │  JuicedData  │  —            │  ★ 10K+               │
└──────────────────┴──────────────┴───────────────┴───────────────────────┘
```

### Key Observations on China's Open-Source Trajectory

1. **From consumer to contributor to creator**: China moved from using Western open-source to major contribution (Apache member companies) to originating globally successful projects (TiDB, Doris)
2. **Apache Foundation dominance**: Chinese companies hold the most Apache TLP projects from Asia-Pacific
3. **CNCF pipeline**: Alibaba leads CNCF contributions from Asia; multiple graduated projects
4. **Commercialization pattern**: Successful open-source → commercial cloud service → enterprise adoption (TiDB → TiDB Cloud, RocketMQ → Aliyun RocketMQ)

---

## Microservices & RPC Frameworks

| Framework | Origin | Protocol | Ecosystem |
|-----------|--------|----------|-----------|
| **Dubbo** | Alibaba (Apache) | Dubbo/HTTP/gRPC | Spring Cloud Alibaba, Nacos registry |
| **HSF** | Alibaba (internal) | Custom | Used internally at Alibaba, not open |
| **gRPC** | Global | gRPC | Widely adopted, ByteDance major user |
| **TARS** | Tencent (Open-source) | TARS protocol | Tencent's internal microservice framework |
| **SOFARPC** | Ant Group | Bolt/HTTP | Part of SOFAStack |
| **Motan** | Weibo | Custom | Less widely adopted |

### Service Mesh

| Product | Status |
|---------|--------|
| **Istio/Envoy** | Widely adopted across all major clouds |
| **TSeer** | Tencent's internal service governance platform |
| **Higress** | Alibaba (CNCF) — API gateway + ingress |
| **ASM** | Aliyun managed service mesh |
| **Microservices Engine (MSE)** | Aliyun |

### Spring Cloud Alibaba

A complete microservice stack built on Spring Cloud ecosystem:
- Nacos (service discovery + config)
- Sentinel (circuit breaker, rate limiting)
- Seata (distributed transactions)
- RocketMQ (messaging)
- Dubbo (RPC)

This is the de facto standard microservice stack for Chinese Java enterprises.

---

## Government & Enterprise Adoption

### Xinchuang (信创) — Domestic Substitution

The "信息技术应用创新" (Information Technology Application Innovation) program drives replacement of foreign IT infrastructure with domestic alternatives:

```
┌─────────────────────────────────────────────────────────────┐
│                  XINCHUANG STACK                             │
├─────────────────────────────────────────────────────────────┤
│  Layer              │  Foreign (Replaced) │  Domestic        │
├─────────────────────┼─────────────────────┼──────────────────┤
│  CPU                │  Intel, AMD         │  Kunpeng, Phytium│
│                     │                     │  Loongson, Zhaoxin│
│  OS                 │  Windows, RHEL      │  openEuler,      │
│                     │                     │  Kylin, UOS      │
│  Database           │  Oracle, DB2        │  GaussDB,        │
│                     │                     │  OceanBase, TiDB │
│  Middleware         │  WebLogic, WebSphere│  TongWeb,        │
│                     │                     │  BES,东方通      │
│  Office/ERP         │  Office, SAP        │  WPS, 用友, 金蝶 │
│  Cloud              │  AWS, Azure         │  Aliyun, Huawei, │
│                     │                     │  Tencent Cloud   │
└─────────────────────┴─────────────────────┴──────────────────┘
```

### Regulatory Drivers

1. **Cybersecurity Law (2017)**: Data localization, critical infrastructure protection
2. **Data Security Law (2021)**: Data classification, cross-border transfer rules
3. **PIPL — Personal Information Protection Law (2021)**: China's GDPR equivalent
4. **Financial regulations**: PBOC mandates for distributed databases in banking

### Government Cloud Adoption

```
┌────────────────────────────────────────────────────────────┐
│              GOVERNMENT CLOUD ADOPTION PATTERN              │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐       │
│  │  National   │   │  Provincial │   │  Municipal  │       │
│  │  Gov Cloud  │──▶│  Gov Cloud  │──▶│  Gov Cloud  │       │
│  │  (部委云)    │   │  (省级云)    │   │  (市级云)    │       │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘       │
│         │                  │                  │              │
│         └──────────────────┼──────────────────┘              │
│                            │                                 │
│                   ┌────────┴────────┐                        │
│                   │  Unified Data   │                        │
│                   │  Platform       │                        │
│                   │  (政务数据平台)  │                        │
│                   └─────────────────┘                        │
│                                                             │
│  Primary vendors: Huawei Cloud, Aliyun, China Telecom        │
│  Key projects: 城市大脑 (City Brain), 数字政府 (Digital Gov) │
│  Hangzhou City Brain → Alibaba                               │
│  Shenzhen Smart City → Tencent                               │
│  National platforms → Huawei/China Telecom                   │
│                                                             │
└────────────────────────────────────────────────────────────┘
```

### Enterprise Sector Adoption

| Sector | Primary Drivers | Preferred Stack |
|--------|----------------|-----------------|
| **Banking/Finance** | Regulatory compliance, high availability | OceanBase, TDSQL, GaussDB |
| **Telecom** | Scale, Xinchuang mandate | Huawei Cloud, GaussDB |
| **Manufacturing** | Digital transformation, cost | Aliyun, Tencent Cloud |
| **Retail/E-commerce** | Elasticity, user experience | Aliyun (native), PaaS |
| **Government** | Sovereignty, compliance | Huawei Cloud, local gov cloud |
| **Internet/SaaS** | Developer experience, ecosystem | Aliyun, Tencent Cloud |

---

## Unique Characteristics

### 1. Scale-Driven Architecture

Chinese distributed systems are designed for unprecedented scale from day one:
- **Double 11**: Alibaba's Singles' Day generates 580K+ TPS peak — a crucible that shaped OceanBase, PolarDB, and Apsara
- **Spring Festival Travel Rush**: 12306 railway ticketing system handles billions of queries
- **WeChat红包 (Red Packet)**: Hundreds of millions of concurrent transactions on Lunar New Year

### 2. Regulatory-Driven Localization

- **Data localization laws** force all data to stay within China
- **Xinchuang program** creates mandated demand for domestic alternatives
- **No AWS/Azure** direct operation in China — must partner with local companies
- This creates a **self-contained ecosystem** that evolves independently

### 3. Integration Pattern

```
Vertical Integration (Huawei model):
  Chip (Kunpeng) → OS (openEuler) → DB (openGauss) → Cloud (Huawei Cloud) → Apps

Platform Integration (Alibaba model):
  E-commerce → Payments (Alipay) → Cloud (Aliyun) → Logistics (Cainiao) → Entertainment

Ecosystem Integration (Tencent model):
  Social (WeChat/QQ) → Gaming → Payments (WeChat Pay) → Cloud → Content
```

### 4. Open-Source as a Strategic Lever

- Chinese companies use open-source to:
  - Build developer mindshare and lock-in (Dubbo, RocketMQ, Nacos)
  - Export technology globally (TiDB, Doris)
  - Create de facto standards (Spring Cloud Alibaba)
  - Reduce dependence on foreign commercial software

### 5. Speed of Innovation

- **Mobile-first**: Systems designed for mobile-native workloads
- **Super-app architecture**: WeChat/Alipay super-apps demand extreme backend integration
- **Live streaming/e-commerce**: Real-time video processing at massive scale
- **AI integration**: Distributed ML training platforms (PaddlePaddle, ModelArts, PAI)

### 6. Talent & Education Pipeline

- Strong academic foundations (Tsinghua, Peking, ZJU, USTC)
- Growing distributed systems research (IoTDB from Tsinghua, TDDL from Taobao)
- Major university partnerships with tech companies
- Growing number of CNCF/APACHE committers from China

---

## Timeline of Key Milestones

```
2008 ── Apsara (飞天) distributed OS development begins at Alibaba
2009 ── Alibaba Cloud founded; Hadoop adoption begins
2011 ── TDDL (database middleware) at Taobao → later ShardingSphere lineage
2012 ── OceanBase internal development begins at Alipay
2013 ── Dubbo open-sourced by Alibaba
2014 ── PingCAP founded; TiDB development begins
2015 ── RocketMQ open-sourced by Alibaba
2016 ── SOFAStack open-sourced by Ant Group
2017 ── Cybersecurity Law enacted
     │  TiDB open-source release
     │  Alibaba donates Dubbo to Apache
2018 ── Alibaba donates RocketMQ to Apache (graduates 2019)
     │  OceanBase first external customer
     │  openGauss internal development at Huawei
2019 ── TDSQL used in core banking systems
     │  ByteDance acquires Ververica (Flink founders) expertise
2020 ── PolarDB GA
     │  GaussDB commercial launch
     │  Data Security Law draft
     │  TiDB 4.0 with HTAP (TiFlash)
2021 ── Data Security Law enacted
     │  PIPL enacted
     │  Apache Doris graduates from Apache
     │  ShardingSphere graduates from Apache
     │  openGauss open-sourced
2022 ── Huawei Cloud surpasses Tencent Cloud in revenue growth
     │  GaussDB replaces Oracle in major bank cores
2023 ── AI boom drives distributed GPU/ML infrastructure demand
     │  StarRocks gains significant market share
     │  TDengine gains international traction
2024 ── Large-scale LLM training drives distributed compute innovation
     │  Xinchuang acceleration across government sectors
     │  Apache Doris ecosystem grows significantly
2025 ── Continued Xinchuang rollout
     │  Multi-cloud/hybrid cloud adoption in enterprise
     │  Distributed database market matures
```

---

## References & Further Reading

- [Apache Software Foundation — China PMC/Committers](https://apache.org)
- [CNCF Landscape — China Projects](https://landscape.cncf.io)
- [PingCAP TiDB Documentation](https://docs.pingcap.com)
- [OceanBase Documentation](https://www.oceanbase.com)
- [openGauss Community](https://opengauss.org)
- [Apache Doris Documentation](https://doris.apache.org)
- [Alibaba Cloud Architecture Center](https://www.alibabacloud.com/architecture)
- [Huawei Cloud Reference Architecture](https://www.huaweicloud.com)
- [Tencent Cloud Database Products](https://cloud.tencent.com/product/cdb)
- [China Academy of Information and Communications Technology (CAICT) Reports](https://www.caict.ac.cn)

---

*Document generated: 2026-06-11 | For architect skill reference | Subject to ongoing evolution*
