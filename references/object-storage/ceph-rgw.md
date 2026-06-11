# Ceph RGW (RADOS Gateway)

## 基本信息

| 维度 | 详情 |
|------|------|
| **系统名称** | Ceph RGW (RADOS Gateway) |
| **首次发布** | ~2012 (随 Ceph 0.48 "Bobtail" 生产就绪) |
| **开发方** | Red Hat / 社区 (Sage Weil 创始, Ceph 项目) |
| **许可协议** | LGPL / GPL v2 |
| **核心协议支持** | Amazon S3 (v2/v4), OpenStack Swift (Auth v1/v2) |
| **核心论文** | Weil, S., et al. "Ceph: A Scalable, High-Performance Distributed File System." OSDI 2006. |
| **当前版本** | 随 Ceph 主线发布 (Reef, Squid, ...) |
| **生产规模** | EB 级部署 (CERN, 中国移动, 多家云厂商) |

---

## ASCII 架构总览

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Client Layer                                        │
│                                                                              │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│   │  S3 Client   │  │ Swift Client │  │  Admin CLI   │  │  Custom App  │    │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
│          │                 │                 │                 │             │
├──────────┼─────────────────┼─────────────────┼─────────────────┼─────────────┤
│          ▼                 ▼                 ▼                 ▼             │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                   RGW Daemon (radosgw)  × N (stateless)              │  │
│   │                                                                      │  │
│   │  ┌───────────┐  ┌────────────┐  ┌───────────┐  ┌──────────────────┐ │  │
│   │  │ Beast/    │  │ Civetweb   │  │ Auth      │  │ Metadata         │ │  │
│   │  │ Civetweb  │→ │ HTTP       │→ │ Layer     │→ │ Processor        │ │  │
│   │  │ Frontend  │  │ Parser     │  │ (S3/Swift)│  │ (Bucket/User/    │ │  │
│   │  └───────────┘  └────────────┘  └───────────┘  │  Object Index)   │ │  │
│   │                                                └────────┬─────────┘ │  │
│   │  ┌───────────┐  ┌────────────┐  ┌───────────┐          │           │  │
│   │  │ Bucket    │  │ Lifecycle  │  │ CORS /    │←─────────┘           │  │
│   │  │ Versioning│  │ Policies   │  │ Website   │                      │  │
│   │  └───────────┘  └────────────┘  └───────────┘                      │  │
│   │                                                                      │  │
│   │  ┌──────────────────────────────────────────────────────────────┐   │  │
│   │  │               Multisite Sync Agent (per zone)                │   │  │
│   │  │  ┌─────────┐  ┌──────────┐  ┌─────────────────────────┐    │   │  │
│   │  │  │ Data    │→ │ Metadata │→ │ Bi-directional Conflict  │    │   │  │
│   │  │  │ Sync    │  │ Sync     │  │ Resolution (zone-level)  │    │   │  │
│   │  │  └─────────┘  └──────────┘  └─────────────────────────┘    │   │  │
│   │  └──────────────────────────────────────────────────────────────┘   │  │
│   └────────────────────────────────┬─────────────────────────────────────┘  │
├────────────────────────────────────┼─────────────────────────────────────────┤
│                                    │                                         │
│   ┌────────────────────────────────▼─────────────────────────────────────┐  │
│   │                        RADOS Layer                                   │  │
│   │                                                                      │  │
│   │   ┌─────────────────────────────────────────────────────────────┐   │  │
│   │   │                    Librados Client                          │   │  │
│   │   └────────────────────────┬────────────────────────────────────┘   │  │
│   │                            │                                         │  │
│   │   ┌────────────────────────▼─────────────────────────────────────┐   │  │
│   │   │                    OSD Map + CRUSH                            │   │  │
│   │   │   (data placement algorithm: compute → OSD, no lookup table)  │   │  │
│   │   └────────────────────────┬────────────────────────────────────┘   │  │
│   │                            │                                         │  │
│   │   ┌────────────────────────▼─────────────────────────────────────┐   │  │
│   │   │                    PG (Placement Groups)                      │   │  │
│   │   │   Objects mapped to PGs → PGs mapped to OSD sets via CRUSH    │   │  │
│   │   └────────────────────────┬────────────────────────────────────┘   │  │
│   │                            │                                         │  │
│   │   ┌────────────────────────┼─────────────────────────────────────┐   │  │
│   │   │              Replication (Primary OSD)                        │   │  │
│   │   │   Primary OSD → Secondary OSD(s) [replication or EC]          │   │  │
│   │   └────────────────────────┼─────────────────────────────────────┘   │  │
│   └────────────────────────────┼─────────────────────────────────────────┘  │
│                                │                                            │
├────────────────────────────────┼────────────────────────────────────────────┤
│   ┌────────────────────────────┼─────────────────────────────────────┐     │
│   │        Storage Nodes (OSD Daemons + BlueStore)                    │     │
│   │                                                                   │     │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │     │
│   │  │  OSD 0   │  │  OSD 1   │  │  OSD 2   │  │  OSD N   │         │     │
│   │  │ BlueStore│  │ BlueStore│  │ BlueStore│  │ BlueStore│         │     │
│   │  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │         │     │
│   │  │ │DB+WAL│ │  │ │DB+WAL│ │  │ │DB+WAL│ │  │ │DB+WAL│ │         │     │
│   │  │ │┌────┐│ │  │ │┌────┐│ │  │ │┌────┐│ │  │ │┌────┐│ │         │     │
│   │  │ ││omap││ │  │ ││omap││ │  │ ││omap││ │  │ ││omap││ │         │     │
│   │  │ │└────┘│ │  │ │└────┘│ │  │ │└────┘│ │  │ │└────┘│ │         │     │
│   │  │ └──┬───┘ │  │ └──┬───┘ │  │ └──┬───┘ │  │ └──┬───┘ │         │     │
│   │  │ ┌──┴───┐ │  │ ┌──┴───┐ │  │ ┌──┴───┐ │  │ ┌──┴───┐ │         │     │
│   │  │ │Blocks│ │  │ │Blocks│ │  │ │Blocks│ │  │ │Blocks│ │         │     │
│   │  │ │(data)│ │  │ │(data)│ │  │ │(data)│ │  │ │(data)│ │         │     │
│   │  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │         │     │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘         │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│  Multisite: Active-Active Replication across Regions                       │
│                                                                            │
│  ┌──────────────┐        ┌──────────────┐        ┌──────────────┐         │
│  │   Region A   │        │   Region B   │        │   Region C   │         │
│  │ ┌──────────┐ │        │ ┌──────────┐ │        │ ┌──────────┐ │         │
│  │ │  Zone 1  │◄┼────────┼─►  Zone 2  │ │        │ │  Zone 3  │ │         │
│  │ │(master)  │ │        │ │          │ │        │ │          │ │         │
│  │ └──────────┘ │        │ └──────────┘ │        │ └──────────┘ │         │
│  └──────┬───────┘        └──────┬───────┘        └──────┬───────┘         │
│         └───────────────────────┼───────────────────────┘                 │
│                                 │                                          │
│                      Shared Realm (metadata config)                       │
│                      Zonegroups define sync topology                     │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

在 Ceph 早期版本中，RADOS 提供了强大的分布式对象存储能力，但只能通过 `librados` 原生接口访问。这意味着：
- 只有 Ceph-aware 的客户端（RBD, CephFS）能使用
- 无法与已有的 S3/Swift 生态集成
- 无法作为 OpenStack 的 Object Storage 后端

**RGW 的核心动机**：在 RADOS 之上构建一个 **协议转换层**，使任何 S3 或 Swift 客户端能直接使用 Ceph 存储，无需修改客户端代码。

### 前代系统局限性

| 前代方案 | 局限性 |
|----------|--------|
| OpenStack Swift | 独立系统，与 Ceph RADOS 不兼容，数据格式不互通 |
| 自研 S3 网关 | 缺乏与底层分布式存储的深度集成，一致性无法保证 |
| 传统 NAS/SAN | 不支持对象存储语义（桶、版本、生命周期） |

### 硬件/网络背景

- **2010s 初期**：万兆以太网普及，对象存储开始成为云存储标准
- **S3 API 事实标准化**：Amazon S3 协议成为对象存储的事实标准，几乎所有云厂商和开源项目都采用 S3 兼容接口
- **OpenStack 崛起**：OpenStack Swift 作为云对象存储方案流行，但 RADOS 在性能和扩展性上更具优势

RGW 的诞生使得 Ceph 可以同时服务三大接口：**文件 (CephFS)**、**块 (RBD)**、**对象 (RGW)**，统一在同一个 RADOS 集群上。

---

## 2. Architecture（架构设计）

### 系统拓扑

RGW 采用 **无状态网关 + 底层分布式存储** 的混合拓扑：

```
RGW Daemons (stateless, horizontal scale-out)
    │
    ├── librados (RADOS client library)
    │
    ▼
RADOS Cluster (OSDs + MONs + MGRs)
    ├── MON: cluster map, leader election (Paxos)
    ├── OSD: data storage, replication (Primary-Secondary)
    └── MGR: metrics, management interface
```

### 2.1 数据模型与命名空间

RGW 在 RADOS 对象存储之上构建了完整的 S3/Swift 语义模型：

```
RADOS Pool 层级:
├── .rgw.root                    (realm/zone 配置)
├── .users                       (用户信息)
├── .users.email                 (email → uid 映射)
├── .users.uid                   (uid 查找)
├── .users.keys                  (access key → uid 映射)
├── .buckets.index.<bucket-id>   (bucket 索引 — 对象列表)
├── .buckets.data.<bucket-id>    (bucket 数据 — 实际对象内容)
├── .rgw.control                 (通知/事件)
├── .log                         (usage logging)
├── .intent-log                  (操作日志)
└── .rgw.meta                    (metadata pool — 桶/用户元数据)
```

**RADOS 对象结构**：
- **Bucket Index**: 存储在 `.buckets.index` pool 的 RADOS 对象中，使用 **omap (Ordered Map)** 维护对象列表。omap 是 BlueStore/RocksDB 中的键值存储，支持高效的范围查询和分页。
- **Object Data**: 实际数据存储在 `.buckets.data` pool，通过 CRUSH 算法定位到 OSD。
- **Metadata**: 桶和用户的元数据存储在 `.rgw.meta` pool，同样使用 omap。

### 2.2 元数据架构归类

**元数据架构模式：算法/计算式 (CRUSH) + 分布式存储 (omap in BlueStore)**

RGW 的元数据架构继承自 RADOS 的双层设计：

| 层面 | 机制 | 说明 |
|------|------|------|
| 数据定位 | **CRUSH 算法** | 无中心化查找表，通过哈希计算确定对象在哪个 OSD |
| 元数据索引 | **omap (RocksDB)** | BlueStore 的嵌入式 KV 存储，用于 bucket index、user index |
| 集群状态 | **Paxos (MON)** | MON 节点通过 Paxos 维护 cluster map |

这是 **算法计算 + 分布式 KV 存储** 的混合模式：
- CRUSH 消除了元数据查找的单点瓶颈（与 GFS/HDFS 的集中式 NameNode/Master 形成对比）
- omap 提供了高效的元数据查询能力（支持 list、prefix scan、分页）

### 2.3 数据架构分析

#### 分块方式 (Data Chunking)

| 类型 | 说明 |
|------|------|
| **对象级分块** | RGW 对象默认作为单个 RADOS 对象存储。大对象可通过 **multipart upload** 分片上传 |
| **RADOS 条带化** | 底层 RADOS 对象可配置条带化 (striping)，类似文件系统的 striping |
| **分片上传** | S3 Multipart Upload 将对象拆分为多个 parts，每个 part 是独立的 RADOS 对象，完成后通过 commit 操作合并 |

#### 分布策略 (Data Distribution)

| 策略 | 说明 |
|------|------|
| **CRUSH (Controlled Replication Under Scalable Hashing)** | 确定性哈希算法，将 PG 映射到 OSD 集合。客户端计算而非查询 |
| **Placement Groups (PG)** | 对象先映射到 PG，PG 再映射到 OSD 集合。这是 Ceph 的关键抽象层 |
| **PG 数量** | 典型配置：每 OSD 约 100 个 PG，保证数据分布均匀 |

```
Object → (hash(object_name) mod num_pg) → PG → CRUSH(PG) → [OSD1, OSD2, OSD3]
```

#### 冗余方式 (Data Redundancy)

| 方式 | 说明 |
|------|------|
| **副本复制** | 默认 3 副本，Primary-Secondary 模式。写入 Primary OSD，Primary 负责复制到 Secondary |
| **纠删码 (Erasure Coding)** | 可配置 EC profile (如 k=4, m=2)，节省存储空间但增加计算开销 |
| **混合** | 不同 pool 可使用不同的冗余策略（热数据副本，冷数据 EC） |

#### I/O 路径 (IO Path)

```
S3/Swift Request
    │
    ▼
┌───────────────────────────────────────┐
│  Beast Frontend (async I/O, default)  │  ← 替代旧版 Civetweb，性能更好
│  或 Civetweb (embedded HTTP server)    │
└─────────────────┬─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│  RGW Request Handler                   │
│  ├── Authentication (S3 v2/v4 sig)    │
│  ├── Authorization (IAM policy/ACL)   │
│  ├── Metadata operations (omap)       │
│  └── Data operations (librados)       │
└─────────────────┬─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│  Librados (RADOS client library)       │
│  ├── PG mapping (CRUSH calculation)   │
│  ├── Primary OSD write                │
│  └── Async/sync operations            │
└─────────────────┬─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│  OSD Daemon → BlueStore                │
│  ├── WAL (write-ahead log, NVMe)      │
│  ├── DB (RocksDB metadata, NVMe)      │
│  └── Data blocks (HDD/SSD)            │
└───────────────────────────────────────┘
```

**BlueStore 存储引擎**（现代 Ceph 默认）：
- 直接管理裸设备，绕过文件系统层
- **DB**: RocksDB 实例，存储 omap、元数据（通常放 NVMe SSD）
- **WAL**: 预写日志，保证写入原子性（通常与 DB 同盘或独立 NVMe）
- **Data**: 原始数据块（HDD 或 SSD）

#### 数据生命周期管理

| 功能 | 说明 |
|------|------|
| **Lifecycle Policies** | S3 兼容的生命周期规则，自动转换存储类或删除过期对象 |
| **Bucket Versioning** | 保留对象历史版本，每个版本有唯一 version ID |
| **Object Expiration** | 支持桶级和对象级过期策略 |
| **Multipart cleanup** | 自动清理未完成的分片上传 |

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 无状态网关设计

**核心创新**：RGW 本身是 **无状态** 的。

```
RGW Daemon A    RGW Daemon B    RGW Daemon C
    │               │               │
    └───────┬───────┴───────┬───────┘
            │               │
            ▼               ▼
        RADOS Cluster (有状态层)
```

**无状态性的含义**：
- RGW 不维护任何客户端会话状态
- 所有状态（桶、用户、对象元数据）都持久化在 RADOS 中
- 任意 RGW 实例可以处理任意请求
- 负载均衡器（HAProxy, Nginx）可随意转发请求到任意 RGW
- 扩缩容只需增减 RGW 进程，无需数据迁移

**与对比系统的差异**：
| 系统 | 网关状态 | 扩展方式 |
|------|----------|----------|
| **RGW** | 无状态 | 水平扩展，无限制 |
| **MinIO** | 无中心 (Erasure Set) | 水平扩展，但受 erasure set 限制 |
| **Swift** | 无状态 (Proxy) | 水平扩展 |
| **OSS** | 有状态 (Proxy Server) | 受限扩展 |

### 3.2 元数据存储在 RADOS omap

RGW 的所有元数据都作为 RADOS 对象存储，这是与 Swift (独立元数据数据库) 和 MinIO (本地 xl.meta) 的关键区别：

```
RADOS Object: bucket.index.0
├── header: bucket metadata (creation time, owner, ACL)
├── omap keys:
│   ├── "obj1" → { size, mtime, etag, version_id, ... }
│   ├── "obj2" → { size, mtime, etag, version_id, ... }
│   └── "obj3" → { size, mtime, etag, version_id, ... }
└── omap values: 存储完整的对象属性
```

**omap 的关键优势**：
- 与数据同一存储层，不需要额外的元数据服务
- 支持高效范围查询（list objects with prefix）
- 事务性更新（RADOS OMAP set 操作是原子的）
- 自动通过 CRUSH 分布和 RADOS 复制

### 3.3 多站点 Active-Active 复制

这是 RGW 最具创新性的特性之一。

#### Realm / Zonegroup / Zone 三层架构

```
Realm (全局命名空间)
│
├── Zonegroup A (地理区域)
│   ├── Zone 1 (master zone, read-write)
│   └── Zone 2 (secondary zone, read-write)
│
├── Zonegroup B
│   ├── Zone 3 (master zone)
│   └── Zone 4 (secondary zone)
│
└── Period (配置版本, 同步到所有 zone)
```

| 概念 | 说明 |
|------|------|
| **Realm** | 最高层级，定义全局命名空间。一个 cluster 可以有多个 realm。包含 zonegroups 的列表和 period（配置版本）。 |
| **Zonegroup** | 地理区域，包含多个 zone。定义 zone 之间的同步关系。一个 zonegroup 有一个 master zone。 |
| **Zone** | 实际部署单元。每个 zone 有独立的 RADOS 集群和 RGW 实例。zone 内所有 RGW 共享同一个 RADOS 池。 |
| **Period** | 配置的版本快照。当 realm/zonegroup/zone 配置变化时，生成新 period 并传播到所有 zone。 |

#### 同步机制

```
Zone 1 (Master)                    Zone 2 (Secondary)
┌──────────────────────┐          ┌──────────────────────┐
│  RGW instances       │          │  RGW instances       │
│       │              │          │       │              │
│       ▼              │          │       ▼              │
│  RADOS Pool          │          │  RADOS Pool          │
│  ┌────────────┐      │          │  ┌────────────┐      │
│  │ Data objs  │      │          │  │ Data objs  │      │
│  └──────┬─────┘      │          │  └──────┬─────┘      │
│  ┌──────┴─────┐      │          │  ┌──────┴─────┐      │
│  │ Index objs │      │          │  │ Index objs │      │
│  └──────┬─────┘      │          │  └──────┬─────┘      │
│  ┌──────┴─────┐      │          │  ┌──────┴─────┐      │
│  │ Meta objs  │      │          │  │ Meta objs  │      │
│  └──────┬─────┘      │          │  └──────┬─────┘      │
│         │            │          │         │            │
│  ┌──────▼─────┐      │          │  ┌──────▼─────┐      │
│  │ Sync Agent │───────────────────►│ Sync Agent │      │
│  │ (log-based)│      │     pull  │ (apply)    │      │
│  └────────────┘      │          │  └────────────┘      │
└──────────────────────┘          └──────────────────────┘
```

**同步关键点**：
- **基于日志的同步**：每个写操作记录到 RADOS 的 intent-log。sync agent 从 master zone 拉取日志变更，应用到 secondary zone。
- **Active-Active**：所有 zone 都可以接收读写请求。写冲突通过 **last-writer-wins (基于时间戳)** 或 **zone 优先级** 解决。
- **异步复制**：zone 之间的复制是异步的，存在可配置的复制延迟。
- **双向同步**：zonegroup 内的 zone 可以互相同步，实现真正的 active-active。
- **元数据同步 vs 数据同步**：元数据变更同步更快（小对象），数据同步可能滞后（大对象传输）。

### 3.4 用户管理

| 凭证类型 | 说明 |
|----------|------|
| **S3 Access Key + Secret Key** | 标准的 AWS S3 认证方式。access key 作为标识，secret key 用于签名（HMAC-SHA256）。存储在 `.users.keys` pool 的 RADOS 对象中。 |
| **Swift User + Key** | OpenStack Swift 兼容认证。用户名 + 密钥。存储在 `.users` pool。 |
| **STS Token** | 临时安全令牌（类似 AWS STS），支持短期凭证。 |
| **IAM Policy** | 桶策略和用户策略，控制细粒度访问权限。 |

用户创建流程：
```
radosgw-admin user create --uid=<uid> --display-name=<name>
    → 写入 .users 对象 (用户信息)
    → 写入 .users.uid 索引 (uid → user)
    → 写入 .users.keys 索引 (access key → uid)
    → 写入 .users.email 索引 (email → uid)
    → 写入 .rgw.meta 对象 (元数据)
```

### 3.5 一致性模型（继承自 RADOS）

RGW 的一致性完全由底层 RADOS 决定：

| 操作 | 一致性 | 说明 |
|------|--------|------|
| **对象写入** | 强一致 | RADOS Primary OSD 确保写入复制到足够副本后才确认。客户端收到 200 OK 时，数据已持久化到配置的副本数。 |
| **对象读取** | 强一致 | 读取 Primary OSD，保证读到最新写入。 |
| **列表操作 (List Bucket)** | 最终一致 | Bucket index 的更新可能滞后。写入后立即 list 可能看不到新对象。 |
| **元数据更新** | 强一致 | RADOS omap 操作是事务性的。 |
| **跨站点复制** | 最终一致 | Active-active 复制是异步的，不同 zone 之间存在延迟。 |
| **Multipart Upload** | 最终一致 | parts 写入后，commit 操作使对象可见。 |

### 3.6 性能优化

| 技术 | 说明 |
|------|------|
| **Beast Frontend** | 基于 Boost.Asio 的异步 HTTP 服务器，替代旧版 Civetweb。支持高并发连接，非阻塞 I/O。 |
| **Cache** | RGW 可配置 RADOS 缓存层，减少重复读取的 OSD 访问。 |
| **Sharded Bucket Index** | 大桶的索引可以分片存储，避免单个 RADOS 对象过大导致的性能瓶颈。 |
| **Bucket Sharding (dynamic resharding)** | 自动检测桶大小，当索引对象过大时自动分裂为多个分片索引。 |
| **Parallel Multipart Upload** | 多个 part 可以并行上传到不同的 OSD。 |
| **Data Sync Batching** | 多站点同步中，小元数据变更可以批量传输，减少网络往返。 |

### 3.7 扩展方式

| 维度 | 方式 |
|------|------|
| **RGW 网关** | 水平扩展。增加 RGW 进程即可，无状态特性保证任意增加。典型部署：每个存储节点运行 1-2 个 RGW 实例。 |
| **RADOS 存储** | 通过增加 OSD 节点和磁盘扩展。CRUSH 自动重新分布数据。 |
| **MON 节点** | 3 或 5 个（Paxos quorum），不随规模线性增长。 |
| **多站点** | 跨地理区域扩展，每个 zone 独立扩展。 |

---

## 4. Trade-offs (CAP / PACELC)

### 4.1 CAP 定位

**CAP 三角：CP 系统（继承自 RADOS）**

```
        Consistency
            ▲
            │
            │  ● RGW/RADOS
            │     (CP)
            │
            │
Availability ───────┼─────── Partition Tolerance
            │
            │
```

**RGW/RADOS 在 CAP 中的位置**：

| 属性 | 选择 | 原因 |
|------|------|------|
| **Consistency (C)** | ✅ 保证 | RADOS Primary-Secondary 复制保证强一致。写入必须复制到配置的副本数后才确认。 |
| **Availability (A)** | ⚠️ 有条件 | 网络分区时，无法到达 Primary OSD 的请求会失败。部分可用（可访问的分区仍可服务）。 |
| **Partition Tolerance (P)** | ✅ 保证 | 分布式系统必然容忍分区。分区时，只有部分数据可用。 |

**RGW 作为 CP 系统的原因**：
- RADOS 的 Primary-Secondary 复制模型要求写入时联系 Primary OSD
- 如果 Primary OSD 所在分区不可达，写入操作会阻塞或失败
- 读取操作可以从副本读取（如果配置了 replica read），但默认读取 Primary

**与 Dynamo 风格 AP 系统的对比**：

| 系统 | CAP 选择 | 复制模型 | 一致性 |
|------|----------|----------|--------|
| **RGW/RADOS** | CP | Primary-Secondary | 强一致 |
| **Dynamo/Cassandra** | AP | Active-Active (vector clock) | 最终一致 |
| **MinIO** | CP (erasure set) | Quorum (read quorum + write quorum) | 强一致 |
| **Swift** | AP | Active-Active (eventual sync) | 最终一致 |

### 4.2 PACELC 定位

```
PACELC 分析：

┌─────────────────────────────────────────────────────────┐
│  IF 网络分区 (P) THEN                                   │
│    选择 Consistency (C) — 拒绝不可达分区的写入           │
│    ELSE (正常情况)                                      │
│      选择 Consistency (C) — 读取 Primary, 写入强一致      │
│    END                                                  │
│                                                         │
│  PACELC 分类: CP-CC (分区时选C, 正常时也选C)              │
└─────────────────────────────────────────────────────────┘
```

| 场景 | 选择 | 影响 |
|------|------|------|
| **分区时** | **C** (Consistency) | 无法到达 Primary OSD 的操作会超时/失败。不会返回陈旧数据。 |
| **正常时** | **C** (Consistency) | 总是读取 Primary OSD，保证强一致。延迟比 AP 系统高（需要等待 Primary 确认）。 |

### 4.3 多站点的 CAP 权衡

多站点复制引入了 **额外的 CAP 维度**：

| 维度 | 选择 | 说明 |
|------|------|------|
| **Zone 内** | CP (强一致) | 单 zone 内的 RADOS 提供强一致。 |
| **Zone 间** | AP (最终一致) | Active-active 复制是异步的，不同 zone 之间是最终一致。写冲突通过 last-writer-wins 解决。 |

这意味着：
- **同一 zone 内的操作**：强一致
- **跨 zone 的操作**：最终一致（典型延迟：秒级到分钟级，取决于网络和数据量）
- **冲突处理**：两个 zone 同时写入同一对象 → last-writer-wins (基于时间戳)

### 4.4 复杂度 vs 运维简单性

| 维度 | 分析 |
|------|------|
| **运维复杂度** | **高**。Ceph 集群本身需要管理 MON、OSD、MGR、CRUSH map、pool 配置。RGW 增加了 realm/zone 配置、同步状态监控。 |
| **扩展运维** | 扩展简单（增加 OSD/RGW），但日常运维（监控、故障排查、数据恢复）需要专业知识。 |
| **与 MinIO 对比** | MinIO 部署简单（单二进制，erasure set 自动管理），但功能较少（无内置多站点）。Ceph 功能更丰富但运维更复杂。 |
| **与 Swift 对比** | Swift 也有 Ring 管理和分区复制，但 RGW 的 CRUSH 和 RADOS 复制更成熟。 |

### 4.5 成本 vs 性能

| 维度 | 分析 |
|------|------|
| **存储效率** | 3 副本 = 3× 存储开销。纠删码可降至 ~1.5× (k=4, m=2)，但增加 CPU 开销。 |
| **性能** | 写入延迟 = Primary OSD 确认 + 复制延迟。Beast frontend 提供高吞吐。NVMe SSD 用于 DB/WAL 显著提升元数据性能。 |
| **硬件要求** | 推荐分离 DB/WAL 到 NVMe，数据用 HDD 或 SSD。MON 需要低延迟网络（Paxos quorum）。 |
| **与 MinIO 对比** | MinIO 在纯 S3 场景下通常有更高的吞吐和更低的延迟（无 CRUSH/Paxos 开销）。但 Ceph 在统一存储（块+文件+对象）场景下有不可替代的优势。 |

---

## 5. Influence & Legacy（影响与遗产）

### 5.1 核心设计模式贡献

| 设计模式 | RGW 的贡献 | 后续影响 |
|----------|-----------|----------|
| **协议转换层** | RGW 证明了 "S3 兼容网关 + 分布式后端" 架构的可行性 | MinIO、SeaweedFS、Garage 等都采用了类似模式 |
| **无状态网关** | RGW 的无状态设计成为 S3 网关的标准模式 | 几乎所有现代 S3 兼容系统都采用无状态网关 |
| **算法式数据分布 (CRUSH)** | CRUSH 消除了集中式元数据查询，成为 Ceph 系列的核心创新 | 影响了其他弹性哈希系统 |
| **Active-Active 多站点** | RGW 的多站点复制是开源 S3 网关中最成熟的跨地域方案 | 启发了其他系统的多区域设计 |

### 5.2 技术谱系 (Technology Genealogy)

```
                    RADOS (2006)
                    /   |   \
                   /    |    \
              RBD (2008) CephFS (2010) RGW (~2012)
                  │        │          │
                  │        │          ├─→ S3 兼容网关 (主要用途)
                  │        │          ├─→ Swift 兼容网关
                  │        │          └─→ 多站点 Active-Active 复制
                  │        │
                  │        └─→ 分布式文件系统
                  │
                  └─→ 块存储

RGW 影响:
    ├── OpenStack Cinder/Nova (通过 rbd/rgw 集成)
    ├── MinIO 设计理念 (S3 原生，但独立于 RADOS)
    ├── SeaweedFS (S3 兼容 + Filer)
    └── 各云厂商 S3 兼容方案 (参考实现)
```

### 5.3 成为标准的设计模式

1. **S3 协议作为对象存储事实标准**：RGW 是最早提供完整 S3 兼容的开源实现之一，帮助确立了 S3 API 的行业标准地位。

2. **无状态网关 + 分布式存储**：这一架构模式已成为 S3 兼容系统的标准设计。RGW 证明了无状态网关可以在不牺牲一致性的前提下实现水平扩展。

3. **CRUSH 算法**：虽然是 RADOS 层的创新，但 RGW 是 CRUSH 的主要受益者之一。CRUSH 的"计算而非查询"理念影响了后续的数据分布系统设计。

### 5.4 当前相关性和使用场景

| 场景 | RGW 的优势 | 备注 |
|------|-----------|------|
| **OpenStack 对象存储** | RGW 是 OpenStack 推荐的 S3 后端（替代 Swift） | Swift 仍可用，但 RGW 更受青睐 |
| **混合云 / 边缘** | 多站点复制支持跨地域部署 | 适合数据驻留要求 |
| **统一存储平台** | 同一集群提供块/文件/对象 | Ceph 独特的竞争优势 |
| **数据湖后端** | S3 兼容使 RGW 成为 Spark/Presto/Trino 的数据湖后端 | 与 Iceberg/Hudi/Delta 配合使用 |
| **K8s 存储** | Rook 将 Ceph (含 RGW) 带入 Kubernetes | CNCF 项目，生产就绪 |

### 5.5 与竞争系统的对比

| 维度 | Ceph RGW | MinIO | OpenStack Swift |
|------|-----------|-------|-----------------|
| **S3 兼容性** | 完整 S3 + Swift | 完整 S3 | Swift 原生 + S3 中间件 |
| **后端** | RADOS (CRUSH + Paxos) | 本地磁盘 (Erasure Set) | 独立对象存储 |
| **一致性** | 强一致 (CP) | 强一致 (Quorum) | 最终一致 (AP) |
| **扩展性** | 无限制 (无状态 + RADOS) | 受 Erasure Set 限制 | 受 Ring 限制 |
| **多站点** | Active-Active (Realm/Zone) | 站点复制 (异步) | 跨区域复制 |
| **部署复杂度** | 高 | 低 | 中 |
| **统一存储** | ✅ 块+文件+对象 | ❌ 仅对象 | ❌ 仅对象 |
| **K8s 集成** | Rook (成熟) | MinIO Operator | 无官方方案 |

---

## 附录：关键配置参考

### 典型 RGW 部署池配置

```bash
# 创建 RGW 所需的 RADOS pools
ceph osd pool create .rgw.root 64
ceph osd pool create .rgw.control 64
ceph osd pool create .rgw.meta 64
ceph osd pool create .users 64
ceph osd pool create .users.email 64
ceph osd pool create .users.uid 64
ceph osd pool create .users.keys 64
ceph osd pool create .buckets.index 64
ceph osd pool create .buckets.data 64
ceph osd pool create .log 64
```

### 多站点配置流程

```bash
# 1. 创建 Realm
radosgw-admin realm create --rgw-realm=global --default

# 2. 创建 Master Zonegroup
radosgw-admin zonegroup create --rgw-zonegroup=us-east \
  --endpoints=http://rgw1:80,http://rgw2:80 --master --default

# 3. 创建 Master Zone
radosgw-admin zone create --rgw-zonegroup=us-east \
  --rgw-zone=zone1 --endpoints=http://rgw1:80 \
  --access-key=... --secret=... --default

# 4. 创建 Secondary Zone
radosgw-admin zone create --rgw-zonegroup=us-east \
  --rgw-zone=zone2 --endpoints=http://rgw3:80 \
  --access-key=... --secret=...

# 5. 更新 Period 并启动同步
radosgw-admin period update --commit
radosgw-admin sync status
```

### Bucket Index Sharding

```bash
# 启用自动 resharding
radosgw-admin bucket limit set --bucket=my-bucket \
  --max-shards=128

# 或者启用动态 resharding（Ceph Nautilus+）
radosgw-admin reshard add --bucket=my-bucket \
  --num-shards=256
```

---

## 参考资料

1. Weil, S., et al. "Ceph: A Scalable, High-Performance Distributed File System." *OSDI 2006*.
2. Ceph Documentation — [Ceph Object Gateway (RGW)](https://docs.ceph.com/en/latest/radosgw/)
3. "Ceph RADOS Gateway Architecture" — Ceph Developer Summit presentations
4. "Ceph Multi-Site Architecture" — Ceph Summit talks (2016-2023)
5. "BlueStore: Ceph's Next-Generation OSD Backend" — Ceph Developer Summit 2015
6. "CRUSH: Controlled, Scalable, Decentralized Placement of Replicated Data" — *SC 2006*
7. "Ceph Multisite: Active-Active Replication" — Red Hat Ceph Storage documentation
8. OpenStack Ceph Integration Guide — Ceph as Swift and S3 backend

---

*文档版本: 1.0 | 创建日期: 2026-06-11 | 分析框架: 5-Dim Analysis Framework*
