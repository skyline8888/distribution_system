# OpenStack Swift

## 基本信息
- **发布时间:** 2010 年 7 月（Rackspace 开源）
- **开发方:** Rackspace → OpenStack 社区
- **开源协议:** Apache License 2.0
- **核心定位:** 大规模、高可用的云原生对象存储
- **项目官网:** https://swift.openstack.org
- **关键论文/文档:** Swift Architecture Guide, Rackspace Cloud Files design docs

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

2010 年前后，云计算进入爆发期。Amazon S3（2006）已验证了对象存储作为云基础设施核心组件的商业模式，但市场上**缺少一个成熟、开源、可自托管的大规模对象存储方案**。

Rackspace 运营 Cloud Files 服务（底层由 Mosso 团队构建），积累了海量对象存储运营经验。他们需要将这套经过生产验证的系统开源，以此作为对抗 Amazon 生态的武器——**用开源对象存储吸引用户进入 OpenStack 云生态**。

### 前代系统的局限性

| 前代/同期系统 | 局限 |
|---|---|
| Amazon S3 | 闭源，不可自托管，厂商锁定 |
| Lustre / GPFS | 面向 HPC 并行文件系统，POSIX 语义，不适合云对象存储场景 |
| 传统 NAS/SAN | 无法横向扩展到 EB 级，元数据集中式瓶颈 |
| 早期分布式文件系统（MogileFS） | 架构简单，缺乏完善的多租户、权限管理 |

### 硬件与网络时代背景

- 2010 年： commodity x86 服务器已成熟，单盘容量 1–2 TB，万兆以太网开始普及
- 硬件故障被视为**常态而非异常**——架构必须容忍节点级故障
- 对象存储的访问模式以 HTTP/REST 为主，不需要 POSIX 语义

---

## 2. Architecture（架构设计）

### 系统拓扑

Swift 采用 **Proxy + Storage Server 分离架构**，所有组件无单点：

```
                        ┌──────────────────────────────────────────┐
                        │            Load Balancer                 │
                        └──────────────┬───────────────────────────┘
                                       │ REST (HTTP)
                         ┌─────────────┼─────────────┐
                         ▼             ▼             ▼
                   ┌──────────┐ ┌──────────┐ ┌──────────┐
                   │  Proxy   │ │  Proxy   │ │  Proxy   │   ← 无状态，可水平扩展
                   │  Server  │ │  Server  │ │  Server  │
                   └────┬─────┘ └────┬─────┘ └────┬─────┘
                        │            │            │
            ┌───────────┼────────────┼────────────┼───────────┐
            │           │            │            │           │
            ▼           ▼            ▼            ▼           ▼
      ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
      │ Account  │ │Account/  │ │Container │ │ Container│ │  Object  │
      │ Server   │ │Container │ │ Server   │ │  Server  │ │  Server  │
      │ (SQLite) │ │  Server  │ │ (SQLite) │ │ (SQLite) │ │  (File)  │
      └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
           │             │            │            │            │
      ┌────┴─────┐ ┌────┴─────┐ ┌────┴─────┐ ┌────┴─────┐ ┌────┴─────┐
      │ Disk/FS  │ │ Disk/FS  │ │ Disk/FS  │ │ Disk/FS  │ │ Disk/FS  │
      │ XFS/Ext4 │ │ XFS/Ext4 │ │ XFS/Ext4 │ │ XFS/Ext4 │ │ XFS/Ext4 │
      └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘
```

### 三层数据模型

```
Account (租户)
  └── Container (桶/集合)
        └── Object (文件)
```

| 层级 | 类比 | 存储方式 | 元数据 |
|---|---|---|---|
| **Account** | 租户/用户 | SQLite DB | 容器列表、配额 |
| **Container** | Bucket/目录 | SQLite DB | 对象列表、ACL |
| **Object** | 文件 | 普通文件（文件系统） | xattr（扩展属性） |

每个层级都是独立的 Ring，独立管理数据分布。

### 元数据架构归类：**Ring 定位（算法计算式 + 轻量元数据存储）**

Swift 的元数据管理是**混合型**：

1. **数据定位**：通过 **Ring（一致性哈希）** 算法计算对象放置位置——不需要集中式元数据服务器来查询"数据在哪"
2. **层级元数据**：Account 和 Container 层的"列表型元数据"（有哪些容器？有哪些对象？）存储在各自的 **SQLite** 数据库中，通过复制保持一致
3. **对象元数据**：以 **xattr（扩展属性）** 形式与对象文件一起存储在本地文件系统上——元数据和数据物理同址

这属于 **算法计算式**（Ring 定位） + **本地轻量存储**（SQLite + xattr）的组合。

### 数据架构分析

#### 分块方式 (Chunking)

- **不分块**：每个 Object 作为完整文件存储
- 不支持自动条带化或对象级分片
- 大文件通过 **Static/Dynamic Large Object (SLO/DLO)** 机制实现——将大文件拆分为多个 segment object，由 manifest object 聚合
- 这与应用层分块不同，是 Swift 特有的"逻辑分段"设计

#### 分布策略 (Distribution) — Ring

Ring 是 Swift 最核心的创新，基于**一致性哈希 (Consistent Hashing)**：

```
┌─────────────────────────────────────────────────────────┐
│                     Ring 工作流程                        │
│                                                         │
│  1. 对 Account/Container/Object 名称做 MD5 哈希          │
│     hash = MD5("/account/container/object")              │
│                                                         │
│  2. 将哈希值映射到 2^32 的环形空间                        │
│     ┌─────────────────────────────────────────────┐      │
│     │  0                                    2^32  │      │
│     │  ●────partitions────●────partitions────●    │      │
│     │    partition 0        partition 1       ... │      │
│     └─────────────────────────────────────────────┘      │
│                                                         │
│  3. 环形空间划分为固定数量的分区 (partitions, 默认 ~2^18) │
│                                                         │
│  4. 每个分区映射到 1 个 Primary Device + N 个 Replica    │
│     (由 replica count 决定, 通常 3 副本)                  │
│                                                         │
│  5. Ring Builder 负责构建和重平衡映射关系                  │
│     - 引入 Zone 概念：跨故障域分布                         │
│     - 权重 (weight)：按磁盘容量/性能分配                   │
│     - Min Part Hours：防止频繁重平衡                       │
└─────────────────────────────────────────────────────────┘
```

Ring 的核心设计特点：

| 特性 | 说明 |
|---|---|
| **分区数固定** | 一旦确定（通常 2^18 = 262144），不可更改。分区是数据迁移的最小粒度 |
| **Zone 隔离** | 副本分布在不同 Zone（故障域），容忍机柜/机房级故障 |
| **Weighted 分布** | 按磁盘容量分配分区数，异构集群友好 |
| **Ring Builder** | 离线工具，管理员手动或脚本触发重平衡 |
| **Ring 文件分发** | Ring 构建后通过 rsync 分发到所有节点（Gossip 式异步传播） |
| **Min Part Hours** | 防止频繁 reblance，默认 1 小时 |

#### 冗余方式 (Redundancy) — 多副本 + 法定数

- **默认 3 副本**，可配置
- 副本跨 Zone 分布，容忍单 Zone 故障
- 纠删码 (Erasure Coding) 在后版本中作为可选方案引入，节省存储成本
- 写操作使用 **Quorum（法定数）** 机制：
  - 写成功条件：写成功数 > 副本数 / 2
  - 例如 3 副本：至少 2 个写成功即返回成功
- **异步复制**：副本间通过 Replicator 进程异步同步，采用 Merkle Hash Tree 做增量比对

#### I/O 路径

```
Client → HTTP → Proxy Server → Storage Server (WSGI) → 本地文件系统 (XFS/Ext4) → 磁盘
```

- **全部基于 HTTP/REST**，Proxy 和 Storage Server 之间也是 HTTP 通信
- Storage Server 是 WSGI 应用，运行在标准 Web 框架上
- 数据最终以**普通文件**形式存在本地文件系统
- **无内核旁路**：完全走内核 VFS → 文件系统 → 块设备路径
- 对象元数据通过 **xattr** 存储在文件系统中，与数据文件物理绑定

#### 数据生命周期管理

- **Object Expirer**：支持对象过期删除（TTL 机制）
- **Object Versioning**：支持对象版本控制
- **Object Linking**：通过 DLO/SLO 实现逻辑大文件
- **TempURL**：临时访问 URL（类似 S3 Pre-signed URL）

---

## 3. Core Technical Innovations（核心技术创新）

### Ring — 一致性哈希的数据分布

Ring 是 Swift 区别于其他对象存储的核心设计：

- **固定分区数**：不像 Chord/Kademlia 那样动态划分，Swift 的分区数在 Ring 创建时就固定。这简化了重平衡——只需移动分区，不需拆分
- **Ring Builder 离线计算**：重平衡不是自动在线进行的，而是通过 `swift-ring-builder` 工具离线计算新的映射，然后分发
- **Gossip 式 Ring 文件分发**：新的 Ring 文件通过节点间异步复制传播，类似 Gossip 协议，最终所有节点达成一致
- **Zone 感知放置**：自动确保副本分布在不同故障域

与 Dynamo 的一致性哈希对比：

| 维度 | Dynamo | Swift Ring |
|---|---|---|
| 虚拟节点 | 每物理节点数百个虚拟节点 | 固定分区，分区映射到设备 |
| 重平衡 | 虚拟节点自动迁移 | 离线 Ring Builder 计算 |
| 分区粒度 | 动态 | 固定 (2^N) |
| Zone 感知 | 无 | 原生支持 |

### 复制模型 — 最终一致 + Quorum

Swift 的复制是**完全异步的**：

```
┌─────────────────────────────────────────────────────────┐
│                  写操作流程                               │
│                                                         │
│  Client PUT object                                      │
│       │                                                 │
│       ▼                                                 │
│  ┌─────────┐                                            │
│  │  Proxy  │  通过 Ring 计算目标节点                      │
│  │ Server  │                                            │
│  └──┬──┬───┘                                            │
│     │  │  并行发送到 3 个节点                             │
│     ▼  ▼  ▼                                             │
│  ┌────┐ ┌────┐ ┌────┐                                   │
│  │ N1 │ │ N2 │ │ N3 │  ← 3 副本                         │
│  └─┬──┘ └──┬─┘ └──┬─┘                                   │
│    │       │      │                                      │
│    └───┬───┘      │   任意 2 个成功即返回 201              │
│        │ Quorum (2/3)                                    │
│        ▼                                                 │
│  Proxy 返回 201 Created                                  │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Replicator 后台进程（持续运行）                    │    │
│  │  - 扫描本地 partitions                            │    │
│  │  - Merkle Hash Tree 比对差异                      │    │
│  │  - 异步复制缺失/过期的数据                        │    │
│  │  - Tombstone 标记删除                            │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

关键机制：

| 机制 | 说明 |
|---|---|
| **Quorum 写入** | W > N/2 即返回成功，不等待所有副本 |
| **Merkle Hash Tree** | 高效的增量复制——只传输差异部分，不传全量数据 |
| **Tombstone（墓碑）** | 删除操作写入墓碑标记，防止已删除数据在复制中被恢复 |
| **Anti-entropy** | Replicator 定期扫描并修复不一致 |
| **Handoff** | 当目标节点不可用时，写入临时节点；目标恢复后再迁移回来 |

### 多服务协同架构

Swift 将不同功能拆分为独立进程：

| 进程 | 职责 |
|---|---|
| **swift-proxy-server** | 接收客户端请求，路由到后端，认证/授权 |
| **swift-object-server** | 存储/读取对象文件 |
| **swift-container-server** | 管理容器级别的元数据（对象列表） |
| **swift-account-server** | 管理账户级别的元数据（容器列表） |
| **swift-object-replicator** | 对象副本同步 |
| **swift-container-replicator** | 容器副本同步 |
| **swift-account-replicator** | 账户副本同步 |
| **swift-object-reaper** | 清理已删除对象 |
| **swift-container-updater** | 更新容器计数 |
| **swift-account-auditor** | 审计账户数据完整性 |
| **swift-object-auditor** | 审计对象数据完整性 |
| **swift-object-expirer** | 过期对象清理 |

这种**细粒度拆分**使得每个组件可以独立扩展、独立调优。

### 大文件支持

- **Dynamic Large Object (DLO)**: 客户端分段上传，自动拼接
- **Static Large Object (SLO)**: 客户端显式指定 segment 列表，更精确控制
- 最大支持约 5TB（受限于 segment 数量和 manifest 大小）

---

## 4. Trade-offs（CAP / PACELC）

### CAP 定位：AP 系统（可用性优先）

```
         Consistency
             ▲
             │
             │    Spanner
             │    HBase
             │
             │         ● Swift
             │        (AP-leaning)
             │
             │              Dynamo
             │              Cassandra
             │
─────────────┼──────────────────────────▶ Availability
             │
             │
```

- **分区容忍 (P)**：必然选择。Swift 设计为跨 Zone/数据中心部署，网络分区是预期情况
- **可用性优先 (A)**：写操作只需 Quorum 成功即可返回，不等待所有副本。读操作也允许从部分副本读取
- **最终一致性 (Eventual Consistency)**：通过 Replicator 后台进程修复不一致，不阻塞读写

### PACELC 定位

| 场景 | 选择 | 说明 |
|---|---|---|
| **Partition 时** | **A**（可用性） | 即使部分节点不可达，仍可读写 |
| **Else（正常时）** | **L**（延迟） | 不等待强一致确认，尽快返回 |

Swift 在 PACELC 中是 **PA/EL**——分区时选可用性，正常时也选延迟。

### 明确权衡

| 权衡维度 | Swift 的选择 | 代价 |
|---|---|---|
| **一致性 vs 可用性** | 可用性优先 | 可能出现读旧数据（stale read） |
| **延迟 vs 一致性** | 低延迟 | Quorum 返回后立即响应，副本可能滞后 |
| **复杂度 vs 运维** | 相对简单 | Ring 需要手动重平衡，不如 CRUSH 自动 |
| **成本 vs 性能** | 低成本，普通硬件 | 无内核旁路，HTTP 开销，不适合极低延迟场景 |
| **扩展性 vs 延迟** | 极致扩展性 | 延迟通常在几十到几百毫秒级，不适合毫秒级 SLA |

### 设计哲学总结

> **Swift 为"海量数据、可接受延迟、极端容错"的场景设计**
>
> 它不是低延迟存储。它的设计目标是：在数千台 commodity 服务器上，存储 EB 级数据，容忍任意节点/机柜故障，通过 HTTP 提供可靠存取。

---

## 5. Influence & Legacy（影响与遗产）

### 启发的后续系统

| 系统 | 受影响的设计 |
|---|---|
| **Ceph RGW** | Swift API 兼容层；Ring 概念启发了 CRUSH 的分区思想 |
| **MinIO** | 早期版本参考了 Swift 的分布式架构理念 |
| **OpenStack 生态** | 作为 OpenStack 核心组件，是 OpenStack 私有云的默认对象存储 |
| **各云厂商对象存储** | 多租户模型、Ring-like 分区分布、Quorum 写入 |

### 成为行业标准的设计模式

| 模式 | 现状 |
|---|---|
| **Account/Container/Object 三层模型** | 影响深远；S3 的 Bucket/Object 可视为精简版 |
| **Ring / 一致性哈希分区** | 分布式存储标准数据分布模式 |
| **Quorum 写入 + 最终一致** | Dynamo 风格的复制模型在对象存储中被广泛采用 |
| **Proxy 无状态前端 + 有状态存储后端** | 云存储标准架构模式 |
| **Merkle Tree 增量复制** | 高效反熵（anti-entropy）成为分布式存储标配 |
| **Tombstone 标记删除** | 最终一致系统中处理删除的标准模式 |

### 技术谱系位置

```
                    Amazon S3 (2006)
                    定义对象存储标准
                         │
                         │ 商业验证
                         ▼
              ┌──────────────────────┐
              │  Mosso Cloud Files   │  (Rackspace 前身)
              │  生产运营经验          │
              └──────────┬───────────┘
                         │ 开源 (2010)
                         ▼
              ┌──────────────────────┐
              │   OpenStack Swift    │
              │   Ring + Quorum + EC │
              └──────┬───────┬───────┘
                     │       │
          ┌──────────┘       └──────────┐
          ▼                             ▼
  OpenStack 私有云               Ceph RGW
  默认对象存储                   Swift API 兼容层
          │                             │
          ▼                             ▼
  各企业私有云                   Ceph 统一存储
  对象存储方案                   (块+文件+对象)

  Dynamo (2007) ──── 一致性哈希 ────┐
                                    │ 思想融合
                                    ▼
                          Swift Ring 设计
                          (固定分区 + Zone 感知)
```

### 当前相关性与使用场景

| 场景 | 适用性 |
|---|---|
| **OpenStack 私有云** | ✅ 核心组件，与 Keystone/Nova/Cinder 深度集成 |
| **大规模冷/温数据存储** | ✅ 归档、备份、日志存储 |
| **CDN 源站** | ✅ 配合 CDN 提供静态资源分发 |
| **极低延迟场景** | ❌ 不适合，HTTP 栈 + 最终一致引入额外延迟 |
| **小文件高频存取** | ⚠️ 可以但非最优，元数据开销较大 |
| **Hadoop/Spark 数据湖** | ⚠️ 可用但非主流（S3/HDFS 更常见） |

### 现状评估

- Swift 仍是 **OpenStack 的核心项目之一**，在电信、金融、政府等私有云场景中有稳定用户群
- 在公有云领域，S3 兼容 API 已成为绝对标准，Swift 原生 API 使用在减少
- Ceph RGW 提供 Swift 兼容层，Swift 的设计思想通过 Ceph 得到了更广泛的传播
- 作为**历史系统**，Swift 的 Ring 架构、三层数据模型、Quorum 复制等设计理念深刻影响了后续分布式对象存储的设计

---

## ASCII 完整架构总览

```
┌─────────────────────────────────────────────────────────────────────┐
│                        OpenStack Swift Architecture                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Client Layer                              │   │
│  │  REST API (Swift Native / S3 Compatible via middleware)     │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                             │                                       │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │                  Load Balancer / Auth                        │   │
│  │            (HAProxy / Keystone / TempAuth)                  │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                             │                                       │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │               Proxy Server (Stateless)                       │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │   │
│  │  │ Proxy-1 │ │ Proxy-2 │ │ Proxy-3 │ │ Proxy-N │           │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘           │   │
│  │       └────────────┼──────────┘           │                │   │
│  │                    │       Ring Lookup     │                │   │
│  └────────────────────┼──────────────────────┼────────────────┘   │
│                       │                      │                    │
│  ┌────────────────────┼──────────────────────┼────────────────┐   │
│  │              Storage Layer (Stateful)                       │   │
│  │                                                             │   │
│  │   Zone 1                Zone 2                Zone 3        │   │
│  │  ┌──────┐             ┌──────┐             ┌──────┐        │   │
│  │  │Node-A│             │Node-B│             │Node-C│        │   │
│  │  ├──────┤             ├──────┤             ├──────┤        │   │
│  │  │Acct S│             │Acct S│             │Acct S│        │   │
│  │  │Cont S│             │Cont S│             │Cont S│        │   │
│  │  │Obj  S│             │Obj  S│             │Obj  S│        │   │
│  │  │Repl  │◄──async────►│Repl  │◄──async────►│Repl  │        │   │
│  │  │Audit │             │Audit │             │Audit │        │   │
│  │  ├──────┤             ├──────┤             ├──────┤        │   │
│  │  │Disk 1│             │Disk 2│             │Disk 3│        │   │
│  │  │Disk 2│             │Disk 2│             │Disk 2│        │   │
│  │  │ ...  │             │ ...  │             │ ...  │        │   │
│  │  └──────┘             └──────┘             └──────┘        │   │
│  │                                                             │   │
│  │  Each node runs:                                            │   │
│  │  - Account Server   (SQLite: container list, metadata)      │   │
│  │  - Container Server (SQLite: object list, metadata)         │   │
│  │  - Object Server    (File + xattr: data + metadata)         │   │
│  │  - Replicator       (Merkle tree anti-entropy sync)         │   │
│  │  - Auditor          (integrity checking)                    │   │
│  │  - Updater          (async metadata updates)                │   │
│  │  - Reaper           (garbage collection)                    │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Ring (Data Distribution)                  │   │
│  │                                                             │   │
│  │  hash = MD5(path) ─► 2^32 ring ─► partition ─► devices     │   │
│  │                                                             │   │
│  │  Features:                                                  │   │
│  │  - Fixed partition count (2^N)                              │   │
│  │  - Zone-aware replica placement                             │   │
│  │  - Weighted by disk capacity                                │   │
│  │  - Offline rebalance (swift-ring-builder)                   │   │
│  │  - Gossip-style ring file distribution                      │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

  Data Model:
  ┌────────────────────────────────────────────┐
  │  Account (tenant/user)                     │
  │    └── Container (bucket)                  │
  │          └── Object (file + xattr metadata)│
  └────────────────────────────────────────────┘

  Replication:
  ┌────────────────────────────────────────────┐
  │  Write: Quorum (W > N/2, e.g., 2/3)        │
  │  Read:  Quorum (R > N/2, e.g., 2/3)        │
  │  Sync:  Async via Replicator (Merkle Tree) │
  │  Delete: Tombstone markers                  │
  │  Model: Eventual Consistency                │
  └────────────────────────────────────────────┘
```

---

## 参考资料

- OpenStack Swift Official Documentation: https://docs.openstack.org/swift/latest/
- Swift Architecture Guide: https://docs.openstack.org/swift/latest/architecture.html
- Rackspace Cloud Files / Mosso original design
- "OpenStack Swift: 大规模对象存储的实践" — 社区技术博客
- Dynamo Paper (2007) — 一致性哈希与最终一致性参考
- Ceph RGW Swift API compatibility layer documentation
