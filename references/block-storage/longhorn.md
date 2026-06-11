# Longhorn

## 基本信息
- **发布时间：** 2019 年正式开源（早期原型可追溯至 2016–2017 Rancher Labs 内部项目 Convoy 的演进）
- **开发方：** Rancher Labs（2020 年被 SUSE 收购，现为 SUSE 旗下项目）
- **开源协议：** Apache 2.0
- **定位：** Kubernetes-native 分布式块存储系统
- **官网/源码：** <https://longhorn.io> · <https://github.com/longhorn/longhorn>
- **核心语言：** Go（控制平面）+ Go（数据平面 engine）

---

## 1. Context & Motivation

### 解决的问题

在 Longhorn 出现之前，Kubernetes 生态的持久化块存储方案主要有两类：

| 方案 | 痛点 |
|------|------|
| **云厂商 EBS/CBS 等** | 强绑定特定云、无法跨云、多云/混合云场景无法使用 |
| **Ceph RBD** | 架构厚重（MON/OSD/MDS/MGR 多组件）、运维复杂、与 K8s 集成是通过 CSI 外挂而非原生设计 |
| **NFS/iSCSI 传统方案** | 无副本机制、单点故障、不可靠 |

Longhorn 要解决的**核心问题**：
1. **Kubernetes-native**：第一个从零开始为 K8s 设计的分布式块存储，而非将外部存储"塞进"K8s
2. **轻量运维**：全部通过 CRD 和 K8s 原生资源（Deployment/DaemonSet）部署，无需独立运维团队
3. **每卷独立控制器**：每个卷拥有自己的 controller 进程，故障隔离，避免"一损俱损"
4. **多云/边缘友好**：不依赖特定云基础设施，能在裸金属、边缘节点、混合云上运行

### 前代系统局限

- **Ceph**：功能全面但架构复杂，RADOS 的 CRUSH 算法和 Paxos MON 对中小型集群过重
- **传统 SAN**：硬件锁定、扩展困难、成本高
- **Convoy（Rancher 前身）**：Rancher 2016 年的存储项目，采用 Docker plugin 模式，架构不够云原生，后被 Longhorn 替代

### 硬件/网络背景

- 2019 年 NVMe SSD 逐渐普及，单节点本地磁盘 IOPS 可达数十万
- K8s 1.13+ CSI 成熟（GA），提供了标准化的容器存储接口
- 边缘计算、GitOps 场景需要轻量级本地持久化方案

---

## 2. Architecture

### 系统拓扑

Longhorn 采用**控制平面与数据平面严格分离**的微服务架构，所有组件都以 K8s 原生资源运行。

#### 整体组件架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                        │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │ Longhorn     │    │ Longhorn     │    │ Longhorn     │       │
│  │ UI           │    │ Driver (CSI) │    │ Settings     │       │
│  │ (Dashboard)  │    │ + Provisioner│    │ Manager      │       │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘       │
│         │                   │                   │                │
│         └───────────────────┼───────────────────┘                │
│                             ▼                                    │
│              ┌──────────────────────────────┐                    │
│              │    Longhorn Manager          │                    │
│              │  (DaemonSet, 每节点一个)      │                    │
│              │  • CRD Watch & 协调          │                    │
│              │ • REST API                   │                    │
│              │  • 卷调度 & 节点管理          │                    │
│              └──────────┬───────────────────┘                    │
│                         │                                        │
│          ┌──────────────┼──────────────┐                        │
│          ▼              ▼              ▼                        │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐                  │
│  │ Instance   │ │ Instance   │ │ Instance   │  ...每节点       │
│  │ Manager    │ │ Manager    │ │ Manager    │                  │
│  │ (Controller│ │ (Replica)  │ │ (Replica)  │                  │
│  │  / Engine) │ │            │ │            │                  │
│  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘                  │
│        │              │              │                          │
│  ┌─────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐                  │
│  │  Volume A   │ │  Volume B   │ │  Volume C   │  ← 数据文件   │
│  │  (ext4/xfs) │ │  (ext4/xfs) │ │  (ext4/xfs) │    /var/lib/  │
│  └────────────┘ └────────────┘ └────────────┘    longhorn/     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

#### 单卷数据路径（Per-Volume Architecture）

Longhorn 的核心设计理念是**每个卷有自己独立的控制进程**：

```
┌─────────────────────────────────────────────────────────────────┐
│                     Per-Volume: Volume "vol-01"                  │
│                     (3 replicas, sync replication)               │
│                                                                  │
│   kubelet                                                        │
│     │                                                            │
│     ▼                                                            │
│  ┌─────────────┐                                                 │
│  │  CSI Plugin  │  ← csi.sock (NodePublish/NodeStage)            │
│  │  (longhorn-  │                                                 │
│  │   csi.sock)  │                                                 │
│  └──────┬──────┘                                                 │
│         │ iSCSI / NBD / tgt                                      │
│         ▼                                                        │
│  ┌──────────────────────────────────────────────────┐            │
│  │        Instance Manager: Engine (Controller)      │            │
│  │        (每卷一个独立进程)                           │            │
│  │                                                    │            │
│  │  ┌──────────────────────────────────────────┐    │            │
│  │  │  Longhorn Engine (Go binary)              │    │            │
│  │  │  • 暴露块设备 (iSCSI target)               │    │            │
│  │  │  • 同步复制到所有 Replicas                 │    │            │
│  │  │  • 读写请求协调                            │    │            │
│  │  │  • 故障检测 & 自动重建                     │    │            │
│  │  └──────────────────────────────────────────┘    │            │
│  └──────────────────────┬───────────────────────────┘            │
│                         │  sync replication (TCP)                │
│         ┌───────────────┼───────────────┐                       │
│         ▼               ▼               ▼                       │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │
│  │  Replica 1   │ │  Replica 2   │ │  Replica 3   │               │
│  │  (node-a)    │ │  (node-b)    │ │  (node-c)    │               │
│  │             │ │             │ │             │               │
│  │  ┌───────┐  │ │  ┌───────┐  │ │  ┌───────┐  │               │
│  │  │data   │  │ │  │data   │  │ │  │data   │  │               │
│  │  │.sparse│  │ │  │.sparse│  │ │  │.sparse│  │  ← 稀疏文件   │
│  │  │       │  │ │  │       │  │ │  │       │  │    映射到本地  │
│  │  │meta   │  │ │  │meta   │  │ │  │meta   │  │    磁盘        │
│  │  │.db    │  │ │  │.db    │  │ │  │.db    │  │               │
│  │  └───────┘  │ │  └───────┘  │ │  └───────┘  │               │
│  └─────────────┘ └─────────────┘ └─────────────┘               │
│                                                                  │
│  写流程:                                                          │
│  kubelet → CSI → Engine → [R1 ✅, R2 ✅, R3 ✅] ← 全部确认后才返回 │
│  读流程:                                                          │
│  kubelet → CSI → Engine → R1 (primary read)                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

#### Engine Image 分离

Longhorn 将 **Engine 逻辑** 与 **Engine Image（二进制版本）** 分离：

```
┌────────────────────┐
│  Engine Image      │  ← Docker image / 二进制文件
│  (版本化)           │     例如: longhornio/longhorn-engine:v1.6.0
│  • 只读、不可变     │
│  • 支持多版本共存   │     允许滚动升级，不同卷可使用不同 engine 版本
│  • 每节点缓存       │
└─────────┬──────────┘
          │ 被 Instance Manager 加载
          ▼
┌────────────────────┐
│  Instance Manager  │  ← 每节点一个 Pod
│  • 管理本节点       │
│  • 所有 Engine/     │
│    Replica 进程     │
│  • 进程级隔离       │
└────────────────────┘
```

这种分离使得：
- Engine 升级不需要重启整个 Longhorn 系统
- 不同卷可以逐步迁移到新版本（蓝绿升级）
- Engine crash 只影响单个卷

### 数据模型与命名空间

Longhorn 通过 K8s CRD 定义核心资源：

| CRD | 作用 |
|-----|------|
| `Volume` | 卷的声明式定义（大小、副本数、前端类型等） |
| `Engine` | 卷的 controller 实例（一个 Volume 对应一个 Engine） |
| `Replica` | 卷的副本实例（一个 Volume 对应 N 个 Replica） |
| `InstanceManager` | 节点级进程管理器 |
| `BackingImage` | 镜像 backing（用于快速克隆） |
| `Backup` / `BackupVolume` | 备份元数据 |
| `Node` / `Setting` | 节点管理与全局配置 |

命名空间：`longhorn-system`，所有 Longhorn 组件运行在此命名空间下。

### 元数据架构归类

**归类：集中式协调 + 分布式数据（6 大模式中的"集中式分区"变体）**

- **元数据管理**：Longhorn Manager（DaemonSet）通过 watch K8s CRD 来协调所有卷的调度和状态，元数据实际存储在 K8s etcd 中
- **数据管理**：每个卷的 Engine-Replica 组成独立的副本组，数据分布在集群各节点上
- **调度决策**：Longhorn Manager 决定哪个 Replica 放在哪个节点（考虑磁盘空间、亲和性、反亲和性）

### 数据架构分析

#### 分块方式 (Data Chunking)
- **不显式分块**：Longhorn 不在卷内部做固定大小的 chunk 切分
- 每个 Replica 在本地磁盘上使用**稀疏文件（sparse file）**存储整个卷数据
- 文件系统层（ext4/xfs）处理块设备之上的文件级分块
- 通过**thin provisioning**实现按需分配物理空间

#### 分布策略 (Data Distribution)
- **全量复制（非分片）**：每个 Replica 持有完整的数据副本，而非数据分片
- 调度器通过**反亲和性**策略将 Replica 分散到不同节点，确保故障域隔离
- 不支持跨卷数据条带化（striping），这是与 Ceph RBD 的关键区别

#### 冗余方式 (Data Redundancy)
- **同步复制（Synchronous Replication）**：写请求必须被所有 Replica 确认后才返回成功
- 副本数可配置（默认 3，最小 1，最大可根据集群规模调整）
- 支持自动重建（auto-rebuild）：检测到 Replica 失效后自动从健康副本同步数据
- 不支持纠删码（Erasure Coding）——这是与 Ceph 的重要差异

#### I/O 路径 (IO Path)

```
应用 Pod
  │
  ▼
kubelet (通过 CSI NodePublishVolume mount block device)
  │
  ▼
CSI Plugin (longhorn-csi-plugin, 每节点 DaemonSet)
  │  暴露为 /dev/longhorn/<volume-name> 或通过 iSCSI/NBD
  ▼
Instance Manager: Engine (该卷的 controller 进程)
  │  数据平面：Go 编写的 engine 二进制
  │  • 接收 I/O 请求
  │  • 同步转发到所有 Replicas
  │  • 等待所有 Replica ACK
  ▼
Instance Manager: Replica (每节点一个 replica 进程)
  │  将数据写入本地稀疏文件
  │  路径: /var/lib/longhorn/replicas/<volume-name>-<UUID>/
  ▼
本地磁盘 (SSD/HDD)
```

- **内核态数据平面**：通过 iSCSI 或 NBD 暴露块设备，最终走内核 TCP/IP 栈
- **用户态控制**：Engine/Replica 是用户态 Go 进程
- 网络协议：Engine ↔ Replica 之间使用自定义 TCP 协议进行同步复制

#### 生命周期管理 (Data Lifecycle)
- **快照（Snapshots）**：基于 copy-on-write，快照元数据通过 `.db` 文件管理
- **备份（Backups）**：快照可异步上传到外部存储（NFS 或 S3 兼容对象存储）
- **卷扩展（Volume Expansion）**：支持在线扩展（online expansion），不需要卸载卷
- **回收（Reclaim）**：支持 K8s PV reclaimPolicy（Delete/Retain）

---

## 3. Core Technical Innovations

### 一致性模型

**强一致性（Strong Consistency）**

Longhorn 采用**同步复制模型**：
- 写请求：Engine 收到写 I/O → 并行发送给所有 Replica → 等待**全部** Replica 返回 ACK → 才向 kubelet 返回成功
- 读请求：默认从 Engine 本地缓存或最近的 Replica 读取
- **只要有一个 Replica 失败，写操作即失败**（除非配置了容忍度）

这保证了：任何成功返回的写操作，数据在所有健康副本上都已持久化。

### 共识协议

**Longhorn 不使用 Paxos 或 Raft**。

与传统分布式存储不同，Longhorn 的共识机制是**基于同步复制的简单多数模型**：
- 不需要选举 Leader（Engine 本身就是该卷的唯一写入协调者）
- 不需要日志复制（数据直接写入 replica 文件，而非先写 WAL 再 apply）
- 故障检测：Engine 定期心跳检测 Replica 健康状态
- 自动重建：发现失效副本后，从健康副本创建新的 Replica 并同步数据

这是一种**简化的副本协调模型**，适合 K8s 环境（etcd 已经处理了集群级共识）。

### 容错机制

| 容错场景 | 处理方式 |
|----------|----------|
| **单个 Replica 失效** | Engine 检测到，标记为 Faulted，自动触发重建 |
| **Engine 进程崩溃** | Instance Manager 自动重启该 Engine 进程 |
| **节点宕机** | 该节点上的 Replica 标记为 Failed，从其他节点的 Replica 重建 |
| **网络分区** | Engine 无法联系 Replica → 该 Replica 标记为错误 → 重建 |
| **磁盘满** | Longhorn 监控磁盘使用率，超过阈值可配置自动禁止调度 |
| **全集群断电恢复** | 通过 K8s 重启后自动恢复卷状态（依赖 etcd 中的 CRD 状态） |

**重建过程（Rebuild）**：
```
1. Longhorn Manager 创建新的 Replica 资源
2. Instance Manager 在目标节点启动 Replica 进程
3. Engine 从健康 Replica 读取数据，同步写入新 Replica
4. 同步完成后，新 Replica 加入数据路径
```

### 性能优化技术

| 优化 | 说明 |
|------|------|
| **每卷独立 Engine** | 故障隔离，避免单点瓶颈影响所有卷 |
| **Thin Provisioning** | 按需分配物理存储，减少初始 I/O |
| **Snapshot CoW** | Copy-on-write 快照，不复制实际数据，仅记录差异 |
| **增量备份** | 备份只传输相对于上一次备份的变更块 |
| **数据 locality** | 可配置 local 模式，让 Engine 和至少一个 Replica 在同一节点，减少网络读延迟 |
| **Write-back cache** | Engine 层可配置缓存策略 |
| **多前端支持** | iSCSI（默认）、NBD、Block Device，适应不同场景 |

### 扩展方式

Longhorn 支持**两种扩展维度**：

**Scale-out（水平扩展）**：
- 添加新 K8s 节点 → Longhorn Manager 自动发现 → 新节点可承载新的 Replicas
- 每个新卷的 Replica 可调度到新节点
- 受限于副本数 × 卷数的增长

**Scale-up（垂直扩展）**：
- 单卷可扩展容量（在线扩展）
- 增加单卷的副本数（需要重建）
- 增强单个节点的磁盘/SSD 能力

**与 Ceph 的关键区别**：Ceph 通过增加 OSD 实现透明数据重平衡；Longhorn 的新节点只承载新创建的 Replica，已有卷的数据不会自动迁移到新节点（除非手动触发重建）。

---

## 4. Trade-offs (CAP / PACELC)

### CAP 定位

```
          Consistency (C)
               ▲
               │
               │  ● Longhorn
               │  (CP 系统)
               │
               │
               ├──────────────────────►
               │    Availability (A)
               │
```

**Longhorn 是 CP 系统（Consistency + Partition Tolerance）**

| 维度 | 选择 | 原因 |
|------|------|------|
| **C (Consistency)** | ✅ 强一致 | 同步复制确保所有副本数据一致 |
| **A (Availability)** | ⚠️ 有条件 | 写操作需要所有 Replica 确认；副本不足时拒绝写入 |
| **P (Partition Tolerance)** | ✅ | 网络分区时牺牲可用性保持一致性 |

**分区时的行为**：
- Engine 无法联系某个 Replica → 该 Replica 标记为 Faulted → 写操作失败（除非还有足够副本）
- 如果只剩 1 个 Replica（低于最小副本阈值），卷变为 Degraded 或 Faulted 状态
- **不会为了可用性而接受数据不一致**

### PACELC 定位

```
PACELC:
  分区时 (P): 选 C（一致性）— 拒绝不一致的写入
  正常时 (E): 选 C（一致性）— 同步复制，牺牲延迟换取一致性
```

| 场景 | 选择 | 说明 |
|------|------|------|
| **Partition → C vs A** | **C** | 同步复制，不一致就拒绝写入 |
| **Else → L vs C** | **C** | 正常运行时也坚持同步复制，写入延迟 = max(replica latencies) |

**代价**：写入延迟取决于**最慢的那个 Replica**（tail latency），而非最快的那个。

### 复杂度 vs 运维

| 维度 | Longhorn 的优势 | Longhorn 的代价 |
|------|----------------|----------------|
| **部署** | 全部 CRD + K8s 原生，helm install 即可 | 每卷一个 Engine/Replica 进程 → Pod 数量膨胀 |
| **运维** | 内置 UI，可视化快照/备份/监控 | 大规模集群（数千卷）时管理开销增大 |
| **升级** | Engine Image 分离支持滚动升级 | 升级过程需要逐个卷迁移，耗时较长 |
| **监控** | 内置 Prometheus metrics | 需要额外配置告警规则 |

### 成本 vs 性能

| 维度 | 分析 |
|------|------|
| **存储效率** | 全量副本（3 副本 = 3× 物理存储），不如 Ceph 的纠删码高效 |
| **I/O 延迟** | 同步复制增加写入延迟；网络 I/O = 写请求 × 副本数 |
| **网络带宽** | 写放大明显：1× 写入请求 → N× 网络传输（N = 副本数） |
| **重建带宽** | 重建期间大量数据同步，可能影响正常 I/O |
| **硬件要求** | 推荐 SSD；HDD 性能较差（同步复制放大 I/O 延迟） |

**适用场景**：中小规模 K8s 集群（几十到几百个卷），对运维简单性要求高于极致性能。

**不适用场景**：数千节点大规模集群、需要纠删码节省存储成本、需要条带化极致性能。

---

## 5. Influence & Legacy

### 启发的后续系统

Longhorn 作为"第一个真正为 K8s 设计的分布式块存储"，开创了以下范式：

| 后续系统 | 受影响的设计 |
|----------|-------------|
| **OpenEBS Mayastor** | 学习了 per-volume controller 思想（虽然 Mayastor 基于 SPDK 做了性能优化） |
| **OpenEBS Jiva** | 直接借鉴了 Engine-Replica 架构模式 |
| **Harvester (SUSE)** | Longhorn 是 Harvester 超融合基础设施的默认存储后端 |
| **Rook 生态** | 虽基于 Ceph，但在 K8s 集成模式上受 Longhorn 启发简化了部署 |

### 成为标准的设计模式

Longhorn 贡献给 K8s 存储生态的**核心设计模式**：

1. **Per-Volume Controller**：每个卷独立进程，故障隔离 → 成为 K8s 存储的事实标准
2. **CRD-Driven Storage**：用 K8s CRD 声明式管理存储 → 后被多数 K8s 存储项目采用
3. **Engine Image Versioning**：控制平面与数据平面版本分离 → 支持无损升级
4. **Instance Manager Pattern**：节点级进程管理器统一管理所有卷的 engine/replica → 降低 Pod 数量
5. **Thin Provisioning + Sparse File**：K8s 环境下按需分配存储 → 节省初始资源

### 技术谱系位置

```
存储系统演进谱系：

传统 SAN/NAS
    │
    ├── Ceph RBD (2012) ────── 分布式、重量级、RADOS 底层
    │
    ├── Convoy (2016, Rancher) ── Docker plugin 模式
    │        │
    │        ▼
    │    Longhorn (2019, Rancher/SUSE)  ← 本系统
    │        │
    │        ├──→ OpenEBS Jiva (借鉴 Engine-Replica)
    │        ├──→ Harvester 默认存储 (2021)
    │        └──→ 影响 K8s CSI 生态设计模式
    │
    └── OpenEBS (2017)
         ├── cStor (ZFS 底层)
         ├── Jiva (类 Longhorn 架构)
         └── Mayastor (SPDK, 高性能方向)
```

### 当前相关性与使用场景

**活跃项目**：
- CNCF 沙箱项目（2020 年加入）
- SUSE 商业支持（Rancher Prime）
- GitHub 7k+ Stars，活跃社区贡献
- 最新稳定版本持续发布（1.x 系列）

**典型使用场景**：
- 中小型 K8s 集群的持久化存储
- 边缘计算节点（轻量级、自包含）
- 开发/测试环境的本地持久化
- 多云/混合云环境下的统一存储方案
- 需要快照/备份能力的场景（数据库、有状态应用）

**竞争定位**：

```
轻量 ←──────────────────────────────────→ 重量级
  │                                        │
  Longhorn      OpenEBS         Ceph RBD   传统 SAN
  (最轻)        (中等)          (最重)     (硬件)
  │                                        │
K8s-native ←────────────────────────────→ 独立部署
```

Longhorn 在"轻量 + K8s-native"象限占据独特位置：比 Ceph 简单得多，比 local-path-provisioner 多了副本和高可用，比云厂商 EBS 多了跨云能力。

---

## 总结：Longhorn 的架构哲学

> **"Simple by design, native by default."**

Longhorn 的设计哲学可以概括为三个原则：

1. **K8s-native 不是集成，而是基因**：不从外部系统适配 K8s，而是从第一天就用 K8s 的方式思考（CRD、Controller、DaemonSet）
2. **简单优于功能全面**：选择同步副本而非纠删码、选择全量复制而非条带化、选择 iSCSI/NBD 而非 RDMA — 以运维简单换取功能克制
3. **故障隔离是第一优先级**：每卷独立 Engine 的设计使得单卷故障不影响其他卷，这在高可用场景下是决定性的优势

Longhorn 不是性能最极致的块存储，也不是功能最全面的，但它是在"正确抽象"和"可运维性"之间找到最佳平衡点的 K8s 存储方案。

---

## 参考资料

- Longhorn 官方文档: <https://longhorn.io/docs/>
- Longhorn GitHub: <https://github.com/longhorn/longhorn>
- Longhorn Architecture 文档: <https://longhorn.io/docs/latest/references/longhorn/>
- Longhorn Engine 架构: <https://longhorn.io/docs/latest/references/longhorn-engine/>
- Rancher 博客 - Longhorn 设计原理: <https://www.rancher.com/blog/>
- CNCF Landscape: <https://landscape.cncf.io/>
- Kubernetes CSI 文档: <https://kubernetes-csi.github.io/docs/>
