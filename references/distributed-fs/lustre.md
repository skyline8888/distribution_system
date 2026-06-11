# Lustre

## 基本信息
- 发布时间：2003 年
- 开发方：Cluster File Inc. → Sun Microsystems (2007) → Whamcloud/Intel (2010) → Open Source (2011)
- 开源/商用：开源（GPLv2），Intel 通过 Whamcloud 持续主导开发
- 核心论文：*Lustre: Building a File System for 1000-node Clusters* (2003); *Lustre 2.x Architecture* (Whamcloud 技术白皮书)
- 当前版本：2.15.x

## 1. Context & Motivation

### 解决的问题
Lustre 诞生于 HPC 社区对**大规模并行 I/O** 的迫切需求。2000 年代初，超级计算集群节点数从百级迈向千级，传统 NFS 和单机文件系统（ext3/ReiserFS）完全无法支撑数百节点同时读写同一文件系统的高带宽、低延迟场景。MPI-IO 应用需要一种能提供**接近聚合带宽**的文件系统。

### 前代系统的局限
- **NFS**：单服务器瓶颈，无并行 I/O，仅适合小规模
- **PVFS (Parallel Virtual FS)**：轻量但缺乏 POSIX 语义、元数据管理弱
- **GPFS (1998)**：功能强大但闭源商用，部署门槛高，许可费用昂贵

### 硬件/网络背景
- InfiniBand 开始部署（20 Gbps → 40 Gbps），为并行 I/O 提供低延迟网络
- 廉价 SATA/SAS 磁盘 + RAID 阵列使大规模存储成本大幅下降
- Linux 内核网络栈（LNET）日趋成熟，支持多种底层网络

## 2. Architecture

### 系统拓扑

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Lustre File System                           │
│                                                                      │
│  ┌───────────────────┐    ┌───────────────────┐                     │
│  │   MGS (Management │    │   MGS (Backup)     │                     │
│  │   Configuration   │◄──►│                    │                     │
│  │   Server)         │    │                    │                     │
│  └────────┬──────────┘    └───────────────────┘                     │
│           │                                                          │
│    ┌──────┴──────────────────────────────────────────────────────┐  │
│    │                       LNET Network                           │  │
│    └──────┬──────────────────────────────────────────────────────┘  │
│           │                                                          │
│  ┌────────▼──────────┐    ┌───────────────────┐                     │
│  │   MDS (Metadata   │    │   MDS (DNE)        │                     │
│  │   Server) #1      │◄──►│   (partition #2)   │                     │
│  │                   │    │   ...              │                     │
│  │   ├── OST Object  │    └───────────────────┘                     │
│  │   │   Map Table   │                                              │
│  │   └── LDLM Lock   │                                              │
│  └────────┬──────────┘                                              │
│           │                                                          │
│  ┌────────▼──────────────────────────────────────────────────┐      │
│  │                      LNET Network                           │      │
│  └────────┬──────────────────────────────────────────────────┘      │
│           │                                                          │
│  ┌────────▼────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │  OSS #1         │  │  OSS #2      │  │  OSS #N      │           │
│  │  ├── OST #1     │  │  ├── OST #3  │  │  ├── OST #2N │           │
│  │  └── OST #2     │  │  └── OST #4  │  │  └── OST #2N+│           │
│  └─────────────────┘  └──────────────┘  └──────────────┘           │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │                Lustre Client (Kernel Module)              │       │
│  │  ┌────────┐  ┌────────┐  ┌────────┐                      │       │
│  │  │ Client │  │ Client │  │ Client │  ...                 │       │
│  │  │ Node 1 │  │ Node 2 │  │ Node N │                      │       │
│  │  └────────┘  └────────┘  └────────┘                      │       │
│  └──────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────┘
```

### 节点角色

- **MGS (Management Server)**：集中存储整个 Lustre 文件系统的**配置信息**（文件系统名、MDS/OSS 列表、网络设置、挂载参数）。客户端挂载时首先联系 MGS 获取配置，随后与 MDS/OSS 直接通信。通常与 MDS 共址部署。

- **MDS (Metadata Server)**：存储所有**文件元数据**——文件名、目录结构、权限、时间戳、文件属性，以及**文件到 OST 的映射表**（即文件的 striping 布局）。一个 MDT (Metadata Target) 对应一个 MDS。

- **OSS (Object Storage Server)**：管理一个或多个 **OST (Object Storage Target)**，每个 OST 实际存储文件的**数据对象**。OSS 通过 LNET 网络为客户端提供对象级 I/O 服务。

- **Lustre Client**：**内核模块**形式运行，拦截 POSIX 系统调用，将其转换为对 MDS（元数据操作）和 OSS（数据操作）的 RPC 请求。客户端缓存元数据和数据以提升性能。

### 数据模型与命名空间

- Lustre 提供标准 **POSIX 命名空间**——层次化目录树，支持文件、目录、符号链接、硬链接
- 文件在 OST 上以**对象 (Object)** 形式存储，每个对象有唯一 FID (File Identifier)
- 目录项 (dentry) 和 inode 存储在 MDS 的后端文件系统（历史上为 ext3/ldiskfs，后支持 ZFS）

### 元数据架构归类

**模式：集中式 → 集中式分区 (Distributed Namespace Engine)**

Lustre 最初采用**单 MDS 集中式**架构——所有元数据操作路由到一个 MDS，成为扩展性瓶颈。Lustre 2.4 引入的 **DNE (Distributed Namespace Engine)** 将命名空间**分区到多个 MDS**：

| DNE 分区策略 | 说明 |
|-------------|------|
| Directory-based partitioning | 按目录子树划分，不同子树归属不同 MDT |
| File-level striping across MDTs | 大目录中的文件可分布在多个 MDT |
| Single Directory Namespace (MDD0) | 根目录必须在一个 MDT 上 |

DNE 使元数据吞吐量随 MDS 数量**线性扩展**，但也引入了跨 MDT 操作（如 rename）的协调开销。

### 数据架构分析

**模式归类：共享存储 (OST) + 条带化**

| 维度 | Lustre 设计 |
|------|------------|
| **分块方式** | 文件按可配置的 **stripe size**（默认 1MB）和 **stripe count**（跨 OST 数量）切分 |
| **分布策略** | **Round-robin** 将条带分配给 OST 列表，类似 RAID-0 |
| **冗余方式** | 无内置复制——依赖底层 OST 的 RAID（硬件 RAID 或 ZFS RAID-Z） |
| **I/O 路径** | **内核态**——Lustre Client 作为内核模块，直接通过 LNET 发 RPC |
| **生命周期** | 无内置分层/压缩/纠删码——依赖 OST 后端文件系统能力 |

**Striping 详解**：

```
File: [████████████████████████████████████]
         │        │        │        │
         ▼        ▼        ▼        ▼
Stripe: [====]  [====]  [====]  [====]
         │        │        │        │
         ▼        ▼        ▼        ▼
OST:     OST#0    OST#1    OST#2    OST#3
```

- 用户可为单个文件设置 striping 参数（`lfs setstripe`），实现**文件级粒度**控制
- stripe count = 1：单 OST 存储（默认，适合小文件）
- stripe count = 所有 OST：最大并行度（适合大文件、MPI 应用）
- stripe size：决定数据块大小，影响 I/O 模式和性能

## 3. Core Technical Innovations

### 3.1 LNET (Lustre Network)

- **自研网络抽象层**，屏蔽底层物理网络差异
- 支持 InfiniBand、Omni-Path、RoCE、TCP/IP（GigaBit Ethernet）、Cray Aries/Slingshot
- 提供 RDMA 语义（零拷贝）、多路径路由、消息可靠投递
- 自适应选择最优传输路径（根据网络类型和消息大小）

```
Application → Lustre Client → LNET → [IB | RoCE | TCP | Cray] → LNET → OSS/MDS
```

### 3.2 LDLM (Lock-Based Distributed Lock Manager)

- Lustre 的**核心一致性机制**——基于分布式锁的缓存一致性
- **锁模式**：Intent Lock（意图锁）、Extent Lock（范围锁）、Data Lock（数据锁）、Metadata Lock（元数据锁）
- **工作原理**：
  1. 客户端请求锁（通过 MDS/OSS）
  2. 锁授予后，客户端可缓存数据/元数据并本地操作
  3. 其他客户端需要同一资源时，触发**锁冲突** → 原持有者被回调 (callback) 释放/刷新锁
  4. 回调完成后再授予新请求者
- **优势**：在无冲突场景下，客户端本地缓存命中，**零网络通信**完成操作
- **代价**：锁回调增加延迟，高冲突场景（大量客户端同时写入同一文件）性能下降

### 3.3 OST 条带化 (RAID-0-like Striping)

- 文件数据按 stripe 分布在多个 OST 上，实现**并行 I/O**
- 聚合带宽 = 单 OST 带宽 × stripe count
- 客户端直接并行读写多个 OST，不经过 OSS 中转（类似 Ceph 的客户端直连）
- MPI-IO 场景下，每个进程可并行读写不同条带，实现**线性加速比**

### 3.4 DNE (Distributed Namespace Engine)

- Lustre 2.4 的重大架构升级（2014）
- 将单一 MDS 拆分为多个 MDT，每个 MDT 管理命名空间的一个分区
- **目录级分区**：子目录可分配给不同 MDT，元数据操作并行化
- **跨 MDT 操作**：rename 等涉及多个 MDT 的操作需要协调（两阶段协议）
- **MIGRATION**：支持动态迁移目录到不同 MDT 实现负载均衡

### 3.5 LLITE 客户端缓存

- Lustre Client 在内存中缓存**元数据**（dentry、inode）和**文件数据页**
- **页缓存一致性**由 LDLM 保证——写操作在锁持有期间缓存，锁释放前刷盘
- **DIO (Direct I/O)**：MPI 应用可绕过缓存直接读写，减少缓存一致性开销

### 3.6 容错机制

| 场景 | 机制 |
|------|------|
| MDS 故障 | 主备切换（failover），需共享存储或 DRBD 同步状态 |
| OSS 故障 | OST 标记为 inactive，客户端重试；数据恢复依赖底层 RAID |
| 客户端故障 | 锁自动释放（lease 机制），其他客户端可获取 |
| 网络分区 | LDLM 锁超时，客户端重新挂载 |

**注意**：Lustre 本身**不提供数据复制/纠删码**——数据可靠性完全依赖 OST 底层的 RAID 硬件或 ZFS。这是与 Ceph/GPFS 的关键差异。

## 4. Trade-offs (CAP / PACELC)

### CAP 三角定位

```
        Consistency (C)
              ▲
             / \
            /   \
     Lustre ●     \
          /         \
    (Partition: A)  \
        /             \
       /               \
 Partition ─────────── Availability (A)
```

- **CAP 选择**：Lustre 本质上是一个**CP 系统**——正常状态下通过 LDLM 提供强一致性（POSIX 语义）
- **分区时的行为**：当网络分区发生时，受影响的客户端可能暂时失去对某些资源的访问（部分不可用），而非返回过期数据。但从全局视角看，Lustre **不保证所有分区都可写**——写入操作需要获取锁，锁请求无法穿越分区时会阻塞或超时，实际上选择了**一致性 + 部分可用性**（CP 系统特征）

### PACELC 定位

```
PACELC: If Partition, then choose A or C; Else choose L or E

  Partition: C (一致性优先，锁无法获取则阻塞/失败)
  Else:      E (正常状态下一致性优先，延迟为代价)
```

| PACELC 维度 | 选择 | 说明 |
|-------------|------|------|
| **分区时 (P)** | **C** | 通过 LDLM 锁机制保证一致性；分区内客户端若无法获取锁则操作失败/超时，不返回过期数据 |
| **正常时 (E)** | **E** | 正常状态下优先保证一致性（POSIX 语义），锁回调引入额外延迟 |
| **权衡** | 一致性 > 延迟 | LDLM 在无冲突时缓存命中（低延迟），有冲突时回调（延迟增加） |

### 复杂度 vs 运维简易性

| 维度 | 评估 |
|------|------|
| **部署复杂度** | 高——需要配置 MGS、MDS、OSS、LNET 网络，每个组件有多个参数 |
| **运维复杂度** | 高——容量规划（OST 数量/布局）、striping 调优、故障排查困难 |
| **客户端要求** | 需要内核模块安装和匹配内核版本，升级需谨慎 |
| **监控** | 工具链丰富（lfs、lctl、procfs），但学习曲线陡峭 |

### 成本 vs 性能

| 维度 | 评估 |
|------|------|
| **软件成本** | 免费（GPLv2），但 Whamcloud/Intel 提供商业支持 |
| **硬件成本** | 高——需要 InfiniBand/Omni-Path 网络、高性能存储阵列、足够 RAM |
| **性能回报** | 极高——HPC 场景下可达 TB/s 级聚合带宽 |
| **TCO** | 硬件投入大，但单位带宽成本低（适合大规模、高吞吐场景） |

### 设计权衡总结

| 选择 | 代价 | 收益 |
|------|------|------|
| 无内置复制 | 依赖底层 RAID | 性能最大化、无复制开销 |
| 内核客户端 | 内核模块耦合 | 极致性能、零拷贝 |
| LDLM 锁一致性 | 高冲突时延迟增加 | 无冲突时近零延迟（缓存命中） |
| 集中式 MDS → DNE | DNE 引入跨 MDT 协调 | 元数据线性扩展 |
| 条带化 (RAID-0) | 单 OST 故障导致文件不可用 | 聚合带宽线性增长 |

## 5. Influence & Legacy

### 启发的后续系统

1. **CephFS**：借鉴了 MDS/OSS 分离架构思想，但用 CRUSH + RADOS 替代了集中式 MDS
2. **BeeGFS (原 FhGFS)**：采用类似的元数据/存储分离 + 条带化架构，但内置了复制和更简化的部署
3. **DAOS**：在元数据/数据分离思想上更进一步，使用 NVMe/SCM 优化
4. **WekaIO / VAST Data**：商业并行文件系统，继承了 Lustre 的高性能 I/O 路径设计理念

### 成为标准的设计模式

| 模式 | 传播 |
|------|------|
| **MDS/OSS 分离** | Lustre → CephFS → BeeGFS → DAOS |
| **文件级 striping** | Lustre → GPFS → HDFS striping → DAOS |
| **客户端直连存储节点** | Lustre → Ceph → MinIO |
| **Lock-based 缓存一致性** | GPFS DLM → Lustre LDLM → Ceph MDS lock |
| **LNET 网络抽象层** | 启发了后续存储系统的网络层设计 |

### 技术谱系位置

```
PVFS (1993)
  │
  ├───► GPFS (1998) ── token-based DLM, striping
  │       │
  │       └───► CephFS (2006) ── 分布式 MDS, CRUSH
  │
  └───► Lustre (2003) ── MDS/OSS 分离, LDLM, striping
          │
          ├───► BeeGFS (2009) ── 简化版分离架构
          ├───► DAOS (2020) ── NVMe/SCM 优化的继承者
          └──► WekaIO (2014) ── 商业化高性能路径
```

### 当前相关性和使用场景

**Lustre 仍然是 HPC 领域的标准文件系统**：

| 场景 | 地位 |
|------|------|
| **TOP500 超算** | 超过 50% 使用 Lustre 作为并行文件系统 |
| **AI/ML 训练** | 大规模 GPU 集群的高吞吐数据供给首选 |
| **基因组学/气候模拟** | PB 级数据并行处理 |
| **云原生 HPC** | 通过 FSx for Lustre (AWS) 实现云化部署 |

**活跃发展**：
- Lustre 2.14+ 支持 ZFS 后端、增强 DNE、改进 RDMA 性能
- 云厂商提供托管服务（AWS FSx for Lustre、阿里云文件存储 HDFS 兼容层）
- Whamcloud (Intel) 持续维护上游开发

### 局限与教训

| 局限 | 影响 |
|------|------|
| **单点故障（MDS）** | DNE 缓解了但根目录仍单点 |
| **无内置复制** | 数据可靠性完全依赖底层 RAID |
| **内核模块依赖** | 客户端升级需重启，与云原生趋势不符 |
| **小文件性能** | 大量小文件场景不如对象存储 |
| **运维门槛** | 需要专业团队维护，不适合中小规模 |
