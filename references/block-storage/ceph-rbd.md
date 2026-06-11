# Ceph RBD (RADOS Block Device)

## 基本信息
- **首次发布:** 2010 (随 Ceph 0.18), 生产成熟于 Ceph 0.50+ (2012)
- **开发方:** Inktank → Red Hat (2014 收购) → Ceph 社区 (Linux Foundation)
- **开源协议:** LGPLv2.1 (librbd), GPL (内核模块)
- **核心论文:** Sage Weil, *Ceph: A Scalable, High-Performance Distributed File System*, OSDI 2006
- **官方文档:** https://docs.ceph.com/en/latest/rbd/

---

## 1. Context & Motivation

### 解决的问题
传统块存储依赖集中式 SAN 阵列（FC-SAN、iSCSI），存在以下瓶颈：
- **容量天花板:** 单个阵列控制器的扩展能力有限，scale-up 有物理上限
- **供应商锁定:** 专有硬件 + 专有协议，迁移成本高
- **单点故障:** 控制器故障 = 所有挂载的块设备不可用
- **弹性不足:** 难以在虚拟化/云环境中按需动态供给

### 前代系统的局限
| 系统 | 局限 |
|------|------|
| FC-SAN | 需要专用光纤网络，拓扑刚性，扩展成本高 |
| iSCSI + 集中式阵列 | 控制器是瓶颈，IOPS 和容量耦合在同一硬件 |
| DRBD | 仅支持双节点镜像，无法 scale-out |
| LVM over SAN | 无快照/COW 等高级功能，依赖阵列厂商实现 |

### 硬件与网络背景
RBD 的出现受益于几个趋势：
- **廉价 SAS/SATA 磁盘的普及:** 可用普通 x86 + 磁盘构建存储集群
- **10GbE 网络推广:** 块存储对网络延迟敏感，10GbE 降低了网络门槛
- **虚拟化浪潮:** KVM/QEMU 成熟，云平台（OpenStack）需要软件定义的块存储后端
- **RADOS 的成熟:** Ceph 底层对象存储 RADOS 提供了可靠的数据放置与复制机制，天然适合作为块设备的后端

RBD 的核心思路：**在 RADOS 对象存储之上构建块设备抽象，利用 CRUSH 算法实现数据自动条带化分布，无需集中式控制器。**

---

## 2. Architecture

### 系统拓扑

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Host                               │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐  │
│  │  VM / App   │    │  VM / App   │    │  Filesystem (ext4/  │  │
│  │  (QEMU)     │    │  (QEMU)     │    │   xfs on /dev/rbd0) │  │
│  └──────┬──────┘    └──────┬──────┘    └──────────┬──────────┘  │
│         │                  │                       │            │
│  ┌──────▼──────────────────▼───────────────────────▼──────────┐ │
│  │                    librbd (用户态库)                         │ │
│  │  - 镜像映射 (image mapping)                                 │ │
│  │  - 对象切分 & 条带化计算                                     │ │
│  │  - I/O 调度、缓存 (rbd_cache)                                │ │
│  │  - 快照/克隆语义处理                                         │ │
│  └──────────────────────┬─────────────────────────────────────┘ │
│         ┌───────────────┼───────────────┐                        │
│         ▼               ▼               ▼                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │  librados   │  │  librados   │  │  librados   │  (每个 OSD   │ │
│  │  (pool: vm) │  │  (pool: vm) │  │  (pool: vm) │   连接一条)  │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
│         │                │                │                      │
└─────────┼────────────────┼────────────────┼──────────────────────┘
          │   TCP/MSGv2    │   TCP/MSGv2    │   TCP/MSGv2/RDMA
          ▼                ▼                ▼
┌─────────┴────────────────┴────────────────┴──────────────────────┐
│                        Ceph Cluster                               │
│                                                                   │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐           │
│  │   OSD.0     │    │   OSD.1     │    │   OSD.2     │  ...      │
│  │  BlueStore  │    │  BlueStore  │    │  BlueStore  │           │
│  │  /dev/sda   │    │  /dev/sdb   │    │  /dev/sdc   │           │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘           │
│         │                  │                  │                   │
│         └──────────────────┼──────────────────┘                   │
│                   CRUSH Map (数据分布)                             │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                      MONs (Paxos/Quorum)                     │  │
│  │   - Cluster map, CRUSH map, OSD map                          │  │
│  │   - 提供集群拓扑和 PG 映射信息                                │  │
│  └─────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────┘
```

### 数据模型与命名空间

RBD 的数据模型是一个 **三层映射**：

```
Block Device (逻辑块地址 LBA)
    │
    ▼ 切分 (默认 4MB object_size)
    │
RADOS Objects (rbd_data.<image-id>.0000000000000000)
    │
    ▼ CRUSH 映射
    │
OSD 物理存储 (BlueStore → NVMe/SATA/SAS)
```

**关键概念:**

| 概念 | 说明 |
|------|------|
| **RBD Image** | 一个块设备，在 RADOS pool 中表现为一组对象 |
| **RADOS Object** | 默认 4MB，按顺序编号（object 0, 1, 2, ...） |
| **RADOS Pool** | 对象容器，RBD image 存储在指定 pool 中 |
| **PG (Placement Group)** | 对象 → PG → OSD 的中间层，用于扩展性和负载均衡 |
| **Object Name** | `rbd_data.{image-id}.{object-number}` |

**示例:** 一个 100GB 的 RBD image，object size = 4MB
→ 100 × 1024 / 4 = **25,600 个 RADOS 对象**
→ 这些对象通过 CRUSH 算法分散到集群中所有 OSD 上

### 元数据架构归类

**算法/计算式（Algorithmic）— 无中心元数据服务器**

RBD 自身没有独立的元数据服务。它的元数据（image 头、快照列表、特征标志等）存储在 RADOS 的少量特殊对象中：

- `rbd_id.<image-name>` → 存储 image ID
- `rbd_header.<image-id>` → 存储 image 元数据（大小、特性、快照信息等）
- `rbd_data.<image-id>.{N}` → 实际数据对象

这种设计与 Ceph 整体哲学一致：**用算法代替元数据服务器，用 CRUSH 代替查找表。**

### 数据架构分析

#### 分块方式 (Chunking)
- **固定大小对象:** 默认 4MB（可通过 `rbd_object_size` 调整）
- **对象内偏移:** 块设备 LBA 到对象内偏移是直接计算（`offset = lba % object_size`）
- **条带化 (Striping):** RBD 支持两层条带：
  - **默认条带:** 每个对象独立映射到 OSD（通过 CRUSH 天然分散）
  - **条带化模式 (striping v2):** 允许跨多个 OSD 条带化单个 I/O 请求，参数包括 `stripe_unit`（条带单元大小）、`stripe_count`（条带数）、`stripe_period`（条带周期）

```
默认条带:
  Object 0 ──CRUSH──→ OSD 3
  Object 1 ──CRUSH──→ OSD 7
  Object 2 ──CRUSH──→ OSD 1
  ... (每个对象独立放置)

条带化模式 (stripe_unit=1MB, stripe_count=4):
  I/O 跨越 OSD 3, 7, 1, 5 (一次写操作并行到 4 个 OSD)
```

#### 分布策略 (Distribution)
- **CRUSH 算法:** Ceph 的核心创新，确定性伪随机函数
- **输入:** object name → hash → PG ID → 通过 CRUSH map 计算目标 OSD 列表
- **无中心查找:** 客户端直接计算，无需询问元数据服务器
- **副本放置:** 默认 3 副本，主副本 + 2 从副本分布在不同的 failure domain（host/rack/row）

#### 冗余方式 (Redundancy)
- **副本复制 (Replication):** 默认 3 副本，强一致性
- **纠删码 (Erasure Coding):** pool 级别可配置 EC profile（如 k=3, m=2），但 RBD 对 EC 的支持有限（需要 overlay 或特定配置）
- **校验和:** BlueStore 对数据和元数据使用 CRC32C 校验

#### I/O 路径 (IO Path)

**完整数据路径:**

```
Application / VM
    │
    ▼
┌─────────────────┐
│  Kernel rbd.ko  │  或  QEMU + librbd (userspace)
│  (块设备驱动)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     librbd      │  核心库：镜像映射、对象切分、快照语义
│                 │  - rbd_cache (默认开启, writeback)
│                 │  - I/O 合并与排序
│                 │  - 稀疏读优化 (detect zeros)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    librados     │  RADOS 客户端库
│                 │  - CRUSH 计算 (object → OSD)
│                 │  - 网络通信 (MSGRv2 / RDMA)
│                 │  - 副本协调 (primary OSD 处理)
└────────┬────────┘
         │
         ▼  TCP / RDMA (MSG protocol)
┌─────────────────┐
│      OSD        │  Ceph OSD 守护进程
│                 │  - 接收 I/O 请求
│                 │  - 写入 BlueStore
│                 │  - 副本同步 (replication)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   BlueStore     │  OSD 存储引擎 (取代了 FileStore)
│                 │  - RocksDB (元数据 + WAL)
│                 │  - 裸设备直接写入 (no filesystem overhead)
│                 │  - BlueFS (内部迷你文件系统)
└────────┬────────┘
         │
         ▼
    ┌────────┐
    │ NVMe / │  物理存储
    │ SATA / │
    │ SAS    │
    └────────┘
```

**两种访问模式:**

| 模式 | 组件 | 典型场景 |
|------|------|----------|
| **内核态** | `rbd.ko` 内核模块 → `librbd` | 宿主机直接挂载 RBD 为 `/dev/rbdX` |
| **用户态** | QEMU + `librbd` 直接链接 | OpenStack Nova/KVM 虚拟机磁盘 |

#### 生命周期管理
- **动态创建/删除:** 秒级创建 image
- **在线扩容:** `rbd resize` 支持在线增大（不支持缩小）
- **扁平化克隆:** COW 克隆可 flattend（拷贝所有数据成为独立 image）
- **垃圾回收:** 删除 image 时对象异步清理（通过 `rbd trash` 可恢复）

---

## 3. Core Technical Innovations

### 一致性模型
**强一致性（Strong Consistency）** — 继承自 RADOS

- 每个 RADOS object 有唯一的 primary OSD
- 写操作必须写入 primary + 所有副本（`min_size` 配置）后才返回 ACK
- 读操作默认从 primary 读取，保证读取最新数据
- 客户端看到的行为等价于单机块设备

### 共识协议
- **MON 集群:** 使用 Paxos 变体（Multi-Paxos）维护集群 map
- **OSD 副本:** 写操作由 primary OSD 协调，不依赖独立共识协议
  - RADOS 复制协议: primary OSD 接收写 → 写入本地 → 发送副本 → 等待确认 → 返回客户端
  - PG 状态机管理 OSD 故障恢复和 backfill
- **PG (Placement Group):** RADOS 扩展性的核心抽象
  - 对象 → PG 映射是静态的（通过 hash）
  - PG → OSD 映射通过 CRUSH 动态计算
  - PG 数量固定（通常每 OSD 100 个），避免 OSD 数量增长时的 rehash 成本

### 容错机制

| 机制 | 说明 |
|------|------|
| **副本复制** | 默认 3 副本，允许 2 个 OSD 同时故障不丢数据 |
| **PG 自动恢复** | OSD 故障后，PG 自动 re-peering，从存活副本恢复 |
| **Scrubbing** | 定期数据校验（light scrub / deep scrub），检测静默数据损坏 |
| **Self-Healing** | 检测到数据不一致时自动修复（从好的副本复制） |
| **CRUSH 故障域** | 支持按 host/rack/row 分布副本，容忍不同级别的硬件故障 |

### 高级特性

#### 快照 (Snapshots)
- **轻量级:** 仅记录快照时刻的元数据，不拷贝数据
- **实现原理:** 快照创建时，标记当前对象集合的"冻结版本"
- **写时复制 (COW):** 快照后对原 image 的写操作，先将被修改的对象复制到快照空间
- **数量:** 单个 image 可创建大量快照（实践中受元数据对象大小限制）

```
快照时间线:

  t0: [Obj0] [Obj1] [Obj2] ... [ObjN]   ← 创建快照 snap1
       │      │      │
       ▼      ▼      ▼
      snap1  snap1  snap1   (快照指向当前版本)

  t1: [Obj0'] [Obj1] [Obj2] ... [ObjN]  ← 修改 Obj0
       │       │      │
       ▼       ▼      ▼
      snap1   Obj1   Obj2   (snap1 仍指向原始 Obj0)
       │
       ▼
      Obj0'   (新版本)
```

#### 克隆 (Clones / Copy-on-Write)
- **保护快照:** 快照必须先 `rbd snap protect` 才能克隆
- **COW 克隆:** 克隆 image 与父快照共享未修改的数据块
- **空间效率:** 多个 VM 克隆自同一快照，仅存储各自修改的 delta
- **扁平化:** `rbd flatten` 将所有父数据拷贝到克隆，解除依赖

```
分层镜像 (Layered Images):

        [Base Image v1]
              │
        ┌─────┴─────┐
        │           │
    [snap1]     [snap2]
        │           │
   ┌────┴────┐      │
   │         │      │
[Clone A] [Clone B] [Clone C]
   │
   └─→ [Clone A'] (在 Clone A 基础上修改)

存储占用:
  Base Image: 100GB (完整数据)
  Clone A:    5GB   (仅 delta)
  Clone B:    3GB   (仅 delta)
  Clone C:    8GB   (仅 delta)
  总计: ~116GB (vs 300GB 完全独立)
```

#### 动态扩容 (Resize)
- `rbd resize --size <new-size> <image>`
- 在线操作，无需卸载
- 仅支持增大，不支持缩小（防止数据丢失）
- 新增空间通过追加新对象实现

#### 加密 (LUKS / dm-crypt)
- RBD 本身不提供原生加密，但支持两种加密方案：
  - **LUKS on RBD:** 在 RBD 设备上创建 LUKS 分区，通过 dm-crypt 加密
  - **客户端加密:** 在 librbd 之上加一层加密
- RADOS 层支持传输加密（MSGRv2 + TLS）和静态加密（BlueStore crypt，Ceph 14.2+）

#### QoS (IOPS / Bandwidth Limits)
- Ceph 12.0 (Luminous) 引入 per-image 的 QoS 控制
- 通过 `rbd_qos_iops_limit` 和 `rbd_qos_bps_limit` 设置
- 限制在 librbd 层实现（令牌桶算法）
- 支持 burst 配置（`*_burst` 参数）

```
QoS 配置示例:
  rbd_qos_iops_limit = 1000    # 最大 IOPS
  rbd_qos_iops_burst = 2000    # 突发 IOPS
  rbd_qos_bps_limit = 100MiB   # 最大带宽
  rbd_qos_bps_burst = 200MiB   # 突发带宽
```

#### 特性标志 (Feature Flags)
RBD image 有一组特性标志，控制高级功能：

| 特性 | 说明 |
|------|------|
| `layering` | 快照克隆支持 |
| `exclusive-lock` | 排他锁，保证单客户端写入 |
| `object-map` | 跟踪哪些对象已分配（加速稀疏读） |
| `deep-flatten` | 支持递归展平分层快照 |
| `journaling` | RBD 日志，用于镜像同步 |
| `data-pool` | 指定独立数据 pool（EC pool 场景） |

### 扩展方式
- **横向扩展 (Scale-out):** 增加 OSD 即可线性增加容量和 IOPS
- **客户端并行:** 多个客户端可同时访问不同 RBD image（同一 image 需要 `exclusive-lock` 协调）
- **Pool 隔离:** 不同业务使用不同 pool，实现 I/O 和资源隔离
- **CRUSH 规则:** 通过定制 CRUSH ruleset 控制不同 image 的物理分布

---

## 4. Trade-offs (CAP / PACELC)

### CAP 定理定位

**RBD 继承 RADOS 的 CP 定位:**

| 维度 | 选择 | 说明 |
|------|------|------|
| **Consistency** | ✅ 强一致 | 每个 object 有唯一 primary，写操作同步到所有副本后才返回 |
| **Availability** | ❌ 分区时不可用 | OSD 故障时，相关 PG 需要 re-peering，期间该 PG 上的对象不可读写（或降级为只读） |
| **Partition Tolerance** | ✅ | 分布式系统，容忍网络分区 |

**CAP 结论: CP 系统** — 在网络分区时，牺牲可用性保证一致性。这与块存储的语义要求一致：块设备必须保证数据的强一致性，否则文件系统会被损坏。

### PACELC 定位

| 场景 | 选择 | 说明 |
|------|------|------|
| **Partition (P)** | **C**onsistency | 网络分区时优先保证一致性，部分数据可能暂时不可用 |
| **Else (E-L)** | **C**onsistency | 正常运行时也优先一致性，不牺牲一致性换取延迟 |

PACELC 结论: **CC** — 无论是否有分区，都选择一致性。

### 复杂度 vs 运维简易性

| 优势 | 代价 |
|------|------|
| 无中心架构，无单点故障 | CRUSH map 和 PG 调优需要专业知识 |
| 自动数据均衡 | 初始部署和调优复杂 |
| 标准硬件 | 网络要求高（建议 10GbE+） |
| 丰富的管理工具 (cephadm, ceph-mgr) | 故障排查涉及多层（MON → OSD → PG → CRUSH） |

### 成本 vs 性能

| 优势 | 代价 |
|------|------|
| 使用廉价 x86 + SATA/SAS | 延迟高于高端 SAN 阵列（无专用 ASIC） |
| 线性 scale-out | 客户端 CPU 开销（librados CRUSH 计算） |
| 软件定义 | 需要更多运维人力 |
| 开源免许可费 | 网络带宽成本（3 副本 = 3× 网络流量） |

**典型性能参考:**
- 随机读 IOPS: 取决于 OSD 数量和磁盘类型（NVMe OSD 可达数十万）
- 延迟: ~1-5ms（NVMe + RDMA），~5-20ms（SATA + TCP）
- 吞吐: 受限于网络和磁盘，通常单 image 可达 GB/s 级别

---

## 5. Influence & Legacy

### 启发的后续系统
| 系统 | 关系 |
|------|------|
| **OpenStack Cinder** | RBD 是 Cinder 的默认/首选块存储后端 |
| **OpenStack Nova** | 通过 librbd 直接使用 RBD image 作为虚拟机磁盘 |
| **Kubernetes CSI RBD (ceph-csi)** | K8s 原生对接 RBD 的容器存储接口驱动 |
| **Rook** | K8s 原生编排 Ceph 存储，RBD 是核心存储类 |
| **Proxmox VE** | 原生支持 RBD 作为 VM 存储后端 |
| **Apache CloudStack** | 支持 RBD 作为 primary storage |

### 成为标准的设计模式
1. **软件定义块存储:** RBD 证明了块存储可以完全软件化，无需专用 SAN 硬件
2. **COW 克隆 + 分层镜像:** 成为云原生 VM 模板分发的事实标准
3. **CRUSH 算法式数据分布:** 影响了后续许多分布式存储系统的设计
4. **librados 客户端库模式:** 客户端直接计算数据位置，无中心瓶颈

### 技术谱系位置

```
  ┌─────────────────── 块存储演进 ───────────────────┐
  │                                                   │
  │  FC-SAN (1990s) ──→ iSCSI SAN (2000s)            │
  │        │                    │                      │
  │        ▼                    ▼                      │
  │  集中式阵列          集中式阵列 + IP               │
  │        │                    │                      │
  │        └────────┬───────────┘                      │
  │                 ▼                                  │
  │  ┌──────────────────────────┐                      │
  │  │  Ceph RADOS (2006)       │ ← CRUSH + 强一致     │
  │  │    └── RBD (2010)        │ ← 块设备抽象          │
  │  └──────────┬───────────────┘                      │
  │             │                                      │
  │    ┌────────┼────────┬──────────┐                  │
  │    ▼        ▼        ▼          ▼                  │
  │ OpenStack  K8s CSI  Proxmox    CloudStack         │
  │ Cinder     (Rook)              Primary Storage     │
  │                                                   │
  │  ─── 并行发展 ───                                  │
  │  Amazon EBS (2006)  ──→  云块存储范式              │
  │  NVMe-oF (2016)     ──→  下一代块协议              │
  │  阿里云 ESSD (盘古) ──→  云原生块存储              │
  └───────────────────────────────────────────────────┘
```

### 当前相关性和使用场景

**RBD 在今天依然是:**
1. **OpenStack 部署的事实标准块存储后端** — 大多数 OpenStack 生产部署使用 RBD + Ceph
2. **Kubernetes 持久卷选项** — 通过 ceph-csi RBD driver
3. **私有云虚拟化存储** — Proxmox VE、oVirt 等虚拟化平台原生支持
4. **开发/测试环境** — 快照/克隆特性非常适合快速创建测试环境

**与云厂商块存储的对比:**

| 特性 | Ceph RBD | AWS EBS | 阿里云 ESSD |
|------|----------|---------|-------------|
| 部署模式 | 自建 / 自运维 | 托管服务 | 托管服务 |
| 一致性 | 强一致 | 强一致 | 强一致 |
| 快照 | COW, 即时 | EBS Snapshot (S3) | 快照服务 |
| 克隆 | COW, 分层 | 从快照创建 | 从快照创建 |
| 扩展 | 线性 (加 OSD) | 自动 | 自动 |
| 成本 | 硬件成本 | 按量付费 | 按量付费 |

### 局限与竞争
- **NVMe-oF 的崛起:** 为高性能场景提供了更低延迟的替代方案
- **云托管块的便利:** AWS EBS、阿里云 ESSD 等托管服务降低了运维负担
- **Ceph 运维复杂度:** 尽管 cephadm 简化了部署，但大规模 Ceph 集群的运维仍然需要专业团队
- **RBD 对 EC 的限制:** 纠删码 pool 上运行 RBD 需要额外的 data-pool 配置，增加了复杂度

---

## 参考资料

1. Sage Weil et al., *Ceph: A Scalable, High-Performance Distributed File System*, OSDI 2006
2. Sage Weil et al., *RADOS: A Scalable, Reliable Storage Service for Petabyte-scale Storage Clusters*, 2007
3. Ceph Official Documentation — RBD: https://docs.ceph.com/en/latest/rbd/
4. Ceph BlueStore: https://docs.ceph.com/en/latest/rados/storage-devices/bluestore/
5. CRUSH Algorithm: https://docs.ceph.com/en/latest/rados/operations/crush-map/
6. OpenStack Cinder Ceph RBD Driver: https://docs.openstack.org/cinder/latest/admin/blockstorage-cinder-volume-backend.html
7. ceph-csi RBD: https://github.com/ceph/ceph-csi
8. Ilias Tsitsimpis et al., *Ceph — The Future of Storage*, USENIX ;login:, 2015
9. Red Hat Ceph Storage Documentation: https://access.redhat.com/documentation/en-us/red_hat_ceph_storage/
