# OpenEBS

## 基本信息

| 维度 | 内容 |
|------|------|
| **初始发布** | 2017 年 |
| **创始公司** | MayaData（前身为 CloudByte，2019 年被 DataCore Software 收购） |
| **开源协议** | Apache 2.0 |
| **定位** | Kubernetes-native 容器附加存储（Container-Attached Storage, CAS） |
| **核心仓库** | github.com/openebs/openebs |
| **存储引擎** | cStor、Jiva、LocalPV、Mayastor（NVMe-oF + SPDK） |
| **CNCF 状态** | CNCF Sandbox 项目（2020） |

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

2017 年 Kubernetes 已在生产环境中大规模采用，但**有状态工作负载的持久化存储**仍是痛点：

- **传统 SAN/NAS 不适配 K8s 动态调度**：Pod 漂移后存储无法跟随，手动配置 LUN/iSCSI target 与 K8s 声明式模型格格不入。
- **云厂商块存储锁定**：AWS EBS、GCE PD 仅在其云平台可用，跨云/混合云场景无法统一。
- **Ceph 运维复杂度高**：Ceph 作为独立分布式存储集群需要专职运维团队，对中小型 K8s 集群过重。
- **本地存储无容灾**：直接使用 hostPath 或 Local PV 无副本，节点故障即数据丢失。

### 前代系统局限性

| 前代方案 | 局限性 |
|----------|--------|
| 传统 SAN（FC/iSCSI 阵列） | 硬件绑定、运维重、不支持 K8s 动态供给 |
| 云块存储（EBS/GCE PD） | 云锁定、跨 AZ 不可用 |
| Ceph RBD | 独立集群运维复杂、学习曲线陡峭 |
| hostPath / Local PV | 无副本、无快照、无迁移能力 |

### 硬件与网络背景

- 2017 年前后 NVMe SSD 开始普及，单机 IOPS 从万级跃升至百万级。
- 容器化浪潮推动存储从"外部集中式"转向"本地分布式"。
- CSI（Container Storage Interface）标准于 2018 年正式发布，为 K8s 存储插件提供统一接口。

### 核心理念：Container-Attached Storage (CAS)

OpenEBS 提出了 **CAS** 范式——与 Container-Attached Computing 对称：

> **每个持久化卷（PV）都运行自己的存储控制器和副本 Pod，与应用 Pod 共生于同一 K8s 集群。**

这意味着：
- 存储管理完全通过 K8s API（Deployment/StatefulSet/StorageClass）完成。
- 无需外部存储集群，存储即 K8s 工作负载。
- 存储策略（副本数、快照、QoS）通过 K8s CRD 声明式定义。

---

## 2. Architecture（架构设计）

### 系统拓扑

OpenEBS 采用**微服务化、引擎可插拔**的架构：

```
+====================================================================+
|                     Kubernetes Cluster                              |
|                                                                    |
|  +----------------------------------------------------------------+ |
|  |                   Control Plane                                 | |
|  |                                                                 | |
|  |  +-------------------+    +------------------+                  | |
|  |  | OpenEBS Operator  |    | NDM (Node Disk   |                  | |
|  |  | (Maya-apiserver / |    |  Manager Daemon) |                  | |
|  |  |  openebs-provisioner) |  |                 |                  | |
|  |  +--------+----------+    +--------+---------+                  | |
|  |           |                        |                            | |
|  |           | K8s API (CRD)          | BlockDevice CR             | |
|  |           v                        v                            | |
|  |  +---------------------------------------------------------+    | |
|  |  |              StorageClass / Policy Engine                 |    | |
|  +--+---------------------------------------------------------+----+ |
|     |                                                           |
|     | PVC 创建请求                                               |
|     v                                                           |
+-----+-----------------------------------------------------------+
      |
      |  根据 StorageClass 选择引擎
      |
+-----v-----------------------------------------------------------+
|                     Data Plane (per-Volume Pods)                  |
|                                                                  |
|  Volume: pv-abc (cStor Engine)                                   |
|  +---------------------------+   +----------------------------+  |
|  | cStor Controller Pod    |   | cStor Replica Pods (x3)     |  |
|  |                           |   |                            |  |
|  | +-----------------------+ |   | +------------------------+ |  |
|  | | iSCSI Target (tgt)    | |   | | ZFS Pool (disk1)       | |  |
|  | |   :3260               | |   | | ZFS Pool (disk2)       | |  |
|  | +-----------------------+ |   | +------------------------+ |  |
|  |                           |   |                            |  |
|  | <---- synchronous sync -->|   |                            |  |
|  +---------------------------+   +----------------------------+  |
|                                                                  |
|  Volume: pv-def (Mayastor Engine)                                |
|  +----------------------------------------------------------+    |
|  | Mayastor Controller (NVMe-oF) + Mayastor Replica Pods    |    |
|  | SPDK-based, user-space NVMe-oF target                    |    |
|  +----------------------------------------------------------+    |
|                                                                  |
|  Volume: pv-ghi (LocalPV Engine)                                 |
|  +----------------------------------------------------------+    |
|  | Direct host disk / hostpath → Node-local, no replication |    |
|  +----------------------------------------------------------+    |
+------------------------------------------------------------------+
```

### 组件详解

#### 控制面

| 组件 | 职责 |
|------|------|
| **NDM (Node Disk Manager)** | DaemonSet 部署，自动发现裸盘，创建 `BlockDevice` CR，处理磁盘热插拔 |
| **OpenEBS Provisioner** | 监听 PVC 事件，根据 StorageClass 策略创建对应引擎的 Controller + Replica Pods |
| **CSI Driver** | 实现 CSI 接口（NodePublish/ControllerPublish/VolumeAttach），暴露卷到容器 |
| **StorageClass / Policy CRD** | 声明式定义引擎类型、副本数、快照策略、调度亲和性 |

#### 数据面 — 多引擎架构

OpenEBS 核心设计是**存储引擎可插拔**，不同引擎服务于不同场景：

| 引擎 | 底层技术 | 适用场景 | 复制方式 |
|------|----------|----------|----------|
| **cStor** | ZFS on Linux (ZoL) | 通用持久化存储，需快照/克隆/复制 | 同步复制 |
| **Jiva** | iSCSI target + DRBD-like 复制 | 轻量级通用存储 | 同步复制（默认 3 副本） |
| **LocalPV** | 本地磁盘/目录 | 高 IOPS 低延迟、自带复制的上层应用 | 无（单节点） |
| **Mayastor** | NVMe-oF + SPDK | 高性能/低延迟 NVMe 级块存储 | gRPC 同步复制 |

### cStor 引擎架构

```
                    Application Pod
                          |
                    PVC: storageClass=cstor
                          |
                   iSCSI Target (:3260)
                          |
            +-------------+-------------+
            |    cStor Controller Pod    |
            |  (istgt - iSCSI target)    |
            |                            |
            |  Volume IQN:               |
            |  iqn.2016-09.io.openebs... |
            +-------------+--------------+
                          |
            +-------------+-------------+
            |  Sync Replication Protocol |
            +-------------+-------------+
             /              |              \
            /               |               \
   +--------+----+   +------+-----+   +------+------+
   | cStor-Rep-1 |   | cStor-Rep-2 |   | cStor-Rep-3 |
   |             |   |             |   |             |
   | ZFS Pool    |   | ZFS Pool    |   | ZFS Pool    |
   | /dev/sda    |   | /dev/sdb    |   | /dev/sdc    |
   +-------------+   +-------------+   +-------------+
```

- **ZFS Pool**: 将多个物理磁盘聚合成一个 zpool，支持 RAID-Z/镜像/vdev。
- **快照/克隆**: 利用 ZFS 原生快照能力，秒级创建，零拷贝克隆。
- **增量复制**: `zfs send/recv` 实现跨集群异步复制（灾备场景）。
- **调度**: Replicas 通过 `nodeSelector` 和 `anti-affinity` 分布到不同节点。

### Jiva 引擎架构

```
              Application Pod
                    |
              iSCSI Target (:3260)
                    |
          +---------+---------+
          |   Jiva Controller  |
          | (iscsi-targetd)    |
          |                    |
          | +----------------+ |
          | | Backend Store  | |
          | | (ext4 on file) | |
          | +-------+--------+ |
          +---------+----------+
                    |
         +----------+-----------+----------+
         |          |           |          |
   +-----v---+ +----v----+ +----v----+
   | Jiva    | | Jiva    | | Jiva    |
   | Replica | | Replica | | Replica |
   | (TCMU)  | | (TCMU)  | | (TCMU)  |
   +---------+ +---------+ +---------+
```

- **iSCSI Target**: 控制器暴露标准 iSCSI LUN，应用通过内核 iscsi 挂载。
- **同步复制**: 写操作需所有副本确认后才返回（默认 3 副本，可配置）。
- **TCMU (Target Core Memory Userspace)**: 副本通过 Linux TCMU 框架在用户态处理 I/O。
- **适用场景**: 轻量级、易部署，适合中小集群。

### LocalPV 引擎

```
     Application Pod
           |
     Direct Mount
           |
    +------+------+
    |  hostPath    |
    |  or raw disk |
    |  /dev/sdX    |
    +-------------+
    (Node-local, no replication)
```

- **零额外开销**: 直接暴露节点本地磁盘或目录，无控制器/副本 Pod。
- **调度绑定**: 通过 `nodeAffinity` 将 Pod 固定到特定节点。
- **适用场景**: 自带副本机制的上层应用（Cassandra、Elasticsearch、etcd）、极致性能要求。
- **两种模式**: `hostpath`（目录）和 `device`（裸盘）。

### Mayastor 引擎（下一代高性能）

```
              Application Pod
                    |
            NVMe-oF (TCP/RDMA)
                    |
          +---------+---------+
          |   Mayastor Core   |
          |  (SPDK NVMe-oF    |
          |   target, user-   |
          |   space)          |
          |                   |
          |  +-------------+  |
          |  | SPDK bdev   |  |
          |  | (NVMe/      |  |
          |  |  malloc/    |  |
          |  |  aio)       |  |
          |  +------+------+  |
          +---------+---------+
                    |
              +-----+-----+
              | gRPC sync  |
              | replication|
              +-----+-----+
             /      |      \
   +--------+ +-----+ +-----+
   | Replica | | Replica | Replica |
   | (SPDK)  | | (SPDK)  | (SPDK)  |
   +---------+ +---------+---------+
```

- **SPDK (Storage Performance Development Kit)**: 用户态 NVMe 驱动，绕过内核 I/O 栈，实现 100 万+ IOPS。
- **NVMe-oF**: 原生 NVMe over Fabrics 协议（支持 TCP 和 RDMA 传输），保持 NVMe 的多队列语义。
- **gRPC 复制**: 控制面和复制协议基于 gRPC/Protobuf，支持自动故障切换。
- **ETCD 元数据**: 卷元数据和复制状态存储在 ETCD 中。
- **与 cStor/Jiva 的关键差异**: 内核旁路 + NVMe 原生协议，性能差距 5-10x。

### NDM (Node Disk Manager)

```
     Each K8s Node
     +-------------------------------+
     |  NDM DaemonSet Pod             |
     |                                |
     |  Scans:                        |
     |  - /sys/block                  |
     |  - udev events                 |
     |  - SMART data                  |
     |                                |
     |  Filters out:                  |
     |  - OS disk (sda)               |
     |  - Mount points                |
     |  - Loop/LVM/DM devices         |
     |                                |
     |  Creates BlockDevice CR for    |
     |  each eligible disk            |
     +-------------------------------+
            |
            | BlockDevice CR
            v
     +-------------------------------+
     |  BlockDevice CRD               |
     |  - spec.path: /dev/sdb         |
     |  - spec.capacity               |
     |  - spec.details.model/vendor   |
     |  - status.state: Active        |
     |  - nodeName: node-1            |
     +-------------------------------+
```

### 元数据架构归类

**归类：算法/计算式 + 外部引擎（混合模式）**

OpenEBS 的元数据管理因引擎而异：

| 引擎 | 元数据管理方式 |
|------|---------------|
| cStor | ZFS 原生元数据（zpool label + uberblock），元数据嵌入磁盘 |
| Jiva | 控制器本地文件 + iSCSI target 配置 |
| LocalPV | 无分布式元数据，节点本地路径即元数据 |
| Mayastor | ETCD 集中式存储（卷配置、复制拓扑、健康状态） |

### 数据架构分析

| 维度 | cStor | Jiva | LocalPV | Mayastor |
|------|-------|------|---------|----------|
| **数据分块** | ZFS block-level | 文件后端 | 裸盘/目录 | SPDK NVMe namespace |
| **数据分布** | 同步多副本 | 同步多副本 | 单节点 | 同步多副本 (gRPC) |
| **冗余方式** | ZFS RAID-Z/mirror | 3 副本同步 | 无 | 3 副本同步 |
| **I/O 路径** | 内核态 (ZFS) | 内核态 (iSCSI + TCMU) | 内核直访 | 用户态 (SPDK) |
| **协议** | iSCSI | iSCSI | 直接挂载 | NVMe-oF (TCP/RDMA) |
| **生命周期** | ZFS snapshot/clone/send-recv | 快照（CoW） | 无内置 | gRPC 快照 |

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 Container-Attached Storage (CAS) 范式

这是 OpenEBS **最核心的创新**——将存储从"外部基础设施"转变为"K8s 一等公民"：

| 传统存储 | CAS (OpenEBS) |
|----------|---------------|
| 独立存储集群 | 存储即 K8s Pod |
| 手动配置 LUN | 声明式 StorageClass |
| 专有管理工具 | kubectl + CRD |
| 运维团队专属 | K8s 运维团队可管理 |
| 扩容需采购/配置 | 加节点即扩容 |

**CAS 的关键设计原则**：

1. **每卷一组 Pod**：每个 PV 有自己的 Controller + Replica Pod 集合，生命周期与 PVC 绑定。
2. **K8s 原生调度**：利用 Pod anti-affinity 确保副本分布在不同节点。
3. **声明式策略**：通过 StorageClass 参数控制副本数、引擎类型、快照策略。
4. **热迁移**：Controller Pod 可漂移（配合共享存储后端），副本重建自动触发。

### 3.2 多引擎可插拔架构

OpenEBS 不绑定单一存储后端，而是提供**引擎抽象层**：

```
         +-------------------+
         |   CSI Driver      |
         | (统一接口层)        |
         +--------+----------+
                  |
         +--------v----------+
         |  Engine Abstraction|
         | (存储引擎选择层)    |
         +---+---+---+---+---+
             |   |   |   |
        +----+ +--+ +--+ +------+
        |cStor| |Jiva| |LP| |Mayastor|
        +----+ +--+ +--+ +------+
```

这种设计使 OpenEBS 能同时服务：
- 需要企业级特性的场景（cStor 快照/克隆）
- 极致性能场景（Mayastor NVMe-oF）
- 自带复制的上层应用（LocalPV）

### 3.3 ZFS 原生集成（cStor）

- **写时复制 (CoW)**: ZFS 天然防止写撕裂，断电安全。
- **端到端校验和**: 每个数据块有独立校验和，静默损坏可检测。
- **增量复制**: `zfs send -i` 实现高效跨集群灾备。
- **稀疏卷**: thin provisioning，实际使用量按需分配。

### 3.4 NDM 自动磁盘发现

- **热插拔感知**: 监听 udev 事件，新盘上线自动创建 BlockDevice CR。
- **过滤策略**: 自动排除系统盘、已挂载盘、LVM/DM 设备，防止误用。
- **SMART 集成**: 收集磁盘健康信息，支持预测性维护。

### 3.5 Mayastor: SPDK + NVMe-oF 突破内核瓶颈

传统 iSCSI（cStor/Jiva）的 I/O 路径：

```
App → VFS → Block Layer → SCSI Mid Layer → iSCSI TCP → NIC Driver
                              ↑ 多次内核态↔用户态切换
```

Mayastor 的路径：

```
App → NVMe-oF TCP → SPDK (polling, user-space) → NIC DPDK
      ↑ 零拷贝，内核旁路，轮询模式
```

性能差异（典型基准）：
- cStor/Jiva (iSCSI): ~50K-200K IOPS
- Mayastor (NVMe-oF + SPDK): ~500K-1M+ IOPS
- 延迟: cStor ~1-5ms, Mayastor ~100-500μs

### 3.6 CSI Driver 集成

OpenEBS 通过 CSI 插件暴露卷到 K8s 生态：

| CSI 接口 | 实现 |
|----------|------|
| ControllerPublish | 创建/删除卷，配置复制拓扑 |
| NodePublish | 挂载 iSCSI/NVMe-oF target 到 Pod |
| NodeStage | 格式化 + 首次挂载 |
| CreateSnapshot | 引擎特定的快照操作 |
| ControllerExpand | 在线扩容卷 |

StorageClass 示例：

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: openebs-cstor
provisioner: cstor.csi.openebs.io
parameters:
  cas-type: cstor
  cstorPoolCluster: cstor-disk-pool
  replicaCount: "3"
allowVolumeExpansion: true
```

---

## 4. Trade-offs (CAP / PACELC)

### CAP 定位 — 因引擎而异

| 引擎 | CAP 选择 | 说明 |
|------|----------|------|
| **cStor** | **CP** | 同步复制确保强一致；副本不足时拒绝写入（Consistency > Availability） |
| **Jiva** | **CP** | 同步复制，需多数副本确认后才返回；网络分区时不可写 |
| **Mayastor** | **CP** | gRPC 同步复制，强一致；ETCD 存储元数据 |
| **LocalPV** | **不适用** | 单节点存储，无分布式复制，CAP 不直接适用 |

### PACELC 定位

| 引擎 | 分区时 (P) | 正常时 (E/L) | 说明 |
|------|-----------|-------------|------|
| **cStor** | **C** | **L** | 分区时拒绝写入保一致；正常时优先低延迟 |
| **Jiva** | **C** | **L** | 同上 |
| **Mayastor** | **C** | **L** | 同上 |
| **LocalPV** | — | **L** | 无分区场景；正常时极致低延迟 |

### 关键权衡

#### 复杂度 vs 运维简单性

| 维度 | 分析 |
|------|------|
| **优势** | 存储管理融入 K8s 运维流，无需专职存储团队；声明式配置减少人为错误 |
| **代价** | 每个卷引入额外 Pod（控制器 + 副本），大规模部署时 Pod 数量膨胀；多引擎选择增加决策复杂度 |

#### 成本 vs 性能

| 引擎 | 成本 | 性能 | 适用规模 |
|------|------|------|----------|
| **LocalPV** | 最低（零开销） | 最高（裸盘） | 大规模，自带副本的上层应用 |
| **cStor** | 中等（ZFS 内存开销 ~1GB/1TB） | 中等 | 中小集群，需要企业级特性 |
| **Jiva** | 中等 | 中等偏低 | 轻量级/测试环境 |
| **Mayastor** | 高（需 NVMe SSD + 大页内存） | 最高（NVMe 级） | 高性能需求，I/O 密集应用 |

#### 复制 vs 性能

- 同步复制确保强一致但增加写延迟（需等待所有副本确认）。
- cStor 的 ZFS ARC 缓存可部分缓解读延迟。
- LocalPV 放弃复制换取极致性能——适合 Cassandra/Elasticsearch 等自带副本的应用。

#### 内核态 vs 用户态

- cStor/Jiva 走内核 I/O 栈，兼容性好但性能受限于内核上下文切换。
- Mayastor (SPDK) 绕过内核，性能高但需要大页内存配置、DPDK 网卡驱动，部署门槛更高。

---

## 5. Influence & Legacy（影响与遗产）

### 5.1 启发的后续系统

| 系统 | 受 OpenEBS 的影响 |
|------|------------------|
| **Longhorn** | CAS 概念的直接竞争者，同样采用 per-volume controller + replica 模型，但使用自己的轻量级复制引擎而非 ZFS |
| **Rook-Ceph** | 虽然基于 Ceph，但借鉴了 OpenEBS 的 K8s Operator 模式，将 Ceph 作为 K8s 工作负载管理 |
| **Mayastor → OpenEBS 3.0** | Mayastor 的 NVMe-oF + SPDK 路线影响了整个 K8s 存储生态向 NVMe 原生协议迁移 |

### 5.2 成为标准的设计模式

| 模式 | 影响 |
|------|------|
| **CAS 范式** | 成为 K8s 有状态工作负载存储的事实标准思路，每卷一组 Pod 的模式被广泛采用 |
| **多引擎可插拔** | StorageClass 选择引擎的模式成为 CSI 生态的标准做法 |
| **NDM 磁盘发现** | 自动裸盘发现 + CRD 抽象的模式影响了其他 K8s 存储项目 |
| **CSI + CRD 组合** | 声明式存储策略通过 CRD 定义，CSI 实现数据面，成为行业标准 |

### 5.3 技术谱系位置

```
                    ZFS (Sun, 2001)
                        |
                        v
                  ZFS on Linux (ZoL)
                        |
                        v
    +------------------------------------------+
    |           OpenEBS (2017)                  |
    |    (MayaData/CloudByte → DataCore)        |
    |                                            |
    |    CAS 概念首创                            |
    |    多引擎: cStor/Jiva/LocalPV/Mayastor    |
    +------+--------------------+---------------+
           |                    |
           | CAS 范式            | NVMe-oF 方向
           v                    v
     +----------+         +----------+
     | Longhorn |         | Mayastor |
     | (2019)   |         | 成为     |
     | Rancher  |         | OpenEBS  |
     |          |         | 高性能   |
     | 轻量级    |         | 引擎     |
     | CAS      |         +----------+
     +----------+
```

### 5.4 当前相关性与使用场景

**活跃使用场景**：
- 边缘 K8s 集群（k3s/k0s + LocalPV）
- 开发/测试环境（Jiva 快速部署）
- 需要快照/克隆的中小生产集群（cStor）
- 高性能数据库/缓存（Mayastor NVMe-oF）

**竞争格局**：

| 方案 | 优势 | 劣势 |
|------|------|------|
| **OpenEBS** | 多引擎选择、CAS 先驱、CNCF 项目 | Pod 膨胀、ZFS 内存开销 |
| **Longhorn** | 更轻量、Rancher 生态集成好 | 引擎单一、性能不及 Mayastor |
| **Rook-Ceph** | 功能最完整（块/文件/对象） | 运维复杂度最高 |
| **云厂商 CSI** | 完全托管 | 云锁定 |

**OpenEBS 3.0+ 方向**：
- Mayastor 成为默认推荐引擎（替代 cStor/Jiva 的长期地位）。
- 简化部署模型，减少每卷 Pod 数量。
- 向 NVMe-oF + SPDK 全面靠拢。

---

## 参考资料

- OpenEBS 官方文档: https://openebs.io/docs
- OpenEBS GitHub: https://github.com/openebs/openebs
- CNCF OpenEBS Landscape: https://landscape.cncf.io/?item=storage--container-attached-storage--openebs
- Mayastor Architecture: https://mayastor.gitbook.io/introduction/
- SPDK: https://spdk.io/
- CAS Whitepaper (Kubernetes SIG Storage): https://github.com/openebs/openebs/blob/master/documentation/cas.md
- DataCore 收购 MayaData (2019): https://www.datacore.com/
