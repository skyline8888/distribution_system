# Ceph

## 基本信息
- 发布时间：2006 年（Sage Weil 博士论文）
- 开发方：Sage Weil / Inktank / Red Hat
- 开源/商用：开源（LGPL）
- 核心论文：*Ceph: A Scalable Distributed Object Store* (2006), *RADOS: A Scalable Reliable Storage Service* (2007)
- 当前版本：18.x (Reef)

## 架构设计

### 核心架构
```
┌──────────────────────────────────────────────────────┐
│                    Ceph Cluster                       │
├──────────────────────────────────────────────────────┤
│  MON (Monitor) ──── Cluster Map & Metadata            │
│     ├── OSD Map                                         │
│     ├── PG Map                                          │
│     └── CRUSH Map                                       │
│                                                        │
│  MDS (Metadata Server) ──── CephFS Only               │
│     ├── Metadata Cache                                  │
│     └── Dynamic Subtree Partitioning                    │
│                                                        │
│  OSD (Object Storage Daemon) ──── Data Storage         │
│     ├── PG (Placement Group)                            │
│     ├── BlueStore / FileStore                           │
│     └── Replication / Erasure Coding                    │
│                                                        │
│  Client (librados / FUSE / Kernel)                     │
└──────────────────────────────────────────────────────┘
```

### 节点角色
- **MON (Monitor)**: 维护集群状态 map，Paxos 共识，至少 3 个节点
- **MDS (Metadata Server)**: 仅 CephFS 需要，管理文件元数据，支持动态子树分区
- **OSD (Object Storage Daemon)**: 实际存储数据，每个 OSD 通常对应一块磁盘
- **Client**: 通过 librados 直接访问 OSD，无需代理

## 核心技术

### 1. RADOS (Reliable Autonomic Distributed Object Store)
- Ceph 的底层，是一个**自管理、自修复**的对象存储系统
- 客户端直接与 OSD 通信，无需经过中央协调
- OSD 之间协作完成数据复制和恢复

### 2. CRUSH 算法 (Controlled Replication Under Scalable Hashing)
- **核心创新**：客户端可**独立计算**数据位置，无需查表
- 输入：对象名 → hash → 映射到 PG → 映射到 OSD 列表
- 感知拓扑结构（rack、row、datacenter）
- 扩容时只有少量数据需要迁移

```
Object ID ──hash──► PG ID ──CRUSH──► [OSD.1, OSD.2, OSD.3]
```

### 3. Placement Groups (PG)
- 对象先映射到 PG，PG 再映射到 OSD
- PG 是数据迁移和恢复的**基本单位**
- 减少元数据量：N 个对象 → M 个 PG (M << N)
- 典型配置：每 OSD 100-200 个 PG

### 4. BlueStore 存储引擎
- 直接管理裸设备，绕过文件系统
- 数据直接写入磁盘，元数据使用 RocksDB
- 支持压缩、校验和、EC（纠删码）
- 比旧版 FileStore 性能提升显著

## 元数据管理

- **CephFS**: MDS 管理文件元数据
  - 动态子树分区：热目录自动迁移到不同 MDS
  - 元数据 journal 保证一致性
  - 多 MDS 主-备/主-主模式
- **RBD (块存储)**: 元数据分布在 PG 中
- **RGW (对象存储)**: 元数据存储在 RADOS 对象中

## 一致性模型

- **RADOS**: 强一致性（主从复制）
- **CephFS**: POSIX 语义，强一致
- **RGW**: 最终一致性（多站点场景）
- 共识：MON 使用 Paxos

## 容错与高可用

- **数据复制**：默认 3 副本，可配置
- **纠删码 (Erasure Coding)**: 降低存储开销
- **自愈**：OSD 故障自动检测、数据自动重建
- **MON 高可用**: Paxos，多数派存活即可
- **MDS 高可用**: 主-备，故障自动切换
- **CRUSH 感知故障域**: 可在 rack/row 级别做隔离

## 性能特征

| 指标 | 典型值 |
|------|--------|
| 单集群规模 | EB 级 |
| 最大 OSD 数 | 数千 |
| 吞吐量 | 10GB/s+ 聚合 |
| 延迟 | ms 级 |
| 小 IOPS | 万级（BlueStore + NVMe） |
| 存储开销 | 3x（副本）或 1.33x（EC 4+2） |

## 适用场景

- **最佳**：统一存储平台（文件/块/对象）、OpenStack 后端、Kubernetes PV
- **优势**：统一架构、自管理、无限扩展、开源生态
- **局限**：运维复杂度高、小文件性能一般、MON/Paxos 是元数据瓶颈

## 历史影响

1. **CRUSH 算法**：开创了无中心化数据分布的先河，被多个系统借鉴
2. **统一存储**：首个同时提供文件/块/对象的开源系统
3. **PG 抽象**：减少元数据量的设计影响深远
4. **云原生存储**：Kubernetes CSI 后端的主流选择
5. **BlueStore**：裸设备管理思路影响后续存储引擎设计

## 局限与教训

- CRUSH 计算虽然去中心化，但扩容时 PG 迁移量大
- MON 使用 Paxos，大规模下成为瓶颈
- 运维复杂度极高，调优参数众多
- 小文件场景性能不佳（与 GPFS 类似）
- 多租户隔离能力有限
