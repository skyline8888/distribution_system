# 分布式文件系统演进总览

## 演进脉络

```
GPFS (1998) → Lustre (2003) → HDFS (2006) → Ceph (2006) → GlusterFS (2005)
→ Alluxio (2013) → MinIO (2014) → JuiceFS (2018) → 3FS (2025)
```

## 核心驱动力

1. **计算范式变迁**：HPC → 大数据 → 云计算 → AI/ML
2. **硬件演进**：HDD → SSD → NVMe → CXL 内存池化
3. **网络升级**：千兆 → 万兆 → RoCE → InfiniBand → CXL
4. **存储介质**：本地磁盘 → SAN/NAS → 对象存储 → 存算分离

## 技术演进主线

### 元数据管理
- 集中式 MDS (GPFS, Lustre) → 去元数据 (GlusterFS) → 分布式元数据 (Ceph) → 云原生元数据分离 (JuiceFS)

### 数据分布
- 静态划分 (GPFS) → 弹性哈希 (GlusterFS) → CRUSH (Ceph) → Volume 分片 (SeaweedFS)

### 一致性模型
- 强一致 (GPFS, Lustre) → 弱一致/最终一致 (Ceph RGW) → 可配置一致性

### 架构模式
- 共享存储 → 分布式本地存储 → 存算分离 → Serverless 存储

## 时代划分

### 第一代：高性能并行文件系统 (1998-2008)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
| GPFS | 1998 | 并行 I/O、分布式锁管理、metadata/data 分离 |
| Lustre | 2003 | MDS/OSS 分层、striping、大规模并发 |
| PVFS | 2000 | 轻量级并行 I/O、MPI 集成 |

**特点**：面向 HPC，追求极致吞吐，架构偏集中化

### 第二代：开源分布式文件系统 (2005-2015)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
| GlusterFS | 2005 (RedHat 2011 收购) | 无中心架构、弹性哈希卷、translator 框架 |
| HDFS | 2006 | 大文件顺序读写、NameNode/DataNode、rack awareness |
| Ceph | 2006 (论文) / ~2012 (生产就绪) | RADOS、CRUSH、统一存储 (文件/块/对象) |

**特点**：拥抱开源，面向大数据和云，去中心化趋势

### 第三代：云原生轻量存储 (2013-2022)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
|  Alluxio/Tachyon | 2013 | 虚拟分布式存储层、计算缓存、多存储后端 |
| MinIO | 2014 | 高性能 S3 兼容、纠删码、Erasure Coding |
| SeaweedFS | 2014 | 扁平目录结构、O(1) 定位、Volume Server |
| JuiceFS | 2018 (开源 2021) | 元数据引擎可插拔 (Redis/MySQL/TiKV) + 对象存储数据层 |

**特点**：存算分离、S3 兼容、云原生、轻量部署

### 第四代：新一代高性能文件系统 (2021+)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
| Curve | 2021 | 网易开源、分布式块存储、Raft 共识 |
| 3FS | 2025 (DeepSeek 开源) | AI 训练优化、RDMA + NVMe、Fire-and-Forget 写入 |

**特点**：AI 训练场景驱动，极致性能，新硬件适配

## 关键技术对比

| 维度 | 第一代 (GPFS) | 第二代 (Ceph) | 第三代 (JuiceFS) | 第四代 (3FS) |
|------|---------------|---------------|-------------------|--------------|
| 架构 | 中心化 MDS | 完全去中心化 | 元数据分离 | 轻量分布式 |
| 数据分布 | 静态 striping | CRUSH 算法 | 对象存储 | CRAQ 链式复制 |
| 一致性 | 强一致 | 可配置 | 最终/可配置 | 强一致 |
| 典型吞吐 | GB/s | 10GB/s+ | 依赖后端 | 6.6 TiB/s (官方测试) |
| 典型延迟 | ms | ms | 10ms+ | μs-ms |
| 部署复杂度 | 高 | 高 | 低 | 中 |
| 主要场景 | HPC | 通用云存储 | 云原生/AI 数据湖 | AI 训练/推理 |

## 参考资料

- Schmuck & Haskin, "GPFS: A Shared-Disk File System for Large Computing Clusters", FAST 2002.
- Schwan, "Lustre: Building a File System for 1000-node Clusters", Linux Symposium 2003.
- Weil et al., "Ceph: A Scalable, High-Performance Distributed File System", OSDI 2006.
- Shvachko et al., "The Hadoop Distributed File System", IEEE MSST 2010.
- JuiceFS 官方文档, https://juicefs.com/docs/
- DeepSeek, "3FS (Fire-Flyer File System)", GitHub, 2025-02. https://github.com/deepseek-ai/3FS
