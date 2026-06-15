# 3FS — Fire-Flyer File System

## 基本信息

| 项目 | 内容 |
|------|------|
| **全称** | Fire-Flyer File System (3FS) |
| **开源时间** | 2025 年 2 月 |
| **开发方** | DeepSeek (深度求索) |
| **开源协议** | MIT License |
| **代码仓库** | https://github.com/deepseek-ai/3FS |
| **主要语言** | C++ |
| **设计目标** | 面向 AI 训练场景的高性能分布式文件系统，强调吞吐量和低延迟 |

> ⚠️注意：3FS 于 2025 年 2 月首次开源，公开的架构文档和论文仍在积累中。以下分析主要基于开源代码和 DeepSeek 技术报告。

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

AI 大模型训练对存储系统提出了极端的性能要求：

- **训练数据读取**：数千 GPU 同时读取训练数据，需要极高的聚合读带宽
- **Checkpoint 写入**：大模型周期性保存检查点（数十 GB 至 TB 级），需要高并发写带宽
- **低延迟**：GPU 等待数据的每一毫秒都是昂贵的计算资源浪费

### 前代系统的局限

| 系统 | 局限 |
|------|------|
| HDFS | 面向大数据批处理设计，延迟高，不适合 GPU 训练的实时数据供给 |
| Lustre/GPFS | HPC 传统系统，运维复杂，与云原生环境集成困难 |
| JuiceFS | 依赖对象存储作为数据层，增加了额外的网络跳数和延迟 |
| Ceph | 通用性强但 AI 训练场景下吞吐不是最优 |

### 硬件驱动力

- NVMe SSD 提供单盘数 GB/s 的带宽
- RDMA (RoCE v2 / InfiniBand) 网络提供微秒级延迟
- 这些硬件能力需要存储软件从内核态到用户态的全面重新设计才能充分发挥

---

## 2. Architecture（架构设计）

### 核心组件

基于开源代码，3FS 采用三层架构：

```
┌─────────────────────────────────────────────────┐
│                  Client Layer                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │  FUSE    │  │  FUSE    │  │  FUSE    │  ...  │
│  │  Client  │  │  Client  │  │  Client  │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
└───────┼──────────────┼──────────────┼────────────┘
        │              │              │
┌───────▼──────────────▼──────────────▼────────────┐
│              Metadata Service                    │
│  (管理文件命名空间、布局信息)                      │
└───────┬──────────────┬──────────────┬────────────┘
        │              │              │
┌───────▼──────────────▼──────────────▼────────────┐
│              Storage Service                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │  Storage │  │  Storage │  │  Storage │  ...  │
│  │  Node    │  │  Node    │  │  Node    │       │
│  │ (NVMe)   │  │ (NVMe)   │  │ (NVMe)   │       │
│  └──────────┘  └──────────┘  └──────────┘       │
└──────────────────────────────────────────────────┘
```
### 数据复制协议：CRAQ

3FS 的核心技术选型之一是采用 **CRAQ (Chain Replication with Apportioned Queries)** 作为数据复制协议：

- **链式复制**：写入从链头传播到链尾，链尾确认后才视为提交
- **读负载分散**：读请求可以发送到链中任何节点（不只是链头/链尾），提高读吞吐
- **强一致性**：CRAQ 在保证线性一致性的同时提供高读吞吐

```
写入路径 (Chain Replication):
  Client → Head → Middle → ... → Tail → ACK

读取路径 (Apportioned Queries):
  Client → 任何链节点（若为 clean 状态直接返回，否则询问 Tail）
```

### 写入模式：Fire-and-Forget

根据 DeepSeek 技术报告，3FS 针对 Checkpoint 等写入密集场景实现了 "Fire-and-Forget" 优化：
- 客户端写入数据后不阻塞等待全链确认
- 通过异步确认机制减少写入延迟
- 适合 AI 训练中 Checkpoint 的批量写入模式

### 网络层

- **RDMA 优先**：数据路径使用 RDMA 实现零拷贝传输
- **用户态 I/O**：绕过内核，减少上下文切换开销

---

## 3. Core Technical Innovations（核心技术创新）

| 创新点 | 说明 |
|--------|------|
| **CRAQ 链式复制** | 在保证强一致性的同时实现高读吞吐，读可发往任何副本 |
| **Fire-and-Forget 写入** | 异步写入确认，降低 Checkpoint 写入延迟 |
| **RDMA 数据路径** | 用户态网络传输，微秒级延迟 |
| **NVMe 直接访问** | 绕过内核文件系统，直接操作 NVMe 设备 |

---

## 4. Trade-offs

| 权衡 | 3FS 的选择 | 代价 |
|------|-----------|------|
| **一致性 vs 性能** | CRAQ 强一致 + 读分散 | 链式复制写延迟略高于并行写 |
| **通用性 vs 专用性** | 面向 AI 训练场景优化 | 通用文件操作可能非最优 |
| **硬件依赖 vs 通用性** | 要求 RDMA + NVMe | 部署门槛高，非标准网络性能降级 |
| **运维复杂度** | 自建数据面 | 相比 JuiceFS (依赖对象存储) 运维更复杂 |

---

## 5. Influence & Legacy（影响与定位）

### 性能数据

根据 DeepSeek 技术报告：
- 聚合读吞吐：**6.6 TiB/s**（180 个存储节点，540 块 NVMe SSD）
- 聚合写吞吐：**3.66 TiB/s**（同上配置）

> ⚠️以上数据来自 DeepSeek 官方技术报告，测试条件为其内部集群环境。

### 在演进中的位置

3FS 代表了分布式文件系统的 "第四代" 方向：**场景特化 + 新硬件原生支持**。

- 与 JuiceFS 的差异：JuiceFS 依赖对象存储作为数据后端，3FS 自建数据面（CRAQ + NVMe）
- 与 Lustre/GPFS 的差异：3FS 面向 AI 训练而非传统 HPC，架构更轻量
- 与 HDFS 的差异：3FS 面向低延迟高吞吐，HDFS 面向大数据批处理

### 当前状态

- 2025 年 2 月开源，社区仍在早期阶段
- DeepSeek 内部大规模生产使用
- 与 DeepSeek 的 AI 训练基础设施深度绑定

---

## 参考资料

1. DeepSeek, "Fire-Flyer File System (3FS)", GitHub, 2025-02. https://github.com/deepseek-ai/3FS
2. DeepSeek, "DeepSeek-V3 Technical Report", 2024.
3. van Renesse & Schneider, "Chain Replication for Supporting High Throughput and Availability", OSDI 2004.
4. Terrace & Freedman, "Object Storage on CRAQ: High-Throughput Chain Replication for Read-Mostly Workloads", USENIX ATC 2009.

---

*文档版本: 2.0 | 最后更新: 2026-06-15 | 基于开源代码和官方技术报告*

