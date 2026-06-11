# HDFS (Hadoop Distributed File System)

## 基本信息
- 发布时间：2006 年（Apache Hadoop 子项目）
- 开发方：Doug Cutting / Yahoo! / Apache
- 开源/商用：开源（Apache 2.0）
- 核心论文：受 Google GFS 论文启发（*The Google File System*, 2003）
- 当前版本：Hadoop 3.x

## 架构设计

### 核心架构
```
┌─────────────────────────────────────────────┐
│                 HDFS Cluster                 │
├─────────────────────────────────────────────┤
│                                              │
│  NameNode (Active/Standby)                  │
│  ┌──────────────────────────────────┐       │
│  │  FsImage + EditLog               │       │
│  │  Namespace: files, dirs, blocks  │       │
│  │  Block → DataNode mapping        │       │
│  └──────────────────────────────────┘       │
│         ↑              ↑                     │
│         │  Heartbeat   │  Block Report       │
│         ↓              ↓                     │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐    │
│  │DataNode  │ │DataNode  │ │DataNode  │    │
│  │Blocks    │ │Blocks    │ │Blocks    │    │
│  └──────────┘ └──────────┘ └──────────┘    │
│                                              │
│  Secondary NameNode (Checkpoint only)        │
│  JournalNode (HA mode, Quorum Journal)       │
│  ZKFC (ZooKeeper Failover Controller)        │
└─────────────────────────────────────────────┘
```

### 节点角色
- **NameNode**: 管理命名空间、元数据、block-to-datanode 映射。单点瓶颈（Hadoop 2.x 引入 HA）
- **DataNode**: 实际存储数据块，定期向 NameNode 发送心跳和块报告
- **Secondary NameNode**: 定期合并 FsImage 和 EditLog（不是热备！）
- **JournalNode**: HA 模式下共享 EditLog，使用 Quorum 机制
- **ZKFC**: 基于 ZooKeeper 的自动故障切换

## 核心技术

### 1. 大文件设计
- 面向**大文件**顺序读写（默认 block 128MB，Hadoop 3.x 支持纠删码后更大）
- 不适合小文件场景（每个文件/目录/block 在 NameNode 占 ~150 字节内存）
- 文件只追加（append），不支持随机写

### 2. 副本策略
- 默认 3 副本：
  - 第 1 副本：本地节点
  - 第 2 副本：不同 rack
  - 第 3 副本：同第 2 副本的 rack
- Rack awareness：副本分散在不同机架
- 支持自定义副本策略

### 3. 数据流
- 客户端写：NameNode 分配 DataNode 列表 → 客户端建立 pipeline → 顺序写入
- Pipeline 写入：数据沿 DataNode 链传递，每个节点本地写入后传给下一节点

### 4. HDFS Federation (Hadoop 2.x)
- 多个 NameNode，每个管理一部分命名空间
- 共享底层 DataNode 存储
- 缓解单 NameNode 内存瓶颈，但不解决单 namespace 吞吐

## 元数据管理

- **集中式**：所有元数据在 NameNode 内存中
- **FsImage**: 文件系统快照（序列化到磁盘）
- **EditLog**: 操作日志（追加写入）
- **Checkpoint**: Secondary NN 定期合并 FsImage + EditLog
- **HA 模式**: JournalNode 共享 EditLog，ZKFC 管理切换

## 一致性模型

- 文件创建/删除强一致（通过 NameNode）
- 数据写入：flush 后才对读者可见
- 不支持文件随机修改（append-only）

## 容错与高可用

- **DataNode 故障**: NameNode 检测心跳丢失 → 复制丢失的 block
- **NameNode HA**: Active/Standby + JournalNode + ZKFC
- **机架容错**: 副本分布在多个机架
- **NameNode 内存限制**: ~数十亿文件（受 JVM 堆限制）

## 性能特征

| 指标 | 典型值 |
|------|--------|
| 单集群规模 | PB 级 |
| 最大 DataNode 数 | 数千 |
| 吞吐量 | 高（大文件顺序读写） |
| 随机读延迟 | 高（不适合） |
| 小文件性能 | 差 |
| 最大文件数 | ~数十亿（NameNode 内存限制） |

## 适用场景

- **最佳**：大数据批量处理（MapReduce/Spark/Tez）、数据湖、日志归档
- **局限**：不适合小文件、随机读写、低延迟场景
- **现代替代**: 对象存储 (S3) + 计算引擎 (Spark) 模式正在替代 HDFS

## 历史影响

1. **大数据生态基石**：Hadoop 生态的核心存储层
2. **GFS 开源实现**：将 GFS 理念带到开源世界
3. **副本策略**：rack-aware 副本放置被广泛借鉴
4. **Pipeline 写入**：影响后续分布式存储写入设计

## 局限与教训

- NameNode 是单点瓶颈（虽 HA 但未根本解决）
- 小文件问题严重
- 不支持随机写限制了应用场景
- 与对象存储相比，运维成本高
- 现代架构中正在被 S3/OSS + 计算引擎模式替代
