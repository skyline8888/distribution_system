# TiDB

## 基本信息
- 发布时间：2016 年（PingCAP 开源）
- 开发方：PingCAP
- 开源/商用：开源（Apache 2.0）
- 核心论文：*TiDB: A Cloud-Native Distributed HTAP Database* (2016)
- 当前版本：7.x / 8.x

## 架构设计

### 核心架构
```
┌─────────────────────────────────────────────────────┐
│                    TiDB Cluster                       │
├─────────────────────────────────────────────────────┤
│                                                      │
│  TiDB (SQL Layer, Stateless)                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐            │
│  │ TiDB-1   │ │ TiDB-2   │ │ TiDB-N   │            │
│  │ MySQL    │ │ MySQL    │ │ MySQL    │            │
│  │ Protocol │ │ Protocol │ │ Protocol │            │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘            │
│       │            │            │                    │
│       └────────────┼────────────┘                    │
│                    ↓                                 │
│  PD (Placement Driver, Stateful)                     │
│  ┌────────────────────────────────────┐             │
│  │  - 元数据管理 (KV)                  │             │
│  │  - 时间戳分配 (TSO)                 │             │
│  │  - Region 调度                      │             │
│  │  - Raft Group 管理                  │             │
│  └────────────────────────────────────┘             │
│                    ↓                                 │
│  TiKV (Storage Layer, Stateful)                     │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐            │
│  │ TiKV-1   │ │ TiKV-2   │ │ TiKV-N   │            │
│  │ Raft     │ │ Raft     │ │ Raft     │            │
│  │ Region   │ │ Region   │ │ Region   │            │
│  │ RocksDB  │ │ RocksDB  │ │ RocksDB  │            │
│  └──────────┘ └──────────┘ └──────────┘            │
│                    ↓                                 │
│  TiFlash (Columnar, Optional)                       │
│  ┌────────────────────────────────────┐             │
│  │  - 列式存储                        │             │
│  │  - Raft Learner 复制               │             │
│  │  - MPP 执行引擎                    │             │
│  └────────────────────────────────────┘             │
└─────────────────────────────────────────────────────┘
```

### 节点角色
- **TiDB**: 无状态 SQL 层，兼容 MySQL 协议，解析/优化/执行 SQL
- **PD**: 集群大脑，管理元数据、分配 TSO（时间戳）、调度 Region
- **TiKV**: 分布式行式 KV 存储，Raft 共识，Region 分片
- **TiFlash**: 列式分析引擎，通过 Raft Learner 从 TiKV 异步复制

## 存储引擎

### TiKV (行式存储)
- **存储引擎**: RocksDB（LSM-Tree）
- **数据模型**: 有序的 Key-Value
- **分片单位**: Region（默认 96MB，自动分裂合并）
- **Key 编码**: 表 ID → 索引 ID → 行 Key
- **Raft Group**: 每个 Region 一个 Raft Group

### TiFlash (列式存储)
- **存储引擎**: DeltaTree（列式）
- **复制方式**: Raft Learner，不影响 TiKV 写入性能
- **执行引擎**: MPP（大规模并行处理）

## 数据模型与查询语言

- **数据模型**: 关系型（表、索引、视图）
- **查询语言**: 完整 MySQL 协议兼容
- **引擎选择**: 优化器自动选择 TiKV（行）或 TiFlash（列）

## 分布式共识与事务协议

### 1. Raft 共识
- 每个 Region 使用 Raft 保证多副本一致
- Leader 负责读写，Follower 同步
- PD 管理 Raft Group 的 Leader 选举

### 2. Percolator 分布式事务
- 两阶段提交（2PC）优化版
- 乐观事务 + 悲观事务（TiDB 4.0+）
- **TSO (Timestamp Oracle)**: PD 分配全局递增时间戳
  - 逻辑时钟 + 物理时钟混合
  - 保证事务顺序
- **Lock CF**: 专门的列存储锁信息
- **GC**: 基于 MVCC 的垃圾回收

### 3. 事务隔离级别
- SI (Snapshot Isolation) - 默认
- RC (Read Committed)
- 支持 SELECT ... FOR UPDATE

## 复制与分片策略

### Region 分片
- 数据按 Key 范围切分为 Region
- Region 大小 ~96MB，超过自动分裂
- PD 负责 Region 在 TiKV 间的平衡调度
- 热点 Region 自动分裂

### 副本管理
- 默认 3 副本（可配置）
- 支持 Learner 副本（TiFlash）
- 支持 Raft Log 压缩

## 一致性级别

- **强一致性**：同一 Region 内强一致
- **线性一致读**：支持（需 Raft Read Index）
- **全局外部一致性**：通过 TSO 实现
- **跨 Region 事务**：2PC 保证原子性

## 容错与高可用

- **TiDB 无状态**: 任意节点故障不影响，负载均衡重定向
- **TiKV Raft**: 多数派存活即可继续服务
- **PD Raft**: PD 自身也是 Raft 集群，3/5 副本
- **自动 Region 迁移**: TiKV 故障时 PD 自动迁移 Region
- **BR 备份恢复**: 全量/增量备份工具

## 性能特征

| 指标 | 典型值 |
|------|--------|
| 水平扩展 | 透明，在线添加节点 |
| 单表规模 | 无上限（自动分片） |
| 写延迟 | 1-5ms（本地 Raft） |
| 读延迟 | 亚 ms - 数 ms |
| OLTP TPS | 万级（单集群） |
| OLAP 吞吐 | 取决于 TiFlash 节点数 |
| 兼容协议 | MySQL 5.7+ |

## 适用场景

- **最佳**：HTAP 混合负载、MySQL 水平扩展替代、云原生 OLTP
- **优势**：MySQL 兼容、透明分片、HTAP、云原生
- **局限**：TSO 是全局瓶颈、大事务性能下降、跨 Region 事务开销大

## 历史影响

1. **NewSQL 代表**：中国最具影响力的开源数据库之一
2. **HTAP 融合**：TiKV + TiFlash 架构开创了行/列融合分析模式
3. **Raft + Percolator**：为 NewSQL 提供了可复制的参考架构
4. **云原生数据库**：推动了数据库存算分离和云原生趋势
5. **生态影响**：DM (数据迁移)、TiCDC (CDC 工具)、BR (备份恢复) 等配套工具

## 局限与教训

- TSO（PD）是全局串行化瓶颈，限制了极致写入性能
- 大事务（涉及大量 Region）的 2PC 开销显著
- 跨可用区部署时 Raft 延迟较高
- 学习曲线和运维复杂度仍较高
- 某些 MySQL 特性不完全兼容（如存储过程、触发器）
