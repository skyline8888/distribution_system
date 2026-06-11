# Cassandra

## 基本信息
- 发布时间：2008 年（Facebook 开源）
- 开发方：Avc Lacave (Facebook) → Apache
- 开源/商用：开源（Apache 2.0）
- 核心论文：*Dynamo: Amazon's Highly Available Key-value Store* (2007) + *BigTable* (2006) 的结合
- 当前版本：4.x / 5.0

## 架构设计

### 核心架构
```
┌──────────────────────────────────────────────────────┐
│              Cassandra Cluster (Peer-to-Peer)         │
├──────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐            │
│  │Node A│  │Node B│  │Node C│  │Node N│            │
│  │      │  │      │  │      │  │      │            │
│  │ Gossip│  │ Gossip│  │ Gossip│  │ Gossip│            │
│  │ Ring  │  │ Ring  │  │ Ring  │  │ Ring  │            │
│  └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘            │
│     └──────────┴──────────┴──────────┘               │
│                      ↓                                │
│              Gossip Protocol                          │
│              (Decentralized Membership)               │
└──────────────────────────────────────────────────────┘
```

### 核心特征
- **完全去中心化**：无主节点，所有节点平等
- **一致性哈希环 (Token Ring)**：数据分布在环上
- **Gossip 协议**：节点间交换状态信息
- **无单点**：任何节点可处理任何请求

## 核心技术

### 1. 一致性哈希
- 节点和数据都映射到 2^127 的哈希环
- Virtual Node (vnode)：每个物理节点对应多个虚拟节点
- 数据路由：key 的 hash 值顺时针找到的第一个节点

### 2. 写入路径 (LSM-Tree 风格)
```
Write → Commit Log → MemTable (内存) → SSTable (磁盘, 不可变)
                                           ↓
                                    Compaction (合并)
```
- 写入先写 commit log（持久化保证）
- 然后写入内存 MemTable
- MemTable 满后刷盘为不可变 SSTable
- 后台 Compaction 合并多个 SSTable

### 3. 读取路径
```
Read → Row Cache (可选) → MemTable → SSTable (多个, Bloom Filter 过滤)
```
- 先查 Row Cache（如果启用）
- 然后查 MemTable
- 最后查多个 SSTable（使用 Bloom Filter 跳过无关文件）
- 读放大是主要性能挑战

### 4. Gossip 协议
- 每节点每秒与 1-3 个随机节点交换信息
- 传播节点状态、负载、schema 变更
- 最终一致性：信息传播到全集群需要时间

## 数据模型

- **数据模型**: 宽列存储（Wide Column）
- **查询语言**: CQL (Cassandra Query Language) - SQL-like
- **核心概念**:
  - Keyspace → 类似 database
  - Table → 行+列，但列是动态的
  - Partition Key → 决定数据分布
  - Clustering Key → 分区内排序
- **设计原则**: 按查询建模（Query-First Design）

## 分布式共识与事务

- **无分布式事务**：不支持跨行/跨分区 ACID
- **轻量事务 (LWT)**: 基于 Paxos 的单行条件更新（`IF NOT EXISTS`）
- **Batch**: 同一分区内的批量操作原子，跨分区不保证

## 复制与分片策略

### 复制
- **复制因子 (RF)**: 每个数据副本数，通常 3
- **复制策略**:
  - SimpleStrategy: 简单顺时针
  - NetworkTopologyStrategy: 按数据中心/机架分布

### 分片
- 由一致性哈希决定
- 一个 Partition 的所有数据在同一节点
- 无法拆分单个 Partition（Partition 大小受限）

## 一致性级别

- **可调一致性 (Tunable Consistency)**: 每个请求可指定
  - `ONE`: 一个副本确认
  - `QUORUM`: 多数副本确认 (RF/2 + 1)
  - `ALL`: 所有副本确认
  - `LOCAL_QUORUM`: 本地数据中心多数
  - `EACH_QUORUM`: 每个数据中心多数
- **写 + 读 > 副本数** → 强一致性（如 W=QUORUM, R=QUORUM）
- **CAP 取舍**: 偏向 AP（高可用 + 分区容错）

## 容错与高可用

- **无单点故障**: 任何节点可下线而不影响集群
- ** hinted handoff**: 目标节点不可用时，临时存储并转发
- **Read/Repair**: 读时检测不一致并自动修复
- **Anti-Entropy (Merkle Tree)**: 后台完整性检查
- **节点添加/移除**: 自动重新平衡数据

## 性能特征

| 指标 | 典型值 |
|------|--------|
| 水平扩展 | 线性，几乎无上限 |
| 写入吞吐 | 极高（追加写，无随机写） |
| 写入延迟 | 亚 ms - ms |
| 读取延迟 | ms 级（受读放大影响） |
| 读取吞吐 | 低于写入 |
| 最大节点数 | 数百（受 Gossip 限制） |
| 最大数据量 | PB 级 |

## 适用场景

- **最佳**：写入密集型、时序数据、IoT、消息队列、需要跨地域多活
- **优势**：线性扩展、多数据中心、写入性能极高、运维简单
- **局限**：不支持复杂查询、无 JOIN、读性能弱于写、分区大小受限

## 历史影响

1. **LSM-Tree 普及**：推动了 LSM 存储引擎在分布式系统中的广泛应用
2. **可调一致性**：开创了按请求级别调一致性的先河
3. **去中心化架构**：Gossip + 一致性哈希成为无中心设计的标准模式
4. **Query-First 建模**：影响了 NoSQL 的数据建模方法论
5. **Dynamo 论文最佳实现**：最接近 Dynamo 论文精神的开源实现

## 局限与教训

- 无单点意味着 schema 变更需要传播到全集群
- 大 Partition 是反模式，需要应用层避免
- 不支持 JOIN 和复杂聚合
- 读放大（多 SSTable 查询）影响读性能
- Gossip 协议限制了集群规模（通常不超过 100 节点）
- 运维中 tombstone 管理是常见坑
