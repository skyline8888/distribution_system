# Sharding / Partitioning 分片与分区

## 基本信息
- **核心概念**: 将大规模数据集水平切分到多个独立节点（shard/partition）存储与处理
- **首次大规模应用**: Google Bigtable (2006), Amazon Dynamo (2007)
- **核心论文**: Bigtable (OSDI 2006), Dynamo (SOSP 2007), Spanner (OSDI 2012)
- **核心问题**: 单机存储/吞吐有物理上限 → 如何通过分片实现线性水平扩展

---

## 1. Context & Motivation（背景与动机）

### 解决的问题

当数据量或访问量超出单机能力时，系统必须将数据分散到多台机器上。Sharding 是分布式存储系统实现 **水平扩展（scale-out）** 的核心机制：

```
        单机极限                    分片突破
  ┌──────────────┐          ┌────┬────┬────┐
  │   Database   │          │ S1 │ S2 │ S3 │
  │              │    →     │    │    │    │
  │  Capacity:   │          │ 33%│ 33%│ 33%│
  │  Max ~TB     │          └────┴────┴────┘
  │  Max ~10K QPS│            每个分片可独立扩展
  └──────────────┘          总容量 = N × 单分片容量
```

### 前代系统的局限性

| 局限 | 说明 |
|------|------|
| **垂直扩展天花板** | 单机的 CPU、内存、磁盘 IOPS 存在物理上限 |
| **集中式瓶颈** | 单一节点无法承载海量并发读写 |
| **故障域过大** | 单点故障影响全量数据 |
| **地理延迟** | 全球用户访问单一数据中心延迟不可接受 |

### 硬件/网络趋势的推动

- **2000s 中期**: 商品硬件集群成本远低于高端服务器，但需要软件层解决分布式问题
- **网络带宽提升**: 1Gbps → 10Gbps → 25/100Gbps 使得跨节点数据交换成本降低
- **多核化**: CPU 核数增长但单机内存/存储增长不匹配，需要分布式架构

---

## 2. Architecture（架构设计）

Sharding 的核心是 **数据分布策略（Data Distribution Strategy）**，决定每条数据应该存储在哪个分片上。以下是五种主流分区策略的深度分析。

### 2.1 Range-based Partitioning（范围分区）

将键空间按连续的范围划分，每个分片负责一个键区间。

```
键空间: [A ────────────────────────────────── Z]

         ┌──────────┐ ┌──────────┐ ┌──────────┐
  Shard0 │ A..F     │ │ G..M     │ │ N..Z     │
         └──────────┘ └──────────┘ └──────────┘
             Shard1        Shard2        Shard3

  写入 "Hello" → 落在 G..M → 路由到 Shard2
  写入 "Apple" → 落在 A..F → 路由到 Shard1
```

#### 代表系统

| 系统 | 分片单位 | 分裂机制 |
|------|----------|----------|
| **Bigtable** | Tablet (默认 ~100-200MB) | 达到大小阈值时自动 split |
| **HBase** | Region (默认 ~10GB) | 达到大小阈值时自动 split |
| **TiKV** | Region (默认 96MB) | PD 调度 split + merge |
| **Spanner** | Split (类似 Tablet) | 自动 split + 基于负载迁移 |

#### 范围分区的路由机制

```
              Client Request: key="user_5000"
                        │
                        ▼
              ┌─────────────────────┐
              │   Metadata Service  │  ← 维护 key-range → shard 映射
              │   (Bigtable Master) │
              │   (HBase Meta)      │
              │   (TiKV PD)         │
              └────────┬────────────┘
                       │ 查询映射: "user_5000" ∈ [user_4000..user_6000]
                       ▼
              ┌─────────────────────┐
              │   Shard / Region    │  ← 直接处理请求
              │   [user_4000..6000] │
              └─────────────────────┘
```

#### 优点
- **范围查询高效**: `WHERE key BETWEEN x AND y` 只需访问连续的几个分片
- **有序存储**: 数据天然按 key 排序，支持高效的范围扫描
- **分裂简单**: 找到中点即可 split，无需重新 hash

#### 缺点
- **热点风险高**: 如果键有单调递增趋势（如时间戳、自增 ID），写入会集中在最新范围分片
- **分裂后迁移**: 分裂后可能需要数据迁移来平衡负载

#### 热点写入典型案例

```
问题：使用自增 ID 或时间戳作为 row key

  时间 t1: 写入 ID=1001, 1002, 1003 → 全部命中最新 Region
  时间 t2: 写入 ID=1004, 1005, 1006 → 仍然命中同一 Region

  ┌──────────┐ ┌──────────┐ ┌──────────┐
  │ Region 1 │ │ Region 2 │ │ Region 3 │ ← 热点！所有写入集中在此
  │ (空闲)   │ │ (空闲)   │ │ ████████ │
  └──────────┘ └──────────┘ └──────────┘

解决方案：
  1. 对 key 加 salt 前缀 → "001-1001", "002-1002" 打散分布
  2. 使用 hash 前缀 + 原始 key 拼接
  3. 使用反向时间戳 (MAX_TIMESTAMP - now)
```

---

### 2.2 Hash-based Partitioning（哈希分区）

对键做哈希后对分片数取模，将数据均匀分布到 N 个分片。

```
  hash(key) mod N → 分片编号

  示例: N = 4
  ┌─────────────────────────────────────────┐
  │ key        │ hash(key)  │ hash % 4 │ Shrd│
  ├────────────┼────────────┼──────────┼─────┤
  │ "alice"    │ 0x8a3f...  │     2    │ S2  │
  │ "bob"      │ 0x2c1d...  │     1    │ S1  │
  │ "charlie"  │ 0xf7e0...  │     0    │ S0  │
  │ "dave"      │ 0xb5a9...  │     3    │ S3  │
  └────────────┴────────────┴──────────┴─────┘


         ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐
  S0     │  C  │  S1 │  B  │  S2 │  A  │  S3 │  D  │
         └─────┘  └─────┘  └─────┘  └─────┘
```

#### 代表系统

| 系统 | 哈希策略 | 说明 |
|------|----------|------|
| **Cassandra** | Murmur3Hash(key) mod num_tokens | 每个节点多个 token 实现虚拟节点 |
| **DynamoDB** | 内部 hash(partition_key) | 自动分区，对用户透明 |
| **Redis Cluster** | CRC16(key) mod 16384 | 固定 16384 个 hash slot |

#### 路由机制

```
  Client: PUT("alice", {name: "Alice"})
          │
          ▼
  hash("alice") = 0x8a3f12c4
          │
          ▼
  0x8a3f12c4 mod 4 = 2
          │
          ▼
  ┌─────────────────────┐
  │  Shard 2            │  ← 直接计算，无需查询元数据
  │  存储 alice 的数据   │
  └─────────────────────┘
```

#### 优点
- **负载均匀**: 哈希函数保证数据近似均匀分布，天然避免热点
- **无元数据查询**: 客户端可直接计算路由（已知 N 时）
- **实现简单**: 逻辑直观，代码量小

#### 缺点
- **范围查询不可用**: 相邻 key 被哈希打散到不同分片，范围扫描需要访问所有分片（scatter-gather）
- **增减分片成本高**: N 变化时 `hash(key) mod N` 结果全变，绝大多数数据需要重新分布
- **虚拟节点缓解但不解决**: 增加 vnode 可以减少 rehash 量但无法消除

#### 增减分片的数据迁移问题

```
原始: hash(key) mod 4 → [S0, S1, S2, S3]

增加 S4 后: hash(key) mod 5 → 结果全部变化！

  S0 中的数据:  ~80% 需要迁移到其他分片
  S1 中的数据:  ~80% 需要迁移到其他分片
  ...

这就是为什么纯 hash mod N 不适合需要频繁扩缩容的场景。
```

---

### 2.3 Consistent Hashing（一致性哈希）

Dynamo 论文提出的 ring-based 哈希策略，解决 hash mod N 增减分片时大量数据迁移的问题。

```
                    哈希环 (Hash Ring)
                    0 ────────────── 2^128

                          ● vnode-S1a
                    ↗         ╲
              S3 ●              ╲
                                ╲
              S2 ●      ╱        ● vnode-S1b
                       ╱
              S4 ●    ╱
                    ╱
              S1 ● ╱

    写入 key="alice":
      hash("alice") → 环上某个位置
      顺时针找到第一个 vnode → 该 vnode 所属的物理节点负责

    示例: hash("alice") 落在 S2 和 S1b 之间
      顺时针 → 遇到 vnode-S1b → 数据存入 S1
```

#### 虚拟节点（Virtual Nodes）

物理节点少时，数据分布不均匀。通过为每个物理节点分配多个虚拟节点（vnode），可以显著改善均匀性：

```
无 vnode (4 物理节点):          有 vnode (4 物理 × 3 vnode):

  ┌────┬────┬────┬────┐         ┌──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┐
  │S1  │S2  │S3  │S4  │         │1a│2a│3a│1b│4a│2b│3b│4b│1c│2c│3c│4c│
  └────┴────┴────┴────┘         └──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┘
  每节点 25%                    每节点 ~25% 但更均匀
                                单节点故障影响 1/12 而非 1/4
```

#### 代表系统

| 系统 | 实现方式 | vnode 数 |
|------|----------|----------|
| **Dynamo** | SHA-1(key) mod 2^128 | 每个物理节点分配多个位置 |
| **Cassandra** | Murmur3(key)，每节点 256 个 token (v2.0+) | 默认 256 |
| **Riak** | 160-bit 环 | 可配置 |
| **OpenStack Swift** | 分区环 (Partition Ring) | 1024-65536 分区 |

#### 路由机制

```
  Client: GET("alice")
          │
          ▼
  h = SHA-1("alice") = 0x3f8a...
          │
          ▼
  在环上查找: 从 h 位置顺时针扫描
          │
          ▼
  第一个 vnode 归属 → Node X (primary)
  复制因子 N=3 → 顺时针后续 2 个 vnode 也存副本
          │
          ▼
  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ Node X   │  │ Node Y   │  │ Node Z   │
  │ (primary)│  │ (replica)│  │ (replica)│
  └──────────┘  └──────────┘  └──────────┘
```

#### 优点
- **增减分片影响最小**: 新增节点只影响环上相邻区域，理论上只有 1/N 的数据需要迁移
- **去中心化**: 无需集中式元数据服务，每个客户端可自行计算
- **弹性好**: vnode 机制使得负载均衡和故障恢复更平滑

#### 缺点
- **范围查询依然不可用**: 和 hash 分区一样，相邻 key 被哈希打散
- **vnode 增加开销**: 更多 vnode = 更多内存占用和更复杂的环管理
- **热点仍可能**: 如果某个 key 本身访问频率极高（hot key），即使分布均匀也无济于事

#### 新增节点时的数据迁移

```
  原始环 (3 节点):

      ┌─────────────┐
      │    Node A    │  负责 33% 的环
      └─────────────┘
     ╱               ╲
  Node B           Node C
  (33%)            (33%)

  加入 Node D 后:

      ┌─────────────┐
      │    Node A    │  负责 25% 的环 (失去 8%)
      └──────┬──────┘
     ╱       │       ╲
  Node B  ┌──┴──┐  Node C
  (25%)   │New D │  (25%)
          │(25%) │
          └──────┘

  只有 Node A 需要让出一部分数据给 Node D
  迁移量 ≈ 1/4 的数据 (而非 3/4)
```

---

### 2.4 Directory-based Partitioning（目录式分区）

使用独立的元数据服务（metadata server / config server）维护 key → shard 的映射关系。路由是间接的：先查目录，再访问分片。

```
              ┌─────────────────────────────┐
              │     Metadata / Config        │
              │     Server 集群              │
              │                              │
              │  Key Range → Shard Mapping   │
              │  ┌───────────────────────┐   │
              │  │ users:0..999    → S1  │   │
              │  │ users:1000..1999→ S2  │   │
              │  │ orders:*        → S3  │   │
              │  │ products:*      → S4  │   │
              │  └───────────────────────┘   │
              └──────────┬──────────────────┘
                         │ 查询路由
  ┌──────────┐          │          ┌──────────┐
  │ Client   │──────────┼──────────│ Shard S1 │
  │          │          │          │ users    │
  └──────────┘          │          └──────────┘
                        │
                        ▼
               ┌──────────────────┐
               │ Shard S2, S3, S4 │  ← 数据节点
               └──────────────────┘
```

#### 代表系统

| 系统 | 元数据服务 | 特点 |
|------|-----------|------|
| **MongoDB** | Config Server (副本集) | mongos 路由，chunk 大小 64MB-1GB |
| **Elasticsearch** | Master 节点 | shard allocation awareness |
| **HBase** (ZK 辅助) | HMaster + Meta Region | 二级元数据设计 |

#### MongoDB 分片架构详图

```
                    ┌──────────────┐
                    │   mongos     │  ← 查询路由节点 (无状态)
                    │   (Router)   │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
      ┌──────────────┐ ┌──────────┐ ┌──────────┐
      │ Config Server│ │ Shard 1  │ │ Shard 2  │
      │ (副本集 ×3)  │ │ (副本集) │ │ (副本集) │
      │              │ │          │ │          │
      │ 存储 chunk   │ │ 实际数据 │ │ 实际数据 │
      │ 分布元数据   │ │          │ │          │
      └──────────────┘ └──────────┘ └──────────┘
                           │            │
                           ▼            ▼
                    ┌────────────────────────┐
                    │   mongos 缓存 chunk    │
                    │   分布到本地缓存        │
                    │   减少元数据查询        │
                    └────────────────────────┘

  路由过程:
  1. Client → mongos: find({user_id: 12345})
  2. mongos 查本地缓存 (或 Config Server): 12345 在哪个 chunk?
  3. mongos → 目标 Shard: 直接转发查询
  4. Shard 返回结果 → mongos → Client
```

#### 优点
- **灵活性最高**: 可以基于任意维度（key range、tag、地理位置、业务类型）进行路由
- **支持复杂分片策略**: 范围分片 + 哈希分片可以混合使用
- **易于 rebalance**: 元数据服务可以集中调度，无需客户端感知
- **支持 zone/tag 路由**: 可以将特定数据固定在特定物理区域

#### 缺点
- **元数据服务成为关键路径**: 路由查询增加了延迟（即使有缓存）
- **元数据服务本身需要高可用**: Config Server 故障影响全局
- **一致性挑战**: 元数据更新与数据迁移需要协调（MongoDB 通过 distributed lock 解决）

---

### 2.5 分区策略对比矩阵

```
┌────────────┬──────────┬──────────┬───────────────┬───────────────┐
│   维度     │ Range    │ Hash     │ Consistent    │ Directory     │
│            │ Partition│ mod N    │ Hashing       │ Based         │
├────────────┼──────────┼──────────┼───────────────┼───────────────┤
│ 范围查询   │ ✅ 高效   │ ❌ 需全扫 │ ❌ 需全扫      │ 取决于实现     │
│ 负载均匀   │ ⚠️ 依赖键 │ ✅ 均匀   │ ✅ 均匀        │ 取决于策略     │
│ 扩缩容成本 │ 中       │ ❌ 高     │ ✅ 低          │ 中            │
│ 元数据依赖 │ 需        │ 无需     │ 无需           │ 强依赖         │
│ 热点防护   │ ❌ 弱     │ ✅ 强     │ ✅ 强          │ 取决于策略     │
│ 实现复杂度 │ 中       │ 低       │ 中高           │ 高            │
│ 典型系统   │ Bigtable │ Redis    │ Dynamo        │ MongoDB       │
│            │ HBase    │ Cluster  │ Cassandra     │ Elasticsearch │
│            │ TiKV     │          │               │               │
└────────────┴──────────┴──────────┴───────────────┴───────────────┘
```

---

### 2.6 Auto-splitting and Rebalancing（自动分裂与重平衡）

现代分布式存储系统不依赖静态分区，而是通过 **自动分裂 + 自动迁移** 实现弹性伸缩。

#### 分裂机制（Split）

```
阶段 1: 正常状态
  ┌──────────────────────────────────────┐
  │ Region: [key_0001 ........ key_5000] │  大小: 80MB
  └──────────────────────────────────────┘

阶段 2: 达到分裂阈值 (例如 96MB)
  ┌──────────────────────────────────────┐
  │ Region: [key_0001 ........ key_5000] │  大小: 100MB ← 触发 split!
  └──────────────────────────────────────┘

阶段 3: 分裂执行
  ┌──────────────────────┐  ┌──────────────────────┐
  │ L: [key_0001..2500] │  │ R: [key_2501..5000] │
  │      50MB            │  │      50MB            │
  └──────────────────────┘  └──────────────────────┘
```

#### 不同系统的分裂策略

| 系统 | 分裂触发条件 | 分裂中点选择 | 元数据更新方式 |
|------|------------|-------------|---------------|
| **Bigtable** | Tablet 大小 ~200MB | 物理中点 | Master 更新元数据 |
| **HBase** | Region 大小 ~10GB | 可配置 (递增/物理中点) | Meta Region 更新 |
| **TiKV** | Region 大小 96MB | 物理中点 + key count | PD 调度 |
| **MongoDB** | Chunk 大小 64MB-1GB | 中位 key | Config Server 更新 |

#### 重平衡（Rebalance / Migration）

```
初始状态 (负载不均):
  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ Node A   │  │ Node B   │  │ Node C   │
  │ ████████ │  │ ████     │  │ ██       │
  │ 8 Regions│  │ 4 Regions│  │ 2 Regions│
  └──────────┘  └──────────┘  └──────────┘

调度器检测到不均衡 → 迁移决策:
  Node A → Node C: 迁移 2 个 Region
  Node A → Node B: 迁移 1 个 Region

平衡后状态:
  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ Node A   │  │ Node B   │  │ Node C   │
  │ █████    │  │ █████    │  │ █████    │
  │ 5 Regions│  │ 5 Regions│  │ 5 Regions│
  └──────────┘  └──────────┘  └──────────┘
```

#### TiKV PD 调度器详解

```
              ┌─────────────┐
              │     PD      │  ← Placement Driver
              │  (调度大脑)  │
              │             │
              │ 职责:        │
              │ 1. 元数据管理│  ← 维护 Region → Store 映射
              │ 2. 负载均衡  │  ← 检测热点，发起迁移
              │ 3. GC        │  ← 清理废弃数据
              │ 4. TSO 分配  │  ← 全局时间戳
              └──────┬──────┘
                     │ Raft 协议 (3 副本)
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
     ┌─────────┐┌─────────┐┌─────────┐
     │ TiKV-1  ││ TiKV-2  ││ TiKV-3  │
     │ Store 1 ││ Store 2 ││ Store 3 │
     └─────────┘└─────────┘└─────────┘

  调度周期:
  1. PD 定期收集各 Store 的 Region 数量、数据量、QPS
  2. 计算均衡度: max_load / avg_load > 阈值 → 触发迁移
  3. 生成 Operator: "move Region R from Store A to Store C"
  4. TiKV 执行: 复制 Region 数据 → 更新 peer → 删除旧 peer
```

#### 热点调度（Hot Spot Scheduling）

```
问题: 某 Region 因特定 key 访问频繁成为热点

  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ Store A  │  │ Store B  │  │ Store C  │
  │ QPS:5000 │  │ QPS:800  │  │ QPS:700  │ ← 严重不均!
  │ (热点)   │  │          │  │          │
  └──────────┘  └──────────┘  └──────────┘

调度器决策:
  1. 识别热点 Region (基于 read/write QPS)
  2. 如果热点是写入 → split Region，打散写入
  3. 如果热点是读取 → 增加 read follower / 缓存

TiKV 热点分裂:
  原 Region: [user_0001 .. user_9999]  ← QPS 5000
       ↓ split
  ┌─────────────────┐ ┌─────────────────┐
  │ [user_0001..5000]│ │ [user_5001..9999]│
  │  QPS: 2500      │ │  QPS: 2500      │
  │  → Store A      │ │  → Store B      │ ← 迁移到不同 Store
  └─────────────────┘ └─────────────────┘
```

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 Hot Spot / Hot Key 问题与解决方案

#### 问题本质

即使分区策略完美均匀，某些 **key 本身的访问频率极高** 也会导致热点：

```
Hot Key 场景:
  - 秒杀商品: "item_12345" 每秒 100K 次读
  - 热门用户: "user_celebrity" 每秒 50K 次读
  - 计数器: "global_counter" 每秒 200K 次写

  ┌──────────┐
  │ Shard X  │  ← 所有请求命中同一个分片
  │ 存 hot   │  ← 该分片的 CPU/网络/IO 被打满
  │ key 数据 │  ← 其他分片空闲
  └──────────┘
```

#### 解决方案矩阵

| 方案 | 适用场景 | 实现原理 | 代价 |
|------|---------|---------|------|
| **读热点: 缓存层** | 读多写少 | Redis/Memcached 前置缓存热点 key | 增加一层，一致性需管理 |
| **读热点: 读副本** | 读多写少 | 将热点数据复制到多个分片提供读服务 | 读副本一致性延迟 |
| **写热点: 本地计数 + 合并** | 计数器场景 | 每个分片本地计数，定期合并到全局 | 最终一致性，延迟聚合 |
| **写热点: 打散 key** | 写入均匀化 | 将 hot key 拆分为 `hot_key_0..hot_key_N` | 读时需合并 N 个值 |
| **写热点: 异步队列** | 高吞吐写入 | 写入先入消息队列，后台批量处理 | 写入延迟增加 |

#### 打散 Key 示例（Counter 场景）

```
原始: 单个计数器 "page_views"

  所有写入 → "page_views" → 单分片瓶颈

打散: 拆分为 "page_views_0" .. "page_views_7"

  写入: 随机选择 page_views_i (i = random(0..7))
  读取: SUM(page_views_0 .. page_views_7)

  ┌──────────┐ ┌──────────┐ ┌──────────┐
  │ pv_0, pv_1│ │ pv_2, pv_3│ │ pv_4..pv_7│
  │ ~12.5K/s  │ │ ~12.5K/s  │ │ ~25K/s    │  ← 写入分散到 3 个分片
  └──────────┘ └──────────┘ └──────────┘

  读取时: 需要聚合 8 个分片的值 → scatter-gather
  总 QPS = 8 × 单分片写入上限
```

---

### 3.2 Cross-shard Transactions（跨分片事务）

#### 问题

当一次事务涉及多个分片的数据时，如何保证 ACID？

```
  跨分片事务示例 (银行转账):
  
  Txn: 从账户 A 扣 100，给账户 B 加 100
  
  账户 A 在 Shard 1
  账户 B 在 Shard 2
  
  ┌──────────┐         ┌──────────┐
  │ Shard 1  │         │ Shard 2  │
  │ 账户 A:   │         │ 账户 B:   │
  │ $1000    │         │ $500     │
  └──────────┘         └──────────┘
  
  需要协调两个分片:
  1. Shard 1: A.balance -= 100  (prepare)
  2. Shard 2: B.balance += 100  (prepare)
  3. 全部成功 → commit
  4. 任一步失败 → rollback all
```

#### 两阶段提交（2PC）

```
              ┌──────────────┐
              │ Coordinator  │  ← 事务协调者
              │ (TM / Proxy) │
              └──────┬───────┘
                     │
            ┌────────┴────────┐
            ▼                 ▼
     ┌─────────────┐   ┌─────────────┐
     │  Shard 1    │   │  Shard 2    │
     │  (Participant)│ │  (Participant)│
     └─────────────┘   └─────────────┘

  Phase 1 - Prepare:
    Coordinator → Shard 1: PREPARE(Txn, deduct A 100)
    Coordinator → Shard 2: PREPARE(Txn, add B 100)
    Shard 1: 写 undo/redo log → 回复 YES
    Shard 2: 写 undo/redo log → 回复 YES

  Phase 2 - Commit:
    Coordinator: 收到所有 YES → 决定 COMMIT
    Coordinator → Shard 1: COMMIT(Txn)
    Coordinator → Shard 2: COMMIT(Txn)
    Shard 1: 应用修改 → 回复 ACK
    Shard 2: 应用修改 → 回复 ACK
```

#### 跨分片事务的性能代价

```
┌──────────────────────┬─────────────┬──────────────┐
│      指标            │ 单分片事务  │ 跨分片事务   │
│                      │ (本地)      │ (2PC)        │
├──────────────────────┼─────────────┼──────────────┤
│ 网络往返 (RTT)       │ 1           │ 3 (prepare + │
│                      │             │   commit + ack)│
│ 延迟                  │ 本地延迟    │ 2× 网络延迟 + │
│                      │             │   多节点日志  │
│ 吞吐量影响            │ 基准        │ 下降 50-80%  │
│ 协调者单点故障        │ 无          │ 有 (需高可用)│
│ 阻塞问题              │ 无          │ 参与者可能    │
│                      │             │   长时间阻塞  │
│ 实现复杂度            │ 低          │ 高           │
└──────────────────────┴─────────────┴──────────────┘
```

#### 不同系统的跨分片事务方案

| 系统 | 方案 | 一致性 | 说明 |
|------|------|--------|------|
| **Spanner** | 2PC + Paxos + TrueTime | 外部一致 | 全局时间戳简化提交顺序 |
| **TiDB** | Percolator 模型 (optimistic) | 快照隔离 | 无需 2PC prepare 阶段，乐观并发控制 |
| **MongoDB** | 2PC (4.0+) | 快照隔离 | 仅支持副本集分片，跨分片有限支持 |
| **Cassandra** | 不支持跨分片事务 | 最终一致 | LWT 仅单行，无跨行/跨分片事务 |
| **DynamoDB** | TransactWriteItems | 严格一致 | 最多 100 项，单 Region |
| **FoundationDB** | 乐观并发 + 事务层 | 严格序列一致 | 有序 KV 上的 ACID 层 |

#### Percolator 模型（TiDB 采用的方案）

```
Percolator 用乐观并发控制替代传统 2PC:

  1. 事务开始: 获取全局 start_ts (从 PD)
  2. 读取数据: 检查是否有未提交的 write (lock)，如有则回退或等待
  3. 预写: 将数据写入 lock column (带 lock_ts)
  4. Prewrite: 校验无冲突 → 写 lock
  5. Commit: 获取 commit_ts → 写 write column → 清除 lock
  6. 其他事务读: 遇到 lock → 回退 (rollback) 该未提交事务

  ┌──────────────────────────────────────────────────────┐
  │  Key  │  Value Column  │  Lock Column   │ Write Col  │
  ├───────┼────────────────┼────────────────┼────────────┤
  │ row_A │ val@t1        │                │ write@t1   │
  │       │                │ lock@t5 (txn1) │            │ ← txn1 的 lock
  │ row_B │ val@t1        │                │ write@t1   │
  └───────┴────────────────┴────────────────┴────────────┘

  优势: 无传统 2PC 的协调者阻塞问题
  劣势: 冲突时需要回退，高冲突场景性能下降
```

---

### 3.3 分片与复制的交互

分片解决 **容量和吞吐**，复制解决 **可用性和容错**。两者正交但紧密配合：

```
              分片 × 复制 完整架构

          ╔═══════════╗ ╔═══════════╗ ╔═══════════╗
          ║  Shard 1  ║ ║  Shard 2  ║ ║  Shard 3  ║
          ╠═══════════╣ ╠═══════════╣ ╠═══════════╣
          ║ Primary   ║ ║ Primary   ║ ║ Primary   ║
          ║ Replica A ║ ║ Replica A ║ ║ Replica A ║
          ╠═══════════╣ ╠═══════════╣ ╠═══════════╣
          ║ Replica B ║ ║ Replica B ║ ║ Replica B ║
          ║ (Node X)  ║ ║ (Node Y)  ║ ║ (Node Z)  ║
          ╠═══════════╣ ╠═══════════╣ ╠═══════════╣
          ║ Replica C ║ ║ Replica C ║ ║ Replica C ║
          ║ (Node Y)  ║ ║ (Node Z)  ║ ║ (Node X)  ║
          ╚═══════════╝ ╚═══════════╝ ╚═══════════╝

  规则:
  - 每个分片有 3 个副本 (replication factor = 3)
  - 副本分布在不同物理节点
  - Primary 负责写，Replica 可分担读
  - Primary 故障 → 副本选举新 Primary
```

---

## 4. Trade-offs (CAP / PACELC)

### 4.1 CAP 关系：分片如何实现水平扩展

```
                    CAP Triangle

                         ▲
                        / \
                       /   \
          Consistency /     \ Partition Tolerance
                     /       \
                    /─────────\
                   /           \
                  / Availability\
                 /_______________\


  分片的核心价值: 实现 Partition Tolerance (P)
  ───────────────────────────────────────

  不分片的系统:
  - 单节点 → 天然无分区容忍 (节点故障 = 服务不可用)
  - 垂直扩展有极限

  分片的系统:
  - 多节点 → 单个分片故障不影响其他分片 (P 保证)
  - 水平扩展无理论上限
  - 在 P 的前提下，选择 C 或 A:
    - 强一致系统: Spanner, TiDB (CP)
    - 高可用系统: Dynamo, Cassandra (AP)
```

### 4.2 不同分片策略的 CAP 定位

```
┌────────────┬───────────┬────────────┬──────────────────────┬────────────────────────────┐
│ 策略/系统  │ CAP 定位  │ PACELC     │ 分区时选择           │ 正常时选择                 │
├────────────┼───────────┼────────────┼──────────────────────┼────────────────────────────┤
│ Bigtable   │ CP        │ PC/EC      │ 一致性 (Master 协调) │ 一致性 (强读)              │
│ HBase      │ CP        │ PC/EC      │ 一致性 (ZK+HMaster)  │ 一致性                     │
│ TiKV       │ CP        │ PC/EC      │ 一致性 (Raft quorum) │ 一致性 (线性读/快照读)     │
│ Spanner    │ CP        │ PC/EC      │ 一致性 (Paxos+TT)    │ 一致性 (外部一致)          │
│ Dynamo     │ AP        │ PA/EL      │ 可用性 (向量时钟)    │ 延迟 (本地读写)            │
│ DynamoDB   │ 可配置    │ PA/EL或PC  │ 强一致或最终一致可选 │ 按需选择                   │
│ Cassandra  │ AP        │ PA/EL      │ 可用性 (tunable)     │ 延迟 (local quorum)        │
│ MongoDB    │ 可配置    │ 可配置     │ 多数写=CP, 单写=AP   │ 可配置 read preference     │
│ Redis Clst │ AP+弱C    │ PA/EL      │ 可用性 (异步复制)    │ 延迟 (单线程)              │
└────────────┴───────────┴────────────┴──────────────────────┴────────────────────────────┘
```

### 4.3 关键权衡

#### 一致性 vs 可用性 (C vs A)

```
  强一致分片 (CP):
    写入: 需要 quorum (N/2+1) 副本确认 → 延迟高但数据可靠
    读取: 从 Primary 或 quorum 读 → 读到最新数据
    故障: quorum 不可用时拒绝服务 → 保证一致性

  最终一致分片 (AP):
    写入: 写任意可用副本 → 延迟低
    读取: 读任意副本 → 可能读到旧数据
    故障: 尽可能响应 → 通过 vector clock / last-write-wins 解决冲突
```

#### 延迟 vs 一致性 (PACELC: E vs C)

```
  即使无分区 (E case)，系统仍需权衡:

  选择 E (Efficiency/Latency):
    - Dynamo: 本地读写，不等待其他副本
    - Cassandra LOCAL_QUORUM: 只等本地数据中心副本
    - 代价: 可能读到过时数据

  选择 C (Consistency):
    - Spanner: 等待 TrueTime 确认 → 确保外部一致
    - TiKV: 线性读需要等 Raft apply
    - 代价: 延迟增加 1-3 个 RTT
```

#### 分片粒度权衡

```
  粗粒度分片 (大 shard):
    ┌──────────────────────────┐
    │  大 Shard (e.g. 10GB)    │
    │  - 管理开销小            │
    │  - 分裂/迁移频率低       │
    │  - 但: 单 shard 故障影响大│
    │  - 但: 负载可能不均      │
    └──────────────────────────┘

  细粒度分片 (小 shard):
    ┌─────┐┌─────┐┌─────┐┌─────┐
    │ 小  ││ 小  ││ 小  ││ 小  │
    │ -管理开销大            │
    │ -分裂/迁移频繁          │
    │ +但: 负载均衡更精细      │
    │ +但: 故障影响面小        │
    └─────┘└─────┘└─────┘└─────┘

  业界趋势: 从小向大演进 (MongoDB: 64MB → 1GB chunk)
  原因: 运维稳定性 > 极致均衡
```

---

## 5. Influence & Legacy（影响与遗产）

### 5.1 技术谱系（Technology Genealogy）

```
                    分片策略演进谱系

 Google Bigtable (2006)
    │
    ├──→ 范围分区 (Tablet) + 自动分裂
    │    ├──→ HBase (2008) ──→ 开源 Bigtable 范式
    │    ├──→ TiKV (2016) ──→ Range + Raft + PD 调度
    │    └──→ Spanner (2012) ──→ Range + TrueTime + Paxos
    │
 Amazon Dynamo (2007)
    │
    ├──→ 一致性哈希 (Ring) + 虚拟节点
    │    ├──→ Cassandra (2008) ──→ Dynamo 模型 + Bigtable 数据模型
    │    ├──→ Riak (2010) ──→ 纯 Dynamo 实现
    │    ├──→ Voldemort (2008) ──→ LinkedIn 的 Dynamo 实现
    │    └──→ DynamoDB (2012) ──→ 托管版，内部一致性哈希
    │
 MongoDB (2009)
    │
    └──→ 目录式分片 (Config Server) + Chunk 管理
         └──→ 影响: 许多 SaaS 数据库采用 Config Server 模式

 混合演进 (2010s 后期):
    ├──→ CockroachDB ──→ Range 分区 + Raft (Spanner 开源替代)
    ├──→ FoundationDB ──→ Range 分区 + ACID 层
    └──→ YugabyteDB ──→ Range (DocDB) + Raft
```

### 5.2 成为行业标准的设计模式

| 模式 | 来源 | 后续采用者 |
|------|------|-----------|
| **自动分裂 + 迁移** | Bigtable Tablet | HBase Region, TiKV Region, MongoDB Chunk |
| **一致性哈希 + vnode** | Dynamo | Cassandra, Riak, OpenStack Swift |
| **Range + 共识协议** | Spanner | CockroachDB, TiKV, YugabyteDB |
| **Config Server 路由** | MongoDB | 众多 SaaS 数据库 |
| **Percolator 事务模型** | Google Percolator | TiDB, FoundationDB |

### 5.3 当前相关性与趋势

#### 现代趋势

1. **自动分片成为标配**: 用户不再手动管理分片边界，系统自动 split/merge/migrate
2. **混合分区策略**: 同一系统内不同表/集合可采用不同分区策略（如 MongoDB 的 hashed shard key + ranged shard key）
3. **Serverless 分片**: DynamoDB, Cosmos DB 等完全隐藏分片细节，按需自动扩展
4. **多租户隔离**: 通过分片实现租户间物理隔离（如 TiDB 的 Resource Control）

#### 仍未解决的问题

- **跨分片 JOIN**: 仍然昂贵，需要 scatter-gather 或数据冗余
- **跨分片事务延迟**: 2PC 本质需要多轮网络往返
- **重平衡期间的性能抖动**: 迁移大量数据时影响在线服务
- **热点 key 的终极方案**: 仍需要业务层配合（缓存、打散、异步化）

---

## 参考资料

1. **Bigtable**: Chang, F. et al. "Bigtable: A Distributed Storage System for Structured Data." OSDI 2006.
2. **Dynamo**: DeCandia, G. et al. "Dynamo: Amazon's Highly Available Key-value Store." SOSP 2007.
3. **Spanner**: Corbett, J.C. et al. "Spanner: Google's Globally-Distributed Database." OSDI 2012.
4. **Cassandra**: Lakshman, A. and Malik, P. "Cassandra: A Decentralized Structured Storage System." SIGOPS 2010.
5. **TiKV Design**: https://tikv.org/docs/latest/concepts/
6. **MongoDB Sharding**: https://www.mongodb.com/docs/manual/sharding/
7. **FoundationDB**: https://apple.github.io/foundationdb/
8. **Percolator**: Peng, D. and Dabek, F. "Large-scale Incremental Processing Using Distributed Transactions and Notifications." OSDI 2010.
