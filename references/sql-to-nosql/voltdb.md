# VoltDB

## 基本信息

| 维度 | 信息 |
|---|---|
| **发布时间** | 2010 年（前身 H-Store 论文 2007） |
| **开发方** | VoltDB Inc.（Michael Stonebraker 创立） |
| **开源/商用** | 早期开源 AGPLv3 → 2019 年停止开源发布，转为纯商业产品 |
| **核心论文** | Stonebraker et al., "The End of An Architectural Era (It's Time for a Complete Rewrite)", VLDB 2007（H-Store） |
| **关键人物** | Michael Stonebraker（MIT 教授，图灵奖得主）、Sam Madden、Andrew Pavlo |

VoltDB 是 NewSQL 运动的开创者之一，与 Google Spanner 几乎同期诞生，但采取了截然不同的架构路线。它的核心主张极其激进：**如果一切都在内存中，而且你能保证事务确定性执行，你就可以彻底消除锁和 Latch——从而获得惊人的吞吐量。**

---

## 1. Context & Motivation（背景与动机）

### 1.1 解决的问题

2000 年代末期，传统 RDBMS（Oracle、MySQL、PostgreSQL）在 OLTP 场景下面临根本性瓶颈：

- **磁盘 I/O 不再是瓶颈**——随着 DRAM 容量指数增长和数据集缩小到可容纳于内存，大多数 OLTP 工作负载可以完全在内存中运行。但传统数据库仍然带着为磁盘优化的包袱：缓冲池、WAL、页级锁、LW 锁等。
- **锁竞争严重**——多核 CPU 普及（4 核→8 核→16 核），但传统 RDBMS 的共享缓冲池 + 粗粒度锁导致并发扩展性在 8 核以上急剧下降。
- **MVCC 和锁管理器的 CPU 开销**——在纯内存场景下，锁获取和释放占事务执行时间的 40%+，远超 I/O。

**VoltDB 的洞察：**如果数据全部在内存中，而且每个分区只有一个线程访问，那就根本不需要锁。没有锁 = 没有锁竞争 = 线性扩展。

### 1.2 前代系统的局限性

| 局限 | 传统 RDBMS | VoltDB 方案 |
|---|---|---|
| 磁盘缓冲池 | 必须维护页缓存、淘汰策略 | 纯内存，无缓冲池 |
| 锁竞争 | 行级锁、页级锁、表级锁 | 单线程分区，无需锁 |
| 并发控制开销 | MVCC、两阶段锁 | 确定性调度，无并发控制 |
| 多核扩展 | 共享缓冲池导致锁竞争 | Shared-Nothing，线性扩展 |

### 1.3 硬件/网络趋势使其成为可能

- **DRAM 成本暴跌**——2010 年前后，服务器配备 100GB+ 内存成为常态，足以容纳大量 OLTP 数据集。
- **多核 CPU 普及**——Intel Nehalem/Westmere 带来 8–16 核服务器，但传统数据库无法有效利用。
- **10GbE 网络成熟**——Shared-Nothing 节点间通信延迟降低到可接受水平（~100μs 级 RTT）。
- **SSD 开始普及**——虽然不是核心依赖，但持久化日志的写入速度大幅提升。

### 1.4 H-Store 学术渊源

VoltDB 的学术前身是 **H-Store**（2007 年 VLDB 论文），由 Michael Stonebraker 团队在 MIT/Brown/Waterloo 联合开发。H-Store 论文通过实验证明：**一个消除锁和缓冲池的纯内存数据库，可以在 TPC-C 基准测试中比当时的商业 RDBMS 快 79 倍**。VoltDB Inc. 于 2010 年将这一研究产品化。

---

## 2. Architecture（架构设计）

### 2.1 系统拓扑

VoltDB 采用 **Shared-Nothing 架构**，每个节点独立运行，通过高速网络协调。

```
+=============================================================================+
|                         VoltDB Cluster                                      |
|                                                                             |
|  +------------------+    +------------------+    +------------------+       |
|  |     Node 1       |    |     Node 2       |    |     Node N       |       |
|  |  (Host Machine)  |    |  (Host Machine)  |    |  (Host Machine)  |       |
|  |                  |    |                  |    |                  |       |
|  |  +------------+  |    |  +------------+  |    |  +------------+  |       |
|  |  | Partition 0|  |    |  | Partition 1|  |    |  | Partition 3|  |       |
|  |  | [Single-   |  |    |  | [Single-   |  |    |  | [Single-   |  |       |
|  |  |  Threaded] |  |    |  |  Threaded] |  |    |  |  Threaded] |  |       |
|  |  |            |  |    |  |            |  |    |  |            |  |       |
|  |  | +--------+ |  |    |  | +--------+ |  |    |  | +--------+ |  |       |
|  |  | |Tables  | |  |    |  | |Tables  | |  |    |  | |Tables  | |  |       |
|  |  | |Data    | |  |    |  | |Data    | |  |    |  | |Data    | |  |       |
|  |  | |Indexes | |  |    |  | |Indexes | |  |    |  | |Indexes | |  |       |
|  |  | +--------+ |  |    |  | +--------+ |  |    |  | +--------+ |  |       |
|  |  |            |  |    |  |            |  |    |  |            |  |       |
|  |  | Stored Proc|  |    |  | Stored Proc|  |    |  | Stored Proc|  |       |
|  |  | Executor   |  |    |  | Executor   |  |    |  | Executor   |  |       |
|  |  +------------+  |    |  +------------+  |    |  +------------+  |       |
|  |                  |    |                  |    |                  |       |
|  |  +------------+  |    |  +------------+  |    |  +------------+  |       |
|  |  | Partition 2|  |    |  | Partition 4|  |    |  | Partition 5|  |       |
|  |  | [Single-   |  |    |  | [Single-   |  |    |  | [Single-   |  |       |
|  |  |  Threaded] |  |    |  |  Threaded] |  |    |  |  Threaded] |  |       |
|  |  +------------+  |    |  +------------+  |    |  +------------+  |       |
|  +------------------+    +------------------+    +------------------+       |
|                                                                             |
|        <---- K-Safety Replicas (cross-node copies for fault tolerance) ----> |
|                                                                             |
|  +-----------------------------------------------------------------------+  |
|  |                    Network: 10GbE / InfiniBand                        |  |
|  |       (cross-partition txn coordination via 2PC)                      |  |
|  +-----------------------------------------------------------------------+  |
+=============================================================================+

  Legend:
  - Each Partition = exactly 1 thread (no intra-partition locking)
  - Tables partitioned by a chosen partition key
  - K-Safety = each partition has K replicas on different nodes
  - Stored Procedures (Java) run atomically inside a single partition
```

### 2.2 数据模型与命名空间

- **关系模型**——VoltDB 提供标准 SQL 接口（DDL、DML、SELECT），与 PostgreSQL 方言兼容。
- **表**——支持创建表、定义主键、外键、唯一约束、索引。
- **分区表 vs 复制表**：
  - **分区表（Partitioned Table）**——每行根据分区键（Partition Key）被路由到一个分区，只有该分区存储该行。
  - **复制表（Replicated Table）**——全量副本存在于每个分区，适合小维度表/配置表。避免了跨分区 join。
- **存储过程**——业务逻辑封装在 Java 编写的存储过程中，作为 **事务的原子执行单元**。

### 2.3 元数据架构归类 → 算法/计算式（Deterministic Routing）

VoltDB 的元数据管理属于 **算法/计算式** 模式：

- **分区路由是确定性的**——给定一条记录的分区键值，通过哈希函数 `partition_id = hash(partition_key) % num_partitions` 即可直接计算出该记录所在的分区。
- **无中心协调节点**——客户端驱动程序根据分区键直接路由请求到对应分区，不需要元数据服务来查找位置。
- **系统目录复制**——schema 定义、DDL 元数据复制到所有节点。

这与 Ceph CRUSH、Dynamo DHT 属于同一思路：**通过算法计算位置，而非查询元数据服务。**

### 2.4 数据架构分析

#### 分块与分布

| 维度 | VoltDB 方案 |
|---|---|
| 分块方式 | 按行分区（Row-level partitioning），非按块 |
| 分布策略 | 一致性哈希的变体：`hash(partition_key) % N` |
| 分区粒度 | 整个表按一个分区键拆分；也可选择复制（replicated） |

#### 冗余方式 → K-Safety

- **K-Safety** 是 VoltDB 的容错机制：每个分区在内存中保存 K 个副本（通常 K=1，即每个分区 2 份副本）。
- 副本分布在不同物理节点上，确保单一节点故障不丢失数据。
- 写操作同步复制到所有副本后才确认（同步复制，强一致）。
- 故障恢复时，系统从存活副本重建丢失的分区。

#### I/O 路径与持久化

- **纯内存执行**——所有事务执行完全在内存中进行。
- **命令日志（Command Logging）**——VoltDB 不写传统的物理 WAL（Write-Ahead Log），而是写 **逻辑命令日志**：记录调用的存储过程名称及其参数，而非数据页的变化。
  - 恢复时：重放命令日志来重建状态。
  - 优势：日志体积小、写入开销极低。
  - 限制：恢复时间取决于日志重放速度，对大容量数据库可能较慢。
- **快照（Snapshots）**——定期生成内存状态的快照到磁盘，缩短恢复时间。

#### 生命周期管理

- 内存中的数据在系统运行时持久存在（不淘汰）。
- 通过快照 + 命令日志实现持久化。
- 支持 TTL（Time-To-Live）过期策略。

---

## 3. Core Technical Innovations（核心技术创新）

### 3.1 确定性执行模型 → 无锁的根源

这是 VoltDB 最核心、最具革命性的创新。

**传统数据库的并发控制：**
```
Thread A: 获取行锁 → 读取 → 修改 → 释放行锁
Thread B: 等待行锁 → 获取行锁 → 读取 → 修改 → 释放行锁
Thread C: 可能死锁 → 死锁检测 → 回滚
```

**VoltDB 的方式：**
```
Partition 0 (Single Thread):
  Txn 1 执行 → Txn 2 执行 → Txn 3 执行 → ...
  （串行执行，但无锁、无等待、无死锁）
```

**关键前提——确定性：**
1. **每个分区只有一个线程**——同一分区内的所有事务由单个线程串行执行。
2. **事务执行是确定性的**——给定相同的输入（存储过程 + 参数），执行结果总是相同的。不依赖系统时间、随机数、外部 I/O 等非确定性操作。
3. **事务在分区内完全隔离**——因为只有一个线程访问分区，天然实现了串行化（Serializable）隔离级别。

**结果：不需要锁、不需要 Latch、不需要死锁检测、不需要并发控制协议。单分区事务的开销极低。**

### 3.2 事务调度——单分区 vs 跨分区

VoltDB 将事务分为两类：

#### 单分区事务（Single-Partition Transaction）

```
Client → Router → Partition X (单线程执行) → 返回结果
```

- **极快**——直接路由到一个分区，该分区串行执行后返回。
- **无分布式协调**——不涉及其他分区。
- **这是 VoltDB 性能的核心场景**——百万级 tx/sec 的吞吐来自此类事务。

#### 跨分区事务（Multi-Partition Transaction）

```
Client → 协调器 → Partition A ──┐
                                ├── 2PC 协调 → 返回结果
                    Partition B ──┘
```

- **需要两阶段提交（2PC）**——涉及多个分区的数据修改必须通过分布式协调协议保证原子性。
- **性能显著下降**——2PC 需要多轮网络往返和同步，吞吐通常比单分区事务低 1-2 个数量级。
- **VoltDB 通过 DDL 设计引导开发者减少跨分区事务**——通过合理的分区键选择，使大多数事务成为单分区事务。

### 3.3 存储过程模型

```java
// VoltDB 存储过程示例
public class Purchase extends VoltProcedure {
    public final SQLStmt sql1 = new SQLStmt(
        "SELECT balance FROM accounts WHERE account_id = ?;"
    );
    public final SQLStmt sql2 = new SQLStmt(
        "UPDATE accounts SET balance = balance - ? WHERE account_id = ?;"
    );

    public long run(int accountId, int amount) {
        voltQueueSQL(sql1, accountId);
        VoltTable[] results = voltExecuteSQL();

        long balance = results[0].fetchRow(0).getLong(0);
        if (balance < amount) {
            throw new VoltAbortException("Insufficient funds");
        }

        voltQueueSQL(sql2, amount, accountId);
        voltExecuteSQL();
        return balance - amount;
    }
}
```

- **Java 编写**——存储过程用 Java 编写，编译后部署到数据库。
- **原子执行**——整个存储过程在一个事务中原子执行。
- **确定性保证**——存储过程不能执行非确定性操作（如 `System.currentTimeMillis()`、随机数、外部网络调用）。
- **客户端只发送存储过程调用**——不像传统 SQL 中客户端发送多条独立 SQL 语句。

### 3.4 一致性模型

- **强一致性（Strong Consistency）**——同步复制 + 串行化隔离。
- **线性一致性（Linearizability）**——在单分区场景下，由于事务严格按序执行，等价于线性一致性。
- **无最终一致性选项**——VoltDB 不支持弱一致或最终一致性模型，这是设计选择而非缺陷。

### 3.5 容错机制

| 机制 | 说明 |
|---|---|
| **K-Safety 副本** | 每个分区 K 个副本，分布在 N 个节点上 |
| **同步复制** | 写操作等待所有副本确认后才返回 |
| **命令日志** | 持久化逻辑操作（SP 名称 + 参数），而非物理 WAL |
| **快照** | 定期将内存状态序列化到磁盘 |
| **自动恢复** | 节点故障后，从存活副本重建分区 |
| **在线弹性** | 支持运行时添加/移除节点（需数据重分布） |

### 3.6 扩展方式

- **Scale-out（水平扩展）**——通过增加节点和分区来提升吞吐量。
- **线性扩展**——单分区事务的吞吐量随分区数线性增长（无共享资源竞争）。
- **实际限制**——跨分区事务的比例会限制实际扩展效率。经验法则：跨分区事务占比应 < 10%。

---

## 4. Trade-offs (CAP / PACELC)

### 4.1 CAP 定位 → CP（强一致性 + 分区容忍性）

```
        C (Consistency)
       / \
      /   \
     /  ⭐  \   ← VoltDB: CP
    /  VoltDB \
   /           \
  A ----------- P
(Availability) (Partition Tolerance)
```

- **一致性（C）**——同步复制 + 串行化隔离，保证强一致性。
- **分区容忍性（P）**——Shared-Nothing 架构，节点间通过网络协调，容忍网络分区。
- **牺牲可用性（A）**——当网络分区发生时，VoltDB 选择保证一致性而非可用性。如果 K+1 个节点无法通信，系统可能拒绝写入。

**VoltDB 明确选择 CP：** 在金融交易、计费等场景中，数据正确性远比"始终可写"重要。

### 4.2 PACELC 定位

```
PACELC:
  分区时 (P): 选择 C (Consistency) ← 强一致，不牺牲正确性
  正常时 (E): 选择 L (Latency)    ← 极低延迟，但代价是跨分区事务性能差
```

| 场景 | 选择 | 说明 |
|---|---|---|
| 网络分区发生时 | **C** (一致性) | 同步复制，不允忍不一致写入 |
| 正常运行时 | **L** (延迟) | 单分区事务极低延迟，但跨分区事务需要 2PC |

### 4.3 复杂度 vs 运维简单性

| 维度 | VoltDB |
|---|---|
| **设计复杂度** | 极高——确定性执行模型、命令日志、K-Safety、分区调度都需要精细实现 |
| **运维复杂度** | 中等偏上——需要仔细设计分区键；跨分区事务会显著影响性能；内存容量需要精确规划 |
| **开发复杂度** | 高——应用必须围绕单分区事务设计，需要重写业务逻辑以适应分区约束 |

### 4.4 成本 vs 性能

| 优势 | 劣势 |
|---|---|
| 单分区事务百万级 tx/sec | 需要充足内存（数据集必须全部装入内存） |
| 无锁设计最大化多核利用率 | 跨分区事务性能差（2PC 开销） |
| 确定性调度简化了并发控制 | 大事务/复杂查询不受支持 |
| 命令日志体积小 | 恢复时间取决于命令日志重放速度 |

### 4.5 关键权衡总结

```
VoltDB 的核心权衡：

  极致单分区性能  ←─────→  跨分区事务受限
  （百万 tx/sec）          （2PC 慢）

  无锁简单模型    ←─────→  分区键设计至关重要
  （确定性执行）           （选错 = 性能灾难）

  全部内存运行    ←─────→  数据集大小受限
  （极低延迟）             （受 DRAM 容量制约）

  强一致保证      ←─────→  牺牲部分可用性
  （串行化隔离）           （CP 选择）
```

---

## 5. Influence & Legacy（影响与遗产）

### 5.1 技术影响

**VoltDB / H-Store 对分布式数据库领域的深远影响：**

| 影响领域 | 具体贡献 |
|---|---|
| **确定性执行模型** | 证明了在内存场景下消除锁和并发控制的可行性，后续 FaunaDB、Calvin 系统等借鉴了这一思想 |
| **NewSQL 运动** | 与 Spanner 一起定义了 NewSQL 的两个分支：Spanner（分布式 + 强一致）和 VoltDB（内存 + 无锁） |
| **内存数据库浪潮** | 推动了 MemSQL（现 SingleStore）、SAP HANA、Redis Enterprise 等内存数据库的发展 |
| **存储过程作为事务边界** | 影响了后续数据库将业务逻辑下沉到数据库的设计思路 |
| **命令日志（Logical Logging）** | 启发了后续系统使用逻辑重放而非物理 WAL 的思路 |

### 5.2 启发的后续系统

```
H-Store (2007, 学术)
    │
    ├── VoltDB (2010, 商业化)
    │       ├── 确定性单线程分区 → 影响 FaunaDB (Calvin 协议)
    │       ├── 内存 ACID → 影响 SingleStore/MemSQL
    │       └── 命令日志 → 影响 Event Sourcing 架构
    │
    └── Calvin (2012, Yale/Facebook)
            └── 将确定性执行推广为通用事务调度协议
                    └── FaunaDB / FoundationDB (间接影响)

对比分支:
    │
    ├── Spanner (2012, Google)
    │       └── 另一个 NewSQL 分支：分布式 + TrueTime + Paxos
    │               └── CockroachDB, TiDB
    │
    └── VoltDB
            └── 内存 + 无锁 + 确定性执行
```

### 5.3 成为标准的设计模式

1. **"Share-nothing + 单线程分区"**——Redis、KeyDB 等内存数据库也采用单线程模型来避免锁。
2. **"确定性事务调度"**——Calvin 协议将这一思想推广到通用分布式事务，FaunaDB 基于此构建。
3. **"逻辑日志重放"**——与 Event Sourcing 理念不谋而合，影响 CQRS 架构模式。

### 5.4 当前相关性

| 方面 | 现状 |
|---|---|
| **市场地位** | VoltDB Inc. 仍在运营，产品为纯商业版本（不再开源）。在金融交易、电信计费、欺诈检测等超低延迟 OLTP 场景有客户。 |
| **开源遗产** | AGPL 时代的代码已停止更新。社区版 VoltDB 已不存在。 |
| **技术遗产** | VoltDB 的核心思想——确定性执行、无锁内存数据库——已被后续系统（FaunaDB、SingleStore、甚至 Redis 的单线程模型）以不同形式继承。 |
| **适用场景** | 适合：极高吞吐 OLTP、可接受严格分区键设计、数据集可装入内存的场景。不适合：复杂分析查询、跨分区事务为主、大数据集。 |

### 5.5 VoltDB 的历史地位

> VoltDB 证明了：**当硬件条件改变（大容量内存 + 多核 CPU），数据库架构必须重新设计，而不是在旧架构上打补丁。** 这一理念直接催生了 NewSQL 运动，并持续影响着当今分布式数据库的设计方向。

虽然 VoltDB 自身在商业化道路上面临挑战（开源转闭源、生态不如 Spanner 分支广泛），但它提出的 **确定性无锁执行** 是分布式数据库历史上最具原创性的架构创新之一。

---

## 参考资料

1. Stonebraker, M. et al. "The End of An Architectural Era (It's Time for a Complete Rewrite)." VLDB 2007.（H-Store 论文）
2. Kallman, R. et al. "H-Store: A High-Performance, Distributed Main Memory Transaction Processing System." VLDB 2008.
3. VoltDB 官方文档: https://docs.voltdb.com/
4. Pavlo, A. "NewSQL: The Modern Relational Database." (CMU 课程讲义)
5. Stonebraker, M. "The Case for Niche Databases." Communications of the ACM, 2015.
