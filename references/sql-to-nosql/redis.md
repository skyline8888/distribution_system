# Redis (2009)

## 基本信息
- **发布时间**：2009 年（最初版本），持续活跃开发至今
- **作者**：Salvatore Sanfilippo（antirez）
- **开源/商用**：开源（BSD 许可证）
- **官方网站**：https://redis.io
- **仓库**：https://github.com/redis/redis
- **当前状态**：极度活跃，7.x/8.x 主线；Redis 公司商业化提供 Redis Cloud / Stack；部分模块转为双许可证（RSALv2 / SSPLv1）
- **⚠ 许可证变化**：2024 年 3 月起，Redis 核心引擎及部分模块从 BSD 改为双许可证（RSALv2 / SSPLv1）。BSD 版本停留在 7.2.4，社区 fork 如 Valkey（Linux Foundation）继续 BSD/Apache 路线

---

## 5-Dim 分析框架

### 一、数据模型 (Data Model)

**类型**：内存数据结构存储（In-Memory Data Structure Store）

Redis 的数据模型围绕**原生数据结构**构建——每种结构有专属的底层编码，而非通用的"值" blob。这使得 Redis 不仅是 KV 存储，更是一个带语义的远程数据结构服务器。

**核心数据结构**：

| 数据结构 | 底层编码 | 典型场景 |
|----------|----------|----------|
| **String** | int / raw / embstr | 缓存值、计数器、分布式锁、session |
| **Hash** | ziplist / hashtable | 对象存储（用户资料、商品属性） |
| **List** | quicklist (linkedlist + ziplist) | 消息队列、最新 N 条记录、时间线 |
| **Set** | intset / hashtable | 标签、交集/并集/差集运算 |
| **Sorted Set (ZSet)** | ziplist / skiplist + hashtable | 排行榜、延迟队列、范围查询 |
| **Bitmap** | string (位操作) | 用户签到、活跃统计、布隆过滤器底层 |
| **HyperLogLog** | 稀疏/密集编码 | UV 统计、基数估算（误差 ~0.81%） |
| **Geo** | sorted set | 地理位置、附近的人、距离计算 |
| **Stream** | radix tree (listpack) | 事件溯源、消息队列、消费者组 |

```
  ┌─────────────────────────────────────────────────────────────┐
  │  Redis Data Model                                           │
  │                                                             │
  │  Key ──────────────────────► Value (typed data structure)   │
  │                                                             │
  │  "user:1001"               Hash {name, email, age}          │
  │  "counter:page:home"       String → "42891" (INCR)          │
  │  "queue:orders"            List → LPUSH / BRPOP             │
  │  "tags:article:42"         Set → SADD / SINTER              │
  │  "leaderboard:game"        ZSet → ZADD / ZRANGE / ZRANK     │
  │  "daily:active:2024-01"    Bitmap → SETBIT / BITCOUNT       │
  │  "hll:visitors"            HyperLogLog → PFADD / PFCOUNT    │
  │  "geo:shops"               Geo → GEOADD / GEORADIUS         │
  │  "events:orders"           Stream → XADD / XREAD / XGROUP   │
  │                                                             │
  │  所有操作都是原子性的（单线程事件循环保证）                  │
  │  支持 TTL（过期时间）、事务（MULTI/EXEC）、Lua 脚本          │
  └─────────────────────────────────────────────────────────────┘
```

**编码优化机制**：Redis 会根据数据内容自动选择最优底层编码：

```
  String 编码选择:
    整数且在 [LONG_MIN, LONG_MAX] → int 编码（直接存储）
    ≤ 44 字节的小字符串          → embstr（一次分配，嵌入 SDS）
    其他                        → raw（SDS 动态分配）

  Hash 编码选择:
    字段数 ≤ 512 且所有值 ≤ 64 字节 → ziplist（紧凑连续内存）
    其他                            → hashtable

  这种"小数据紧凑、大数据灵活"的策略是 Redis 内存效率的关键
```

**内存管理**：
- **SDS (Simple Dynamic String)**：替代 C 字符串，O(1) 获取长度，预分配减少 realloc，二进制安全
- **内存淘汰策略（Eviction）**：`noeviction` / `allkeys-lru` / `volatile-lru` / `allkeys-lfu` / `volatile-lfu` / `allkeys-random` / `volatile-ttl` / `volatile-lru`（带 TTL 的键优先淘汰）
- **最大内存限制**：`maxmemory` 配置，达到上限后按策略淘汰或拒绝写入

---

### 二、架构设计 (Architecture)

#### 整体架构

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                        Redis Server Architecture                    │
  │                                                                     │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │                    Single-Threaded Event Loop                 │  │
  │  │                    (Main Command Processing)                  │  │
  │  │                                                               │  │
  │  │  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌─────────┐ │  │
  │  │  │ Network  │───►│ Protocol │───►│ Command  │───►│  Reply  │ │  │
  │  │  │ I/O      │    │ Parser   │    │ Execute  │    │  Encode │ │  │
  │  │  │ (epoll)  │    │ (RESP)   │    │ (Atomic) │    │  (RESP) │ │  │
  │  │  └──────────┘    └──────────┘    └──────────┘    └─────────┘ │  │
  │  │       ▲                                                    │   │  │
  │  │       │              Redis 6+: I/O Threads (仅网络收发)      │   │  │
  │  │       └────────────────────────────────────────────────────┘   │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                     │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │                    Memory Store (In-Memory)                   │  │
  │  │                                                               │  │
  │  │  dict ──► key → value (typed data structures)                │  │
  │  │         key → expire (TTL dict)                              │  │
  │  │                                                               │  │
  │  │  ┌─────────┐  ┌──────────┐  ┌───────┐  ┌─────────┐          │  │
  │  │  │ String  │  │ Hash     │  │ List  │  │ Set/ZSet│  ...     │  │
  │  │  │ (SDS)   │  │ (ziplist │  │ (quick│  │ (skip   │          │  │
  │  │  │ int/raw │  │  /hashtbl)│  │ list) │  │  list)  │          │  │
  │  │  └─────────┘  └──────────┘  └───────┘  └─────────┘          │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                     │
  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐  │
  │  │  Persistence  │  │  Replication  │  │  Pub/Sub & Lua &      │  │
  │  │  RDB / AOF    │  │  (async)      │  │  Streams & Modules    │  │
  │  └───────────────┘  └───────────────┘  └───────────────────────┘  │
  └─────────────────────────────────────────────────────────────────────┘
```

#### 单线程事件循环模型

```
  ┌──────────────────────────────────────────────────────────────┐
  │                    Event Loop (ae.c)                         │
  │                                                              │
  │  while (running) {                                           │
  │    1. beforeSleep()  ← 处理后台任务（AOF 刷盘、过期键清理）  │
  │                                                              │
  │    2. aeProcessEvents()                                      │
  │       ├─ 文件事件 (File Events)                              │
  │       │  └─ epoll/kqueue/evport 多路复用                     │
  │       │     ├─ 接受新连接 (acceptTcpHandler)                 │
  │       │     ├─ 读取客户端请求 (readQueryFromClient)          │
  │       │     └─ 向客户端发送响应 (writeToClient)              │
  │       │                                                      │
  │       └─ 时间事件 (Time Events)                              │
  │          ├─ serverCron() — 主循环心跳                        │
  │          │  ├─ 过期键渐进式删除 (activeExpireCycle)          │
  │          │  ├─ AOF 重写检查                                  │
  │          │  ├─ RDB 快照检查                                  │
  │          │  ├─ 客户端超时检测                                │
  │          │  └─ 统计信息更新                                  │
  │          └─ 其他定时任务                                     │
  │                                                              │
  │    3. 处理已就绪的命令（原子执行，无锁）                      │
  │  }                                                           │
  └──────────────────────────────────────────────────────────────┘
```

**为什么单线程？**
- 无锁：不需要 mutex/atomic，所有操作天然原子
- 无上下文切换：CPU 缓存友好
- 无竞争条件：不存在并发写同一 key 的问题
- 瓶颈在内存/网络，不在 CPU：内存操作 ~100ns，网络 RTT ~ms 级

**Redis 6+ I/O 线程**：
- 仅用于网络 I/O（读取请求、发送响应）的并行化
- 命令执行仍然是单线程
- 默认关闭，需要显式配置 `io-threads-do-reads yes` + `io-threads N`
- 典型配置 4 线程可提升 2-3 倍吞吐量（网络密集场景）

#### 客户端交互协议 (RESP)

```
  Redis Serialization Protocol (RESP) / RESP3

  请求示例: GET user:1001
    *2\r\n       ← 2 个参数
    $3\r\n       ← 第1个参数长度 3
    GET\r\n      ← 第1个参数
    $9\r\n       ← 第2个参数长度 9
    user:1001\r\n← 第2个参数

  响应示例 (字符串):
    $6\r\n       ← bulk string，长度 6
    antirez\r\n  ← 值

  响应示例 (整数):
    :42\r\n      ← integer 42

  响应示例 (数组):
    *3\r\n       ← 3 个元素
    $1\r\n       ← 第1个元素: "a"
    a\r\n
    $1\r\n       ← 第2个元素: "b"
    b\r\n
    $1\r\n       ← 第3个元素: "c"
    c\r\n

  RESP3 新增类型: push、attribute、set、map、boolean、double、big number
```

---

### 三、核心技术 (Core Technologies)

#### 1. 持久化机制

Redis 是内存数据库，数据默认在内存中。持久化是可选的，但生产环境几乎必配。

**RDB (Redis Database) — 快照**

```
  RDB 快照过程:

  主进程                          fork() 子进程
  ┌──────────────┐                ┌──────────────┐
  │ 继续处理命令  │                │              │
  │ (不阻塞)     │                │ 遍历内存数据  │
  │              │◄── COW ─────── │ 写入临时 RDB │
  │              │   (写时复制)   │ 文件         │
  │              │                │              │
  │              │                │ 完成后替换   │
  │              │                │ dump.rdb     │
  │              │                │ 退出         │
  └──────────────┘                └──────────────┘

  触发方式:
  - 手动: BGSAVE / SAVE
  - 自动: save <seconds> <changes> 配置
    save 900 1     ← 900 秒内至少 1 次变更
    save 300 10    ← 300 秒内至少 10 次变更
    save 60 10000  ← 60 秒内至少 10000 次变更

  优点: 文件紧凑，恢复快，适合备份
  缺点: 快照间隔内数据可能丢失（最多丢失 save 间隔的写入）
  注意: fork() 时 COW 会消耗额外内存（约数据量大小）
```

**AOF (Append-Only File) — 追加日志**

```
  AOF 写入过程:

  每个写命令 → 追加到 aof_buf → 按 fsync 策略刷盘

  fsync 策略:
  ┌──────────────────┬────────────┬──────────────────────────────┐
  │ always           │ 每次写刷盘 │ 最安全，最慢（~2000 ops/s）  │
  │ everysec (默认)  │ 每秒刷盘   │ 安全与性能平衡（最多丢 1s）  │
  │ no               │ OS 决定    │ 最快，丢数据不可控            │
  └──────────────────┴────────────┴──────────────────────────────┘

  AOF 重写 (BGREWRITEAOF):
  - AOF 文件随时间增长（SET x 1 → SET x 2 → SET x 3，只需保留最后一条）
  - 子进程基于当前内存数据生成最小化的 AOF
  - 重写期间主进程继续写新 AOF，重写完成后合并

  优点: 数据安全性高（everysec 最多丢 1 秒）
  缺点: 文件通常比 RDB 大，恢复速度慢
```

**混合持久化（Redis 4.0+）**

```
  AOF 混合模式 (aof-use-rdb-preamble yes):

  AOF 文件 = [RDB 格式 preamble] + [AOF 增量命令]

  ┌───────────────────────────────────────────────────┐
  │ AOF 文件结构                                       │
  │                                                   │
  │  ┌─────────────┐  ┌─────────────────────────────┐ │
  │  │ RDB Preamble│  │ AOF Incremental Commands    │ │
  │  │ (完整快照)   │  │ (RDB 之后的增量写命令)       │ │
  │  └─────────────┘  └─────────────────────────────┘ │
  │                                                   │
  │  恢复时: 先加载 RDB 快照 → 再回放 AOF 增量命令     │
  │  兼顾: RDB 的快速恢复 + AOF 的数据安全             │
  └───────────────────────────────────────────────────┘

  这是生产环境推荐配置
```

#### 2. 复制 (Replication)

```
  Master ─────────────────────► Replica (Slave)
                               (异步复制，默认)

  全量同步 (Full Resync):
  ┌──────────┐                          ┌──────────┐
  │ Master   │                          │ Replica  │
  │          │                          │          │
  │ BGSAVE ──┤ 生成 RDB 快照            │          │
  │          ├──────── RDB ────────────►│          │
  │          │  + 复制缓冲区的增量命令   │ LOAD DB  │
  │          │                          │          │
  │ 继续服务  │                          │ 就绪     │
  └──────────┘                          └──────────┘

  部分同步 (Partial Resync, Redis 2.8+):
  ┌──────────┐                          ┌──────────┐
  │ Master   │                          │ Replica  │
  │          │ PSYNC <runid> <offset>   │          │
  │          ├─────────────────────────►│ 断开重连  │
  │          │ 如果在 replication buf   │          │
  │          │ 中找到 offset 之后的数据  │          │
  │          │                          │          │
  │          ├──── 增量数据 ───────────►│ 继续同步  │
  │          │ (无需全量 RDB)            │          │
  └──────────┘                          └──────────┘

  Replication ID + Offset → 替代旧版 runid 支持级联复制断点续传
```

**复制特性**：
- 异步复制：Master 写完即返回，不等待 Replica 确认
- 链式复制：Replica 可以有自己的 Replica（级联）
- 无盘复制（Diskless Replication, Redis 2.8.18+）：Master 直接通过网络发送 RDB，不写本地磁盘
- 复制积压缓冲区（Replication Backlog）：环形缓冲区，存储最近的写命令，支持部分同步

#### 3. Sentinel — 高可用

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                     Redis Sentinel 架构                          │
  │                                                                  │
  │    ┌──────────┐                                                  │
  │    │ Sentinel │◄── 监控 Master & 其他 Sentinel                   │
  │    │    S1    │    使用 Gossip 协议交换信息                      │
  │    └────┬─────┘                                                  │
  │         │                                                        │
  │    ┌────┴─────┐     ┌────┬─────┐                                 │
  │    │ Sentinel │     │Sentinel│                                   │
  │    │    S2    │     │  S3    │                                   │
  │    └────┬─────┘     └────┬─────┘                                 │
  │         │                │                                       │
  │    ┌────┴────────────────┴────────┐                              │
  │    │      Quorum 决策              │                              │
  │    │  (至少 quorum 个 Sentinel     │                              │
  │    │   同意才触发故障转移)          │                              │
  │    └────────────┬─────────────────┘                              │
  │                 │                                                │
  │                 ▼                                                │
  │    ┌────────────────────────────┐                                │
  │    │    故障转移流程             │                                │
  │    │                            │                                │
  │    │  1. 主观下线 (SDOWN)        │                                │
  │    │     — 单个 Sentinel 判定    │                                │
  │    │                            │                                │
  │    │  2. 客观下线 (ODOWN)        │                                │
  │    │     — quorum 个 Sentinel    │                                │
  │    │       达成一致              │                                │
  │    │                            │                                │
  │    │  3. 选举 Leader Sentinel    │                                │
  │    │     — Raft 算法             │                                │
  │    │                            │                                │
  │    │  4. 选择新 Master           │                                │
  │    │     — 优先级 + 复制进度      │                                │
  │    │       + runid               │                                │
  │    │                            │                                │
  │    │  5. SLAVEOF NO ONE (新Master)│                               │
  │    │     SLAVEOF <new> (其他)    │                                │
  │    │                            │                                │
  │    │  6. 更新配置，通知客户端      │                                │
  │    └────────────────────────────┘                                │
  └──────────────────────────────────────────────────────────────────┘
```

**Sentinel 关键点**：
- Sentinel 本身也是 Redis 实例（特殊模式），可独立部署
- 使用 Raft 协议选举 Leader Sentinel（不是 Master 选举）
- 客户端通过 Sentinel 获取当前 Master 地址（或通过客户端库自动发现）
- 脑裂风险：网络分区时可能出现两个 Master（旧 Master 未感知被替换）

#### 4. Redis Cluster — 分布式

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                        Redis Cluster                               │
  │                                                                    │
  │  16384 个 Hash Slots                                               │
  │  ┌─────────┬─────────┬─────────┬──────────┬──────────┬──────────┐  │
  │  │ 0       │ 5460    │ 10923   │          │          │ 16383    │  │
  │  │ Node A  │ Node B  │ Node C  │          │          │          │  │
  │  │ [0-5460]│[5461-   │[10924-  │          │          │          │  │
  │  │         │  10923] │  16383] │          │          │          │  │
  │  └─────────┴─────────┴─────────┴──────────┴──────────┴──────────┘  │
  │                                                                    │
  │  每个 Master 可带 Replica（1:N）                                    │
  │  Master A ──► Replica A1                                           │
  │  Master B ──► Replica B1                                           │
  │  Master C ──► Replica C1                                           │
  │                                                                    │
  │  ┌──────────────────────────────────────────────────────────────┐  │
  │  │  请求路由:                                                    │  │
  │  │                                                              │  │
  │  │  Client ──► 任意节点 (不一定是 key 所属节点)                  │  │
  │  │                                                              │  │
  │  │  如果 key 在本节点: 直接处理                                  │  │
  │  │  如果 key 不在: 返回 MOVED <slot> <node> 重定向              │  │
  │  │                     或 ASK <slot> <node> (迁移中)             │  │
  │  │                                                              │  │
  │  │  ┌──────────┐     MOVED 3000 ┌──────────┐                    │  │
  │  │  │ Node A   │ ──────────────►│ Node B   │                    │  │
  │  │  │ (slot    │                │ (slot    │                    │  │
  │  │  │  0-5460) │                │ 5461-    │                    │  │
  │  │  │          │◄────────────── │ 10923)   │                    │  │
  │  │  │          │  响应数据       │          │                    │  │
  │  │  └──────────┘                └──────────┘                    │  │
  │  │                                                              │  │
  │  │  智能客户端: 缓存 slot→node 映射，减少重定向                  │  │
  │  └──────────────────────────────────────────────────────────────┘  │
  │                                                                    │
  │  ┌──────────────────────────────────────────────────────────────┐  │
  │  │  Gossip 协议 (Cluster Bus, port = client-port + 10000)       │  │
  │  │                                                              │  │
  │  │  每个节点定期向随机节点发送 PING/PONG                         │  │
  │  │  携带: 节点状态、slot 分配、故障信息                           │  │
  │  │  传播速度: O(log N) 轮次                                     │  │
  │  │                                                              │  │
  │  │  ┌────┐  PING  ┌────┐  PING  ┌────┐                         │  │
  │  │  │ A  │───────►│ B  │───────►│ C  │                         │  │
  │  │  │    │◄───────│    │◄───────│    │                         │  │
  │  │  │    │  PONG   │    │  PONG   │    │                         │  │
  │  │  └────┘        └────┘        └────┘                         │  │
  │  └──────────────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────────────┘
```

**Cluster 关键特性**：
- **无中心协调**：每个节点都知道完整的 slot 分配和集群拓扑
- **数据分片**：`slot = CRC16(key) % 16384`
- **Hash Tag**：`{user:1001}.name` 和 `{user:1001}.email` 会路由到同一 slot（花括号内的内容计算 hash）
- **槽迁移**：在线迁移 slot，使用 ASK 重定向 + MIGRATE 命令
- **最小规模**：至少 3 个 Master 节点（保证故障转移）
- **不支持的操作**：多 key 操作需在同一 slot（或用 hash tag）、事务仅限于单节点

#### 5. Lua 脚本

```
  ┌──────────────────────────────────────────────────────┐
  │  Lua 脚本执行                                        │
  │                                                      │
  │  EVAL "return redis.call('GET', KEYS[1])" 1 mykey    │
  │                                                      │
  │  特性:                                               │
  │  - 原子执行（脚本执行期间不处理其他命令）              │
  │  - 可组合多个 Redis 命令                             │
  │  - 支持条件判断、循环                                 │
  │  - 脚本缓存（SCRIPT LOAD + EVALSHA）                 │
  │  - Redis 7+ 支持函数库 (FUNCTION LOAD)               │
  │                                                      │
  │  经典用例:                                           │
  │  - 分布式锁 (SETNX + EXPIRE 原子操作)                │
  │  - 限流 (滑动窗口计数器)                             │
  │  - 复杂数据结构操作                                   │
  └──────────────────────────────────────────────────────┘
```

#### 6. Pub/Sub 与 Streams

```
  Pub/Sub (发布/订阅):
  ┌──────────┐    "channel:news"     ┌──────────┐
  │ PUBLISH  │ ────────────────────► │ Subscriber│
  │          │                       │   A       │
  │          │                       └──────────┘
  │          │                       ┌──────────┐
  │          │ ────────────────────► │ Subscriber│
  │          │                       │   B       │
  └──────────┘                       └──────────┘

  特点: 火后不管 (fire-and-forget)，消息不持久化
        无消费者确认，无消息回溯
        适合实时通知、聊天、配置推送

  Streams (Redis 5+):
  ┌───────────────────────────────────────────────────────────┐
  │  Stream: "orders"                                         │
  │                                                           │
  │  1640000000000-0  {"user":"alice", "item":"book"}         │
  │  1640000000100-0  {"user":"bob",   "item":"pen"}          │
  │  1640000000200-0  {"user":"carol", "item":"phone"}        │
  │  ...                                                      │
  │                                                           │
  │  消费者组 (Consumer Groups):                              │
  │  XGROUP CREATE orders group1 0                            │
  │                                                           │
  │  ┌──────────┐  ┌──────────┐                               │
  │  │ Consumer │  │ Consumer │  ← 同组内消息不重复消费        │
  │  │    C1    │  │    C2    │    不同组独立消费同一消息      │
  │  └──────────┘  └──────────┘                               │
  │                                                           │
  │  特性: 持久化、消费者组、ACK、Pending 列表、阻塞读取       │
  │        可视为轻量级 Kafka/RabbitMQ                        │
  └───────────────────────────────────────────────────────────┘
```

#### 7. Redis on Flash (RoF) — 分层存储

```
  ┌──────────────────────────────────────────────────────┐
  │  Redis on Flash (企业版 / Redis Stack)                │
  │                                                      │
  │  热点数据 (频繁访问)                                  │
  │  ┌──────────────────────────────────┐                │
  │  │         RAM (内存)               │                │
  │  │  ┌────────┐ ┌────────┐          │                │
  │  │  │ Key A  │ │ Key B  │  ← 完整   │                │
  │  │  │ Value  │ │ Value  │    值在内存│                │
  │  │  └────────┘ └────────┘          │                │
  │  └──────────────────────────────────┘                │
  │                                                      │
  │  冷数据 (低频访问)                                    │
  │  ┌──────────────────────────────────┐                │
  │  │         Flash / SSD              │                │
  │  │  ┌────────┐ ┌────────┐          │                │
  │  │  │ Key C  │ │ Key D  │  ← 值在   │                │
  │  │  │ Value  │ │ Value  │    Flash │                │
  │  │  │(仅索引  │ │(仅索引  │    Key   │                │
  │  │  │ 在内存) │ │ 在内存) │    索引  │                │
  │  │  └────────┘ └────────┘    在内存 │                │
  │  └──────────────────────────────────┘                │
  │                                                      │
  │  Key 始终在内存，Value 可分层存储                      │
  │  自动冷热分层，对应用透明                              │
  └──────────────────────────────────────────────────────┘
```

#### 8. 过期键删除策略

```
  双重策略:

  被动删除 (Lazy Expire):
    - 访问 key 时检查 TTL
    - 如果过期则删除并返回 nil
    - 优点: 无额外开销
    - 缺点: 过期 key 可能长期滞留内存

  主动删除 (Active Expire):
    - 定期抽样检查 (serverCron 中 activeExpireCycle)
    - 每次抽样 20 个带 TTL 的 key
    - 删除过期的，直到过期比例 < 25%
    - 避免 CPU 阻塞（渐进式）

  AOF/RDB 中的过期:
    - RDB: 过期 key 不写入快照
    - AOF: 过期 key 写入 DEL 命令
    - 重写 AOF 时: 过期 key 不写入
```

---

### 四、CAP / PACELC 分析

#### 单机模式 (Standalone)

单机 Redis 不涉及分布式 CAP 问题——它是单节点系统。

| 维度 | 说明 |
|------|------|
| **数据一致性** | 强一致（单副本，单线程串行） |
| **可用性** | 取决于单机可靠性（进程崩溃即不可用） |
| **分区容忍** | N/A（无分布式） |

#### 主从复制 (Master-Replica)

| CAP 维度 | 选择 | 说明 |
|----------|------|------|
| **Consistency** | ✗ 牺牲 | 异步复制，Replica 数据可能落后 |
| **Availability** | ✓ 保证 | Master 故障时（无 Sentinel）只读不可写 |
| **Partition Tolerance** | 部分 | 网络分区时 Master 继续服务 |

#### Redis Cluster

| CAP 维度 | 选择 | 说明 |
|----------|------|------|
| **Consistency** | ✗ 最终一致 | 异步复制 + 无强一致性协议 |
| **Availability** | ✓ 保证 | 多数 Master 存活即可服务 |
| **Partition Tolerance** | ✓ 保证 | 分区容忍，通过 Gossip 传播 |

**CAP 定位：AP-leaning**

Redis Cluster 在 CAP 中偏向 **AP**——保证可用性和分区容忍，牺牲强一致性。

```
  ┌──────────────────────────────────────────────────────────────┐
  │  CAP 定位: Redis Cluster = AP-leaning                        │
  │                                                              │
  │  写入路径:                                                   │
  │  Client ──► Master ──► 立即返回 OK                          │
  │                  │                                          │
  │                  └──► 异步复制 ──► Replica                  │
  │                                                              │
  │  问题: 写入返回后，Replica 尚未同步                            │
  │  后果: 如果 Master 立刻宕机，最后几条写入丢失                  │
  │                                                              │
  │  半同步写 (Redis 7+ WAIT / WAITAOF):                         │
  │  Client ──► Master ──► 等待 N 个 Replica 确认                │
  │                  │                                          │
  │                  └──► 等待 AOF 刷盘                          │
  │                                                              │
  │  可提升一致性保证，但增加延迟                                  │
  └──────────────────────────────────────────────────────────────┘
```

#### PACELC 分析

| 场景 | Redis Cluster 行为 |
|------|-------------------|
| **Partition (P)** | 选择 **Availability** — 存活节点继续服务，丢失少数 Master 槽位则对应 key 不可用 |
| **Else (E) — 正常运行时** | 选择 **Latency** — 内存操作亚毫秒延迟，异步复制不阻塞写入 |

```
  PACELC 矩阵:

  ┌──────────────┬──────────────────────────────────┐
  │  网络正常 (E)│  网络分区 (P)                     │
  ├──────────────┼──────────────────────────────────┤
  │  EL: 选延迟  │  PA: 选可用性                     │
  │              │                                   │
  │  内存操作    │  异步复制，不阻塞                  │
  │  亚毫秒延迟  │  Master 故障时可选举               │
  │              │  部分 key 可能暂时不可用            │
  │              │  (其 Master 在分区另一侧)           │
  └──────────────┴──────────────────────────────────┘

  结论: Redis Cluster = PA/EL
```

**一致性增强手段**：

| 手段 | 说明 | 代价 |
|------|------|------|
| `WAIT N` | 等待 N 个 Replica 确认写入 | 增加写延迟 |
| `WAITAOF` | 等待 AOF 刷盘完成 | 增加写延迟 |
| `MIN-REPLICAS-TO-WRITE` | 少于 N 个 Replica 时拒绝写入 | 降低可用性 |
| 事务 `MULTI/EXEC` | 命令队列原子执行 | 仅单节点，无跨节点回滚 |

---

### 五、历史影响与遗产

#### 时间线

| 年份 | 事件 |
|------|------|
| 2009 | Salvatore Sanfilippo 发布 Redis 1.0，最初为 LLOOGG 日志聚合工具的 KV 后端 |
| 2010 | VMware 雇佣 antirez 全职开发 Redis |
| 2011 | Redis 2.2：引入复制改进、Lua 脚本原型 |
| 2013 | Redis 2.6：Lua scripting、Sentinel 1.0 |
| 2014 | Redis 2.8：部分同步（PSYNC）、Sentinel 2.0 |
| 2015 | Redis 3.0：Redis Cluster 正式发布 |
| 2016 | RedisLabs 成立（现 Redis, Inc.），antirez 加入 |
| 2017 | Redis 4.0：模块系统 (Modules)、混合持久化、Lazy Free |
| 2018 | Redis 5.0：Streams、RDB-AOF 混合持久化改进 |
| 2020 | Redis 6.0：I/O 多线程、ACL 系统、RESP3、SSL/TLS |
| 2021 | Redis 7.0：函数库 (Functions)、INTROSPECTION 命令、ACL 改进 |
| 2023 | Redis 7.2、8.0：性能优化、新数据结构 |
| 2024 | 许可证变更（BSD → RSALv2/SSPLv1），社区 fork Valkey 成立 |

#### 核心影响领域

**1. 缓存标准**
- 几乎成为"分布式缓存"的代名词
- 替代 Memcached 成为主流选择（更丰富的数据结构 + 持久化）
- Spring Cache、Django Cache、Laravel Cache 等框架的默认 Redis 后端

**2. 实时数据处理**
- 发布/订阅 → 实时通知、配置推送
- Streams → 轻量级消息队列、事件溯源
- HyperLogLog → 实时 UV 统计
- Bitmap → 实时活跃用户分析

**3. 游戏排行榜**
- Sorted Set（ZSet）天然适配排行榜场景
- `ZADD score user` / `ZRANGE 0 9` / `ZRANK user` → 插入、Top-N、排名
- 几乎所有在线游戏的排行榜底层

**4. Session 存储**
- 高并发读写、TTL 自动过期、水平扩展
- Express session、Django session、PHP session 的 Redis 后端

**5. 分布式锁**
- `SET key value NX EX seconds` → 简单分布式锁
- Redlock 算法（多实例共识锁）— 有争议但广泛使用
- 分布式任务调度、资源互斥访问

**6. 全文搜索/推荐**
- RedisJSON + RediSearch → JSON 文档存储 + 全文索引
- RedisGraph → 图数据库（Cypher 查询）
- RedisAI → 机器学习推理

#### 衍生生态

| 项目 | 关系 |
|------|------|
| **KeyDB** | 多线程 Redis fork，兼容 Redis 协议 |
| **Dragonfly** | 现代多线程兼容 Redis 协议的内存数据库 |
| **Valkey** | Linux Foundation 项目，BSD 许可证下的 Redis 7.2.4 fork |
| **Garnet** | Microsoft Research 的兼容 RESP 的缓存存储 |
| **SSDB** | 基于 LevelDB 的持久化 Redis 兼容存储 |
| **Kvrocks** | 基于 RocksDB 的 Redis 兼容 KV 存储（Apache 项目） |
| **RedisJSON/RediSearch/RedisGraph/RedisTimeSeries** | Redis 官方模块扩展 |

#### 设计哲学的影响

1. **Keep it Simple**：单线程模型证明了简单架构可以击败复杂并发模型
2. **数据结构即 API**：将数据结构直接暴露为网络服务，而非通过 SQL 抽象
3. **内存优先**：在 SSD 普及之前，Redis 坚持"内存才是数据库"的理念，推动了内存数据库的产业化
4. **渐进式改进**：从简单的 KV 存储逐步添加数据结构、持久化、集群、模块——每个版本都向后兼容
5. **模块系统**：Redis Modules 让社区可以在核心之上构建新功能（搜索、图、JSON、时序），无需修改核心

---

## 关键参数总结

| 参数 | 符号/配置 | 典型值 | 说明 |
|------|----------|--------|------|
| 单实例 QPS | - | 10万-20万+ | 取决于操作类型和硬件 |
| 单实例内存 | maxmemory | 按需配置 | 建议不超过物理内存 75%（留 fork COW 空间） |
| 复制模式 | - | 异步 | 主从异步复制，可配半同步 |
| Cluster Slot 数 | - | 16384 | CRC16(key) % 16384 |
| Cluster 最小节点 | - | 3 Master | 需 Sentinel 或 Cluster 管理 |
| RDB 快照触发 | save | 900 1 / 300 10 / 60 10000 | 按变更频率调整 |
| AOF fsync | appendfsync | everysec | 安全与性能平衡 |
| 内存淘汰 | maxmemory-policy | noeviction / allkeys-lru | 按场景选择 |
| Hash 阈值 | hash-max-ziplist-entries | 512 | ziplist → hashtable 切换阈值 |
| List 节点大小 | list-max-quicklist-size | 2 (KB) | quicklist 节点大小 |
| ZSet 阈值 | zset-max-ziplist-entries | 128 | ziplist → skiplist 切换阈值 |
| I/O 线程数 | io-threads | 4 (Redis 6+) | 默认关闭，网络密集型建议开启 |
| Lua 超时 | lua-time-limit | 5000ms | 防止脚本阻塞 |

---

## 局限与教训

1. **内存限制**：数据必须（主要）驻留在内存中，大数据集成本高。虽然 Redis on Flash 提供了分层方案，但属于企业版功能
2. **单线程瓶颈**：虽然单线程避免了锁竞争，但也意味着单个重操作（如 `KEYS *`、大集合操作）会阻塞整个实例。Redis 7+ 引入了部分命令的异步执行
3. **大 Key 问题**：单个 key 的值过大（如百万成员的 ZSet）会导致单命令延迟飙升，阻塞后续请求。需要业务层拆分或懒加载
4. **异步复制丢数据**：主从异步复制在 Master 宕机时可能丢失最后几毫秒到几秒的写入
5. **脑裂风险**：Sentinel 故障转移时，如果旧 Master 未感知自己被替换，可能出现两个 Master 同时写入（网络分区场景）
6. **事务局限性**：`MULTI/EXEC` 不支持回滚（即使某个命令失败，其他命令仍执行），且仅限于单节点
7. **跨节点操作不支持**：Redis Cluster 中无法对跨 slot 的 key 执行原子操作（`MGET`/`MSET`/事务都不支持跨 slot）
8. **许可证变更风险**：2024 年许可证从 BSD 变为 RSALv2/SSPLv1，对商业用户和云服务提供商带来合规风险。社区通过 Valkey fork 应对
9. **持久化 COW 开销**：RDB 快照和 AOF 重写都依赖 `fork()`，写时复制会消耗相当于数据量大小的额外内存。在内存紧张的服务器上可能触发 OOM
10. **缓存穿透/击穿/雪崩**：作为缓存使用时，需要业务层额外处理缓存穿透（Bloom Filter）、缓存击穿（互斥锁/逻辑过期）、缓存雪崩（随机过期时间）

---

## 与其他系统的对比定位

```
  ┌──────────────────────────────────────────────────────────────┐
  │  Redis 在存储生态中的位置                                     │
  │                                                              │
  │  按数据模型:                                                  │
  │                                                              │
  │  关系型      → MySQL / PostgreSQL / TiDB                     │
  │  文档型      → MongoDB / CouchDB                              │
  │  列族型      → Cassandra / HBase                              │
  │  图数据库    → Neo4j / RedisGraph                             │
  │  内存/KV     → Redis / Memcached / Dragonfly / Garnet        │
  │  搜索引擎    → Elasticsearch / RediSearch                     │
  │  时序数据库  → InfluxDB / RedisTimeSeries                     │
  │                                                              │
  │  按使用模式:                                                  │
  │                                                              │
  │  主存储 (Source of Truth)                                     │
  │    ↓                                                          │
  │  Redis 作为缓存 / 会话存储 / 排行榜 / 消息队列 / 实时统计     │
  │    ↓                                                          │
  │  持久化存储 (MySQL / PostgreSQL / Cassandra)                  │
  │                                                              │
  │  Redis 通常不是"主存储"，而是架构中的高速层                    │
  │  （除非使用 Redis on Flash 或持久化模块）                      │
  └──────────────────────────────────────────────────────────────┘
```
