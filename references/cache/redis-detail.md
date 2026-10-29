# Redis 深度分析

---

## 一、Redis 概述

Redis（Remote Dictionary Server）是一个开源的、基于内存的、键值对（key-value）数据存储系统，由 Salvatore Sanfilippo（antirez）于 2009 年创建。它兼具数据库、缓存、消息队列和流处理引擎的功能，是目前最广泛使用的内存数据库之一。

| 属性 | 说明 |
|------|------|
| 类型 | 内存键值存储 |
| 许可证 | RSALv2 / SSPL（7.4 之后） |
| 开发语言 | C |
| 默认端口 | 6379 |
| 作者 | Salvatore Sanfilippo |
| 当前主版本 | 7.x |

---

## 二、数据结构与底层实现

### 2.1 核心数据结构

| 数据类型 | 底层实现 | 时间复杂度 | 适用场景 |
|----------|---------|-----------|---------|
| String | SDS (Simple Dynamic String) | O(1) 增改查 | 缓存、计数器、分布式锁 |
| List | QuickList (3.2+)，早期为 ziplist + linkedlist | O(1) 头尾操作 | 消息队列、最新消息列表 |
| Hash | ziplist / hashtable (dict) | O(1) ~ O(n) | 对象存储、属性映射 |
| Set | intset / hashtable | O(1) 添加删除 | 标签、去重、集合运算 |
| ZSet (Sorted Set) | ziplist / skiplist + dict | O(log n) | 排行榜、延时队列 |
| Stream | radix tree + listpack | O(1) 追加 | 消息流、事件日志 |
| Bitmap | String + 位操作 | O(1) | 布隆过滤器、签到统计 |
| HyperLogLog | 稠密/稀疏表示 | O(1) | 去估计基数（UV 统计） |
| Geo | ZSet + GeoHash | O(log n) | 附近搜索、位置服务 |

### 2.2 关键底层结构详解

**SDS（Simple Dynamic String）**
```c
struct sdshdr {
    int len;       // 已使用长度
    int free;      // 剩余空间
    char buf[];    // 字符数组
}
```
- **二进制安全**：不以 `\0` 判断结束，支持任意二进制数据
- **预分配策略**：扩容时额外分配空间，减少多次 realloc
- **惰性释放**：缩短时不立即释放内存

**Skiplist（跳表）**
- 平均 O(log n) 的查找/插入/删除
- 通过多级索引实现快速跳跃
- 每个节点随机决定层数（期望层数 ≈ 1.33）
- ZSet 中同时维护 skiplist 和 dict：跳表负责排序，dict 负责 O(1) 查分值

**QuickList**
- 3.2 版本统一 List 底层实现
- 双向链表 + 每个节点是 ziplist/listpack 的组合
- 兼顾内存紧凑（小数据连续存储）和操作效率（链表快速增删）

---

## 三、内存管理

### 3.1 内存策略

| 策略 | 说明 |
|------|------|
| maxmemory | 限制最大可用内存 |
| 淘汰策略 | 8 种 eviction policy |
| 过期机制 | 惰性删除 + 定期删除 |
| 内存碎片 | 自动 defrag（4.0+ activedefrag） |

### 3.2 过期键删除机制

- **定时删除（不采用）**：为每个键创建定时器，CPU 开销大
- **惰性删除（Redis 采用）**：访问键时检查是否过期，过期则删除。CPU 友好，但可能积压大量过期未访问的键
- **定期删除（Redis 采用）**：每秒执行 10 次，随机抽取检查。如果过期比例 > 25%，继续扫描。限制每次扫描时间，避免阻塞

### 3.3 内存淘汰策略（8 种）

| 策略 | 范围 | 说明 |
|------|------|------|
| `noeviction` | — | 不淘汰，写入报错（默认） |
| `allkeys-lru` | 所有键 | 淘汰最近最少使用 |
| `allkeys-lfu` | 所有键 | 淘汰最不常用（4.0+） |
| `allkeys-random` | 所有键 | 随机淘汰 |
| `volatile-lru` | 设过期的键 | LRU 淘汰 |
| `volatile-lfu` | 设过期的键 | LFU 淘汰（4.0+） |
| `volatile-random` | 设过期的键 | 随机淘汰 |
| `volatile-ttl` | 设过期的键 | 优先淘汰即将过期的 |

---

## 四、持久化机制

### 4.1 RDB（Redis Database）

- `SAVE/BGSAVE` 触发，fork 子进程生成二进制快照 dump.rdb
- Copy-On-Write 保证不阻塞主进程

**优点**：文件紧凑，适合备份和灾难恢复；恢复速度快于 AOF
**缺点**：两次快照间宕机会丢失数据；fork 在大内存实例下较慢

### 4.2 AOF（Append Only File）

追加写命令到 appendonly.aof（文本协议），三种 fsync 策略：
- `always`：每次写入都 fsync
- `everysec`：每秒 fsync（推荐）
- `no`：由 OS 决定

**AOF 重写（Rewrite）**：遍历内存数据，用最小命令集生成新 AOF，去除冗余命令（如已删的 key、已覆盖的值）。fork 子进程执行，主进程通过管道增量发送。

### 4.3 RDB + AOF 混合持久化（4.0+）

AOF 重写时，全量部分以 RDB 格式写入（快速加载），增量部分以 AOF 命令格式追加（保证完整性）。兼顾快速加载和数据完整性。

---

## 五、事件模型与网络框架

### 5.1 事件驱动架构

Redis 是单线程事件驱动模型，事件循环包含：
- **IO 多路复用**（epoll）：监听连接读写事件
- **时间事件处理器**：定时任务（如过期键清理、统计）
- **事件分派器**：连接响应、命令请求、关闭连接等

### 5.2 IO 多路复用

| 接口 | 说明 |
|------|------|
| select() | 性能差，最多 1024 个 fd |
| poll() | 无数量限制，但仍 O(n) 遍历 |
| epoll() | O(1)，Linux 首选 |
| kqueue() | BSD/macOS |

### 5.3 多线程 IO（6.0+）

命令解析和执行仍在主线程，仅网络读写并行化。需要配置 `io-threads` 启用。适合高并发连接、大数据量传输场景。

---

## 六、复制机制（Replication）

### 6.1 主从复制流程

1. Slave 发起 `PSYNC2` 同步请求
2. **全量同步**：Master 执行 BGSAVE 生成 RDB → 发送给 Slave → 缓冲区写入命令也发送
3. **部分重同步**：根据 offset 从 replication backlog 补发缺失命令
4. 持续发送增量写命令

### 6.2 复制积压缓冲区

- 固定大小的环形缓冲区，记录最近写入的命令
- 断连重连时判断是否可以部分同步
- 建议大小：预估断连时间 × 写入速率

---

## 七、哨兵模式（Sentinel）

### 核心功能

| 功能 | 说明 |
|------|------|
| 监控 | 检测主从节点是否存活 |
| 通知 | 通过 API/发布订阅通知运维 |
| 自动故障转移 | Master 宕机自动提升 Slave |
| 配置提供者 | 客户端从 Sentinel 获取主从地址 |

### 故障转移流程

1. **主观下线 (SDOWN)**：单个 Sentinel 认为节点不可达
2. **客观下线 (ODOWN)**：超过 quorum 个 Sentinel 确认 SDOWN
3. 选举 Leader Sentinel（Raft 变体）
4. Leader 选择最优 Slave 提升为 Master（优先级最高 → 复制 offset 最大 → runid 最小）
5. 其他 Slave 指向新 Master
6. 旧 Master 恢复后降级为 Slave

---

## 八、Cluster 模式

### 8.1 数据分片

Redis Cluster 使用 **16384 个哈希槽** 进行数据分片：

```
slot = CRC16(key) mod 16384

Node A: 0-5460     (5461个槽)
Node B: 5461-10922  (5462个槽)
Node C: 10923-16383 (5461个槽)
```

### 8.2 Gossip 协议

- **PING/PONG**：携带本节点已知的集群信息，每秒向随机几个节点发送
- **MEET**：新节点加入集群
- **FAIL**：广播某节点已下线

### 8.3 槽位迁移

1. 源节点标记 slot 为 MIGRATING
2. 目标节点标记 slot 为 IMPORTING
3. 逐个迁移 slot 中的 key（MIGRATE 命令）
4. 迁移完成，广播 `CLUSTER SETSLOT NODE <target>`
5. 全集群更新路由表

---

## 九、事务与 Lua 脚本

### 9.1 MULTI/EXEC 事务

- `MULTI` 开始事务，后续命令入队
- `EXEC` 原子执行所有命令
- ✅ 顺序执行，不被打断
- ✅ 全部执行或全部不执行
- ❌ **不支持回滚**
- `WATCH` 实现乐观锁：被监视的 key 被修改则事务放弃

### 9.2 Lua 脚本

原子性执行，不会被其他命令打断。`EVALSHA` 缓存脚本减少网络开销。

```lua
-- 原子性计数器
local current = redis.call('GET', KEYS[1])
if current == false then current = 0
else current = tonumber(current) end
current = current + 1
redis.call('SET', KEYS[1], current)
return current
```

**Function（7.0+）**：替代 EVAL/EVALSHA 的持久化函数，支持注册和管理脚本。

---

## 十、高可用架构对比

| 模式 | 最小节点 | 分片 | 自动故障转移 | 适用场景 |
|------|---------|------|-------------|---------|
| 单机 | 1 | ❌ | ❌ | 开发/测试 |
| 主从 | 2 | ❌ | ❌ | 读写分离 |
| Sentinel | 3(S)+1(M)+1(S) | ❌ | ✅ | 高可用无分片 |
| Cluster | 6 (3M+3S) | ✅ | ✅ | 高可用+水平扩展 |

---

## 十一、性能优化要点

### 常见性能问题与优化

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| 慢查询 | KEYS、SUNION 等 | 用 SCAN 替代 |
| 大 key | 单个 key 过大 | 拆分、压缩 |
| 热 key | 集中访问某 key | 本地缓存、读写分离 |
| 内存碎片 | 频繁修改/删除 | 开启 active-defrag |
| fork 慢 | 内存太大 | 调小或用 AOF everysec |
| 网络瓶颈 | 大量小包 | Pipeline 批量 |
| AOF rewrite 慢 | 数据量大 | 混合持久化 |
| 持久化阻塞 | fsync 阻塞 | everysec + no-appendfsync-on-rewrite |
| 主从延迟 | 网络带宽 | 压缩复制、调整 repl-backlog |

### Pipeline

无 Pipeline：3 次命令 = 3 次 RTT
Pipeline：3 次命令打包发送 = 1 次 RTT，大幅减少网络延迟。

### 大 Key 诊断

```bash
redis-cli --bigkeys          # 查找大 key
redis-cli --memkeys          # 查找内存占用
MEMORY USAGE key             # 分析单个 key
redis-cli --latency          # 延迟监控
```

### 关键配置优化

```conf
# 内存
maxmemory 8gb
maxmemory-policy allkeys-lru

# AOF
appendonly yes
appendfsync everysec
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
no-appendfsync-on-rewrite yes

# 网络
tcp-keepalive 60
tcp-backlog 511
timeout 300

# 复制
repl-backlog-size 256mb
repl-timeout 60
client-output-buffer-limit replica 256mb 64mb 60

# 慢日志
slowlog-log-slower-than 10000
slowlog-max-len 128
```

---

## 十二、安全机制

### 认证与访问控制

- **AUTH 密码认证**（requirepass）
- **ACL（6.0+）**：按用户分配不同权限，限制可执行命令和可访问 key
- **命令重命名/禁用**（rename-command）
- **TLS 支持（6.0+）**：支持加密通信

### ACL 示例

```
ACL SETUSER readonly_user on >password123 ~readonly:* +get +mget +hget
ACL SETUSER default -@dangerous
ACL LIST
ACL WHOAMI
```

---

## 十三、Redis 应用场景

| 场景 | 数据结构 | 说明 |
|------|---------|------|
| 缓存 | String + 过期 | 最常见用法 |
| 分布式锁 | String + SET NX | `SET key value NX PX ms` |
| 计数器 | String + INCR/DECR | 原子操作 |
| 消息队列 | List + LPUSH/BRPOP | 简单队列 |
| 延时队列 | ZSet + score=到期时间 | 扫描到期任务 |
| 排行榜 | ZSet + ZRANGE/ZREVRANGE | 实时排名 |
| 限流 | ZSet/String + Lua | 滑动窗口限流 |
| 位图统计 | Bitmap | 签到、活跃用户统计 |
| 地理位置服务 | GEO | 附近的人、门店搜索 |
| 发布订阅 | Pub/Sub | 实时消息广播 |
| 事件流 | Stream | 持久化消息流 |
| 布隆过滤 | RedisBloom 模块 | 缓存穿透防护 |

### 分布式锁实现

```
加锁：SET lock_key unique_value NX PX 30000

解锁（Lua 保证原子性）：
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
```

注意事项：必须设置过期时间避免死锁；value 必须唯一避免误删；业务执行时间 < 锁过期时间；推荐使用 Redlock（多节点锁）。

---

## 十四、Redis 7.x 新特性

| 特性 | 说明 |
|------|------|
| Functions | 持久化脚本，替代 EVAL/EVALSHA |
| Multi-part AOF | AOF 拆分为 base + incr 文件 |
| ACL v2 | 增强的权限控制 |
| Sharded Pub/Sub | 按槽位分片的发布订阅 |
| 新增命令 | CLUSTER LINKS、LATENCY 等 |
| 性能提升 | 进一步减少内存占用，提升 RDB 加载速度 |

---

## 十五、Redis 生态与工具

| 工具 | 用途 |
|------|------|
| redis-cli | 命令行客户端 |
| redis-benchmark | 性能基准测试 |
| redis-sentinel | 哨兵进程 |
| RedisInsight | 可视化管理工具 |
| Redis Exporter | Prometheus 指标采集 |
| RedisShake | 数据迁移/同步 |
| RedisBloom | 布隆过滤器模块 |
| RedisGraph | 图数据库模块 |
| RedisJSON | JSON 文档支持 |
| RedisSearch | 全文搜索引擎 |

---

## 十六、Redis 与图存储的关系

> 结合图存储团队工作进行延伸分析

### Redis 在图存储场景的潜在角色

| 角色 | 方式 | 优势 | 局限 |
|------|------|------|------|
| 图查询缓存 | 缓存热点子图/查询结果 | 降低后端图数据库压力 | 一致性维护 |
| 图元数据存储 | 存储图 schema、节点属性 | 快速读取 | 内存成本 |
| 图索引辅助 | Set/ZSet 维护邻接关系 | O(1)/O(log n) 查找 | 不适合大规模图遍历 |
| 会话/事务状态 | 存储图查询事务状态 | 分布式会话 | 需要过期清理 |
| 限流与保护 | 对图数据库查询限流 | 防止过载 | — |

### Redis 对比图数据库

| 维度 | Redis | 图数据库 (Neo4j/JanusGraph 等) |
|------|-------|-------------------------------|
| 数据模型 | KV + 数据结构 | 节点 + 边 + 属性 |
| 图遍历 | 需多次查询，O(n) 级别 | 原生图遍历，O(1) 邻居访问 |
| 关系存储 | Set/Hash 模拟 | 原生支持 |
| 多跳查询 | 性能差 | 优化 |
| 事务 | 简单 | 完整 ACID |
| 持久化 | RDB/AOF | WAL + 存储引擎 |
| 扩展性 | Cluster 分片 | 分布式图引擎 |
| 适用规模 | 中小图查询缓存 | 大规模图存储与查询 |

---

## 十七、监控与运维

### 关键监控指标

**基础指标**：connected_clients、used_memory_rss、used_memory:peak、uptime_in_seconds、rejected_connections

**性能指标**：instantaneous_ops_per_sec (QPS)、hit_rate (缓存命中率)、keyspace_hits/misses、latest_fork_usec、rdb_last_bgsave_time_sec

**延迟指标**：avg_latency、慢查询数量 (SLOWLOG LEN)、blocked_clients (BLPOP 等)

**复制指标**：connected_slaves、repl_backlog_size、每个从节点的 lag (master_repl_offset)

### INFO 命令分类

```bash
INFO server        # 服务器信息
INFO clients       # 客户端连接
INFO memory        # 内存使用
INFO persistence   # 持久化
INFO stats         # 统计
INFO replication   # 复制
INFO cpu           # CPU
INFO commandstats  # 命令统计
INFO keyspace      # 键空间
INFO cluster       # 集群
```

### 常见故障排查

| 现象 | 可能原因 | 排查命令 |
|------|---------|---------|
| 响应慢 | 慢查询、大 key | `SLOWLOG GET`, `--bigkeys` |
| 内存满 | 淘汰策略不当 | `INFO memory` |
| 连接拒绝 | maxclients 满 | `INFO clients` |
| 主从不一致 | 网络分区 | `INFO replication` |
| AOF 损坏 | 异常宕机 | `redis-check-aof --fix` |
| 集群脑裂 | 网络分区 | `CLUSTER NODES` |
| RDB 加载慢 | 文件太大 | `--rdb-check` |

---

## 十八、总结

Redis 核心优势：

1. **丰富的数据结构** — 覆盖绝大多数业务场景需求
2. **高性能** — 单线程事件驱动 + 高效数据结构，10万+ QPS
3. **高可用** — 主从复制 + Sentinel + Cluster 多级保障
4. **灵活的持久化** — RDB/AOF/混合，平衡安全与性能
5. **持续演进** — 从单线程到多线程 IO，从 EVAL 到 Functions

在图存储团队的工作中，Redis 可以作为图数据库的有力辅助：缓存加速、会话管理、限流保护、元数据快速访问等。合理使用 Redis 可以显著提升图存储系统的整体响应能力。

---

*文档生成时间：2026-10-29 11:33 GMT+8*
