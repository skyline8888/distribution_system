# SQL → NoSQL 存储系统演进总览

## 演进脉络

```
关系型 (1970s) → 分库分表中间件 (2008) → NoSQL (2009) → NewSQL (2012) → 多模融合 (2018+)
```

## 核心驱动力

1. **数据规模爆炸**：单表百万 → 亿级 → 万亿级
2. **访问模式变化**：OLTP → 混合负载 (HTAP) → 实时分析
3. **扩展需求**：垂直扩展 → 水平分片 → 透明分布式
4. **一致性需求分化**：强一致 → 最终一致 → 可调一致性 → 全局强一致

## 技术演进主线

### 扩展性
- 垂直扩展 → 主从复制 → 分库分表 (手动) → 自动分片 → 透明分布式

### 事务模型
- 本地 ACID → 分布式事务 (2PC) → BASE/最终一致 → 分布式 ACID (Percolator/Spanner)

### 数据模型
- 关系表 → 键值/文档/列族/图 → 多模型统一

### 共识协议
- 主从复制 → Paxos → Raft → Multi-Paxos → TrueTime / Hybrid Logical Clock

## 时代划分

### 第一代：传统关系型数据库 (1970s-2008)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
| Oracle | 1979 | 商业 RDBMS 标杆、PL/SQL、RAC |
| MySQL | 1995 | 开源轻量、InnoDB 引擎、主从复制 |
| PostgreSQL | 1996 | MVCC、SQL 标准支持、可扩展类型系统 |

**特点**：ACID、SQL、单机垂直扩展，主从读扩展

### 第二代：分库分表中间件 (2008-2012)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
| Cobar | 2008 | 阿里早期分库分表中间件 |
| MyCat | 2013 | 基于 Cobar 演进，路由/分片/读写分离 |
| Vitess | 2011 | YouTube 开源、vindex 抽象、在线 re-shard |
| ShardingSphere | 2016 | Apache 孵化、生态完整、多语言 |

**特点**：在现有 RDBMS 之上做水平扩展，应用层/中间件层分片

### 第三代：NoSQL 多样化 (2009-2015)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
| Dynamo | 2007 | 论文：去中心化 KV、一致性哈希、Vector Clock |
| BigTable | 2006 | 论文：分布式结构化存储、SSTable |
| MongoDB | 2009 | 文档存储、BSON、动态 schema |
| Cassandra | 2008 | Dynamo + BigTable、去中心化、LSM-Tree |
| Redis | 2009 | 内存数据结构、丰富数据类型、单线程模型 |
| DynamoDB | 2012 | AWS 托管、Serverless、自动扩展 |

**特点**：放弃强一致/SQL，换取水平扩展和高可用，CAP 取舍明显

### 第四代：NewSQL 回归 (2012-2020)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
| Spanner | 2012 | 全球分布式、TrueTime、Paxos、外部一致性 |
| CockroachDB | 2015 | 开源 Spanner 替代、Raft、多区域部署 |
| TiDB | 2016 | 兼容 MySQL、PD 调度、Raft + Percolator 事务 |
| YugabyteDB | 2017 | PostgreSQL 兼容、Raft、分布式事务 |

**特点**：分布式 + ACID + SQL，试图兼得

### 第五代：多模融合与云原生 (2018+)

| 系统 | 年份 | 核心创新 |
|------|------|----------|
| OceanBase | 2010/2017 开源 | 原生分布式、Paxos、HTAP、TPC-C 记录 |
| PolarDB | 2017 | 阿里云、存算分离、共享存储 |
| Doris/StarRocks | 2018/2020 | MPP 分析、实时 OLAP、向量化执行 |
| Snowflake | 2015 | 云原生数仓、存算分离、多集群共享 |

**特点**：HTAP、存算分离、云原生、多引擎融合

## 关键技术对比

| 维度 | RDBMS (MySQL) | NoSQL (Cassandra) | NewSQL (TiDB) | 多模 (OceanBase) |
|------|---------------|-------------------|---------------|------------------|
| 数据模型 | 关系表 | 列族/文档 | 关系表 | 关系表 |
| 扩展方式 | 主从/分片 | 自动分片 | 透明分布式 | 原生分布式 |
| 一致性 | 强一致 | 最终/可调 | 强一致 | 强一致 |
| 事务 | 本地 ACID | 轻量/无 | 分布式 ACID | 分布式 ACID |
| 共识协议 | 半同步/组复制 | Gossip | Raft + Percolator | Multi-Paxos |
| SQL 支持 | 完整 | 有限 | 完整 (MySQL) | 完整 (MySQL/Oracle) |
| 典型延迟 | 亚 ms | ms 级 | ms 级 | ms 级 |
| 典型场景 | OLTP | 写入密集 | 通用 OLTP | 金融级 OLTP |

## 参考资料

- Dynamo: Amazon's Highly Available Key-value Store (2007)
- BigTable: A Distributed Storage System for Structured Data (2006)
- Spanner: Google's Globally-Distributed Database (2012)
- Percolator: Large Scale Incremental Processing (2010)
- CockroachDB: Resilient Geo-Distributed SQL (2015)
- TiDB: A Cloud-Native Distributed HTAP Database (2016)
