# 数据湖（Data Lake）演进总览

## 什么是数据湖

数据湖 = 集中存储原始数据（任意格式）+ Schema-on-Read + 计算引擎解耦

```
原始数据（结构化 + 半结构化 + 非结构化）
         ↓
     ┌────────┐
     │ 数据湖  │  ← 存储层（通常是对象存储 / HDFS）
     └────┬───┘
          ↓
    ┌─────┴─────┬─────────┐
    ↓           ↓         ↓
  查询       机器学习    ETL/ELT
  (Presto)   (Spark)   (Spark)
```

**数据湖 vs 数据仓库**

| 维度 | 数据仓库 | 数据湖 |
|------|----------|--------|
| 数据格式 | 结构化（严格 Schema） | 任意格式（原始数据） |
| Schema | Schema-on-Write | Schema-on-Read |
| 用户 | 业务分析师 | 数据科学家、工程师 |
| 成本 | 高（专用存储） | 低（对象存储） |
| 灵活性 | 低（Schema 变更困难） | 高（先存后定义） |
| 查询性能 | 优（预优化） | 取决于计算引擎 |
| 数据质量 | 高（ETL 后入库） | 不确定（原始数据） |

## 演进脉络

```
数据仓库 (1990s) → Hadoop/HDFS (2006) → 数据湖概念 (2011)
→ S3 + Presto (2015) → 湖仓一体 (2019)
→ Table Format 三巨头 (Hudi/Delta/Iceberg, 2019-2020)
```

## 核心驱动力

1. **数据类型爆炸**：结构化 → 半结构化（JSON/日志）→ 非结构化（图片/视频）
2. **成本压力**：传统数仓存储成本过高
3. **ML/AI 需求**：需要原始数据训练模型
4. **实时分析**：批流一体需求

## 技术演进主线

### 存储层
- HDFS → 对象存储 (S3) → 表格式 (Iceberg/Hudi/Delta)

### 计算引擎
- MapReduce → Spark → Presto/Trino → Flink (实时)

### 架构模式
- 数仓与湖分离 → 湖仓一体 (Lakehouse) → 开放表格式

## 时代划分

### 第一代：原始数据湖 (2006-2015)

| 技术 | 核心特征 |
|------|----------|
| Hadoop HDFS | 原始数据集中存储，MapReduce 计算 |
| Hive | SQL-on-Hadoop，Schema-on-Read |
| 早期数据湖概念 | EMC 2011 年正式提出"Data Lake"术语 |

**问题**：数据沼泽（Data Swamp）——缺乏治理，数据不可用

### 第二代：云原生数据湖 (2015-2019)

| 技术 | 核心特征 |
|------|----------|
| AWS S3 + Athena/Presto | 对象存储 + Serverless 查询 |
| Delta Lake (Databricks) | ACID 事务、版本控制、Schema 演化 |
| Apache Hudi (Uber) | Upsert/Delete、增量处理、流式摄入 |

**核心突破**：在数据湖上实现 ACID 语义

### 第三代：开放表格式 (2019-2023)

| 技术 | 核心特征 |
|------|----------|
| Apache Iceberg (Netflix) | 快照隔离、隐藏分区、多引擎支持 |
| Apache Hudi 成熟 | CDC 支持、增量查询、流批一体 |
| Delta Lake 开源 | 统一批流、时间旅行、优化写入 |

**三大表格式对比**

| 维度 | Apache Hudi | Delta Lake | Apache Iceberg |
|------|-------------|------------|----------------|
| 发起方 | Uber | Databricks | Netflix |
| 写入模型 | Copy-on-Write + Merge-on-Read | Copy-on-Write + Merge-on-Read | 快照 +  Manifest |
| Upsert | ✅ 原生 | ✅ (需 OPTIMIZE) | ✅ (通过 rewrite) |
| CDC | ✅ 原生 | ✅ | 有限 |
| 流式摄入 | ✅ | ✅ | 有限 |
| 引擎支持 | Spark/Flink | Spark/Databricks | Spark/Flink/Presto/Trino/StarRocks |
| 时间旅行 | ✅ | ✅ | ✅ |
| Schema 演化 | 有限 | ✅ | ✅ 完善 |
| 云厂商支持 | AWS EMR | Databricks | AWS/Snowflake/BigQuery |

### 第四代：湖仓一体 (2019+)

| 技术 | 核心特征 |
|------|----------|
| Databricks Lakehouse | Delta Lake + Spark + 数仓能力 |
| Snowflake + Iceberg | 云数仓查询数据湖 |
| Trino + Iceberg | 联邦查询引擎 |

## 数据湖架构层次

```
┌──────────────────────────────────────────┐
│            计算层 (Compute)               │
│  Spark │ Presto/Trino │ Flink │ ML       │
├──────────────────────────────────────────┤
│          表格式层 (Table Format)           │
│  Iceberg │ Delta Lake │ Hudi              │
├──────────────────────────────────────────┤
│            存储层 (Storage)               │
│  S3 │ MinIO │ HDFS │ ADLS │ OSS          │
├──────────────────────────────────────────┤
│            缓存/加速层 (可选)              │
│  Alluxio │ 本地 SSD 缓存                   │
└──────────────────────────────────────────┘
```

## 参考资料

- Data Lake: A New Architecture for Enterprise Data Management (2011)
- Delta Lake paper and documentation
- Apache Hudi architecture docs
- Apache Iceberg specification
- Databricks Lakehouse whitepaper
