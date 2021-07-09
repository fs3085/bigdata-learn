## 什么是Hive Catalog

Hive使用Hive Metastore(HMS)存储元数据信息，使用关系型数据库来持久化存储这些信息。所以，Flink集成Hive需要打通Hive的metastore，去管理Flink的元数据，这就是Hive Catalog的功能。

Hive Catalog的主要作用是使用Hive MetaStore去管理Flink的元数据。Hive Catalog可以将元数据进行持久化，这样后续的操作就可以反复使用这些表的元数据，而不用每次使用时都要重新注册。如果不去持久化catalog，那么在每个session中取处理数据，都要去重复地创建元数据对象，这样是非常耗时的。

HiveCatalog可以处理两种类型的表：一种是Hive兼容的表，另一种是普通表(generic table)。其中Hive兼容表是以兼容Hive的方式来存储的，所以，对于Hive兼容表而言，我们既可以使用Flink去操作该表，又可以使用Hive去操作该表。

普通表是对Flink而言的，当使用HiveCatalog创建一张普通表，仅仅是使用Hive MetaStore将其元数据进行了持久化，所以可以通过Hive查看这些表的元数据信息(通过DESCRIBE FORMATTED命令)，但是不能通过Hive去处理这些表，因为语法不兼容。

对于是否是普通表，Flink使用is_generic属性进行标识。默认情况下，创建的表是普通表，即is_generic=true，如果要创建Hive兼容表，需要在建表属性中指定is_generic=false。



由于依赖Hive Metastore，所以必须开启Hive MetaStore服务



对于Hive兼容的表，需要注意数据类型，具体的数据类型对应关系以及注意点如下：

| Flink 数据类型 | Hive 数据类型 |
| :------------: | :-----------: |
|    CHAR(p)     |    CHAR(p)    |
|   VARCHAR(p)   |  VARCHAR(p)   |
|     STRING     |    STRING     |
|    BOOLEAN     |    BOOLEAN    |
|    TINYINT     |    TINYINT    |
|    SMALLINT    |   SMALLINT    |
|      INT       |      INT      |
|     BIGINT     |     LONG      |
|     FLOAT      |     FLOAT     |
|     DOUBLE     |    DOUBLE     |
| DECIMAL(p, s)  | DECIMAL(p, s) |
|      DATE      |     DATE      |
|  TIMESTAMP(9)  |   TIMESTAMP   |
|     BYTES      |    BINARY     |
|     ARRAY      |     LIST      |
|   MAP<K, V>    |   MAP<K, V>   |
|      ROW       |    STRUCT     |





## 什么是Hive Dialect

从Flink1.11.0开始，只要开启了Hive dialect配置，用户就可以使用HiveQL语法，这样我们就可以在Flink中使用Hive的语法使用一些DDL和DML操作。

Flink目前支持两种SQL方言(SQL dialects),分别为：**default和hive**。默认的SQL方言是**default**，如果要使用Hive的语法，需要将SQL方言切换到**hive**。

Flink SQL> **set** table.sql-dialect=hive; *-- 使用hive dialect*
Flink SQL> **set** table.sql-dialect=**default**; *-- 使用default dialect*



## Flink写入Hive表

Flink支持以**批处理(Batch)和流处理(Streaming)**的方式写入Hive表。当以批处理的方式写入Hive表时，只有当写入作业结束时，才可以看到写入的数据。**批处理的方式写入支持append模式和overwrite模式**。

### 流处理模式写入

流式写入Hive表，不支持**Insert overwrite **方式



1.Flink读取Hive表默认使用的是batch模式，如果要使用流式读取Hive表，需要而外指定一些参数，见下文。

2.只有在完成 Checkpoint 之后，文件才会从 In-progress 状态变成 Finish 状态，同时生成`_SUCCESS`文件，所以，Flink流式写入Hive表需要开启并配置 Checkpoint。对于Flink SQL Client而言，需要在flink-conf.yaml中开启CheckPoint，配置内容为：

```shell
state.backend: filesystem 

execution.checkpointing.externalized-checkpoint-retention:RETAIN_ON_CANCELLATION 

execution.checkpointing.interval: 60s 

execution.checkpointing.mode: EXACTLY_ONCE 

state.savepoints.dir: hdfs://kms-1:8020/flink-savepoints

state.checkpoints.dir: hdfs:///user/appuser/cluster/flink/flink-checkpoints
```

 **set** execution.type=streaming;

 Checkpoint is not supported for batch jobs



- 支持 Hive 的互操作。有了 Catalog 之后用户就可以通过 Catalog 访问 Hive 的元数据，提供 Data Connector 让用户能通过 Flink 读写 Hive 的实际数据，实现 Flink 与 Hive 的交互。

- 支持 Flink 作为 Hive 的计算引擎（长期目标），像 Hive On Spark，Hive On Tez。

  

  有了元数据之后我们就可以实现 Flink SQL 的 Data Connector 来真正的读写 Hive 实际数据。Flink SQL 写入的数据必须要兼容 Hive 的数据格式，也就是 Hive 可以正常读取 Flink 写入的数据，反过来也是一样的。为了实现这一点我们大量复用 Hive 原有的 Input/Output Format、SerDe 等 API，一是为了减少代码冗余，二是尽可能的保持兼容性。
  在 Data Connect 中读取 Hive 表数据具体实现类为：HiveTableSource、HiveTableInputFormat。写 Hive 表的具体实现类为：HiveTableSink、HiveTableOutputFormat。



