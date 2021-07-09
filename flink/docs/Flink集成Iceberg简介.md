# 概述

Apache Iceberg is an open table format for huge analytic datasets. Iceberg adds tables to Presto and Spark that use a high-performance format that works just like a SQL table.

官方的定义，iceberg是一种表格式。我们可以简单理解为他是基于计算层（flink、spark）和存储层（orc、parqurt）的一个中间层，我们可以把它定义成一种“数据组织格式”，Iceberg将其称之为“表格式”也是表达类似的含义。他与底层的存储格式（比如ORC、Parquet之类的列式存储格式）最大的区别是，它并不定义数据存储方式，而是定义了数据、元数据的组织方式，向上提供统一的“表”的语义。它构建在数据存储格式之上，其底层的数据存储仍然使用Parquet、ORC等进行存储。在hive建立一个iceberg格式的表。用flink或者spark写入iceberg，然后再通过其他方式来读取这个表，比如spark、flink、presto等。

 ![img](https://img2020.cnblogs.com/blog/1217276/202103/1217276-20210317113832767-579452657.png)

Iceberg的架构和实现并未绑定于某一特定引擎，它实现了通用的数据组织格式，利用此格式可以方便地与不同引擎（如Flink、Hive、Spark）对接。





为了在flink中创建iceberg表，我们要求使用**flink SQL client**，因为这对使用者们来说更容易去理解概念。

准备两个jar包：

- 从apache官方仓库下载flink-runtime.jar，https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime/
- flink的hive connector jar包，https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.6_2.11/1.11.0/flink-sql-connector-hive-2.3.6_2.11-1.11.0.jar

启动flink sql client，不带hive connector jar包，可以创建hadoop catalog如下：

```shell
./bin/sql-client.sh embedded \
    -j /data/flink-1.11.2/lib/iceberg-flink-runtime-0.10.0.jar \
    shell
```

启动flink sql client，带hive connector jar包，可以创建hadoop catalog和hive catalog如下：

```shell
./bin/sql-client.sh embedded \
    -j /data/flink-1.11.2/lib/iceberg-flink-runtime-0.10.0.jar \
    -j /data/flink-1.11.2/lib/flink-sql-connector-hive-2.2.0_2.11-1.11.2.jar \
    shell
```





##  创建catalogs和使用catalogs

Flink1.11支持通过flink sql创建catalogs。catalog是Iceberg对表进行管理（create、drop、rename等）的一个组件。目前Iceberg主要支持HiveCatalog和HadoopCatalog两种Catalog。其中**HiveCatalog将当前表metadata文件路径存储在Metastore**，这个表metadata文件是所有读写Iceberg表的入口，所以每次读写Iceberg表都需要先从Metastore中取出对应的表metadata文件路径，然后再解析这个Metadata文件进行接下来的操作。而HadoopCatalog将当前表metadata文件路径记录在一个文件目录下，因此不需要连接Metastore。



### **Hive catalog**

创建一个名为hive_catalog的 iceberg catalog ，用来从 hive metastore 中加载表。

```shell
CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://localhost:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='hdfs://nn:8020/warehouse/path'
);
```

- type: 只能使用iceberg,用于 iceberg 表格式。(必须)
- catalog-type: Iceberg 当前支持hive或hadoopcatalog 类型。(必须)
- uri: Hive metastore 的 thrift URI。 (必须)
- clients: Hive metastore 客户端池大小，默认值为 2。 (可选)
- property-version: 版本号来描述属性版本。此属性可用于在属性格式发生更改时进行向后兼容。当前的属性版本是 1。(可选)
- warehouse: Hive 仓库位置, 如果既不将 hive-conf-dir 设置为指定包含 hive-site.xml 配置文件的位置，也不将正确的 hive-site.xml 添加到类路径，则用户应指定此路径。
- hive-conf-dir: 包含 Hive-site.xml 配置文件的目录的路径，该配置文件将用于提供自定义的 Hive 配置值。 如果在创建 iceberg catalog 时同时设置 hive-conf-dir 和 warehouse，那么将使用 warehouse 值覆盖 < hive-conf-dir >/hive-site.xml (或者 classpath 中的 hive 配置文件)中的 hive.metastore.warehouse.dir 的值。

### ** Hadoop catalog**

Iceberg 还支持 HDFS 中基于目录的 catalog ，可以使用’catalog-type’='hadoop’进行配置：

```shell
CREATE CATALOG hadoop_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://nn:8020/warehouse/path',
  'property-version'='1'
);
```

- warehouse：hdfs目录存储元数据文件和数据文件。（必须）

我们可以执行sql命令USE CATALOG hive_catalog来设置当前的catalog。





## DataStream读写数据（Java API）

### DataStream读数据

Iceberg现在支持使用Java API流式或者批量读取。

####  批量读

这个例子从Iceberg表读取所有记录，然后在flink批处理作业中打印到stdout控制台。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadooptable("hdfs://nn:8020/warehouse/path");
DataStream<RowData> batch = FlinkSource.forRowData()
     .env(env)
     .tableLoader(loader)
     .streaming(false)
     .build();

// Print all records to stdout.
batch.print();

// Submit and execute this batch read job.
env.execute("Test Iceberg Batch Read");
```

#### 流式读

这个例子将会读取从快照id‘3821550127947089987’开始的增量记录，然后在flink流式作业中打印到stdout控制台中。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadooptable("hdfs://nn:8020/warehouse/path");
DataStream<RowData> stream = FlinkSource.forRowData()
     .env(env)
     .tableLoader(loader)
     .streaming(true)
     .startSnapshotId(3821550127947089987)
     .build();

// Print all records to stdout.
stream.print();

// Submit and execute this streaming read job.
env.execute("Test Iceberg streaming Read");
```

还有其他选项可以通过Java Api设置，详情请看[FlinkSource#Builder](http://iceberg.apache.org/flink/javadoc/master/org/apache/iceberg/flink/source/FlinkSource.html).



### DataStream写数据

Iceberg 支持从不同的 DataStream 输入写入 Iceberg 表。

- Appending data 追加数据

我们支持在本地编写 DataStream < rowdata > 和 DataStream < Row> 到 sink iceberg 表。

```java
StreamExecutionEnvironment env = ...;
DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadooptable("hdfs://nn:8020/warehouse/path");
FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .hadoopConf(hadoopConf)
    .build();
env.execute("Test Iceberg DataStream");
```

- Overwrite data 重写数据

为了动态覆盖现有 Iceberg 表中的数据，我们可以在FlinkSink构建器中设置overwrite标志。

```java
StreamExecutionEnvironment env = ...;
DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadooptable("hdfs://nn:8020/warehouse/path");
FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .overwrite(true)
    .hadoopConf(hadoopConf)
    .build();
env.execute("Test Iceberg DataStream");
```



##  重写文件操作

Iceberg可以通过提交flink批作业去提供API重写小文件变为大文件。flink操作表现与spark的[rewriteDataFiles](http://iceberg.apache.org/flink/maintenance/#compact-data-files).一样。

```java
import org.apache.iceberg.flink.actions.Actions;

TableLoader tableLoader = TableLoader.fromHadooptable("hdfs://nn:8020/warehouse/path");
Table table = tableLoader.loadTable();
RewriteDataFilesActionResult result = Actions.forTable(table)
        .rewriteDataFiles()
        .execute();
```

更多的重写文件操作选项文档，请看[RewriteDataFilesAction](http://iceberg.apache.org/flink/javadoc/master/org/apache/iceberg/flink/actions/RewriteDataFilesAction.html)





##  使用编程SQL方式读写Iceberg表

###  添加依赖

```
<dependency>
            <groupId>org.apache.iceberg</groupId>
            <artifactId>iceberg-flink-runtime</artifactId>
            <version>0.10.0</version>
</dependency>
```

### 部分代码实现

```java
// 使用table api 创建 hadoop catalog
 TableResult tableResult = tenv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://nameservice1/tmp',\n" +
                "  'property-version'='1'\n" +
                ")");
 
        // 使用catalog
        tenv.useCatalog("hadoop_catalog");
        // 创建库
        tenv.executeSql("CREATE DATABASE if not exists iceberg_hadoop_db");
        tenv.useDatabase("iceberg_hadoop_db");
 
     
        // 创建iceberg 结果表
        tenv.executeSql("drop table hadoop_catalog.iceberg_hadoop_db.iceberg_001");
        tenv.executeSql("CREATE TABLE  hadoop_catalog.iceberg_hadoop_db.iceberg_001 (\n" +
                "    id BIGINT COMMENT 'unique id',\n" +
                "    data STRING\n" +
                ")");
 
        // 测试写入
        tenv.executeSql("insert into hadoop_catalog.iceberg_hadoop_db.iceberg_001 select 100,'abc'");
```

### 创建hive的外部表来实时查询iceberg表

```sql
hive> add jar /tmp/iceberg-hive-runtime-0.10.0.jar;
 
hive> CREATE EXTERNAL TABLE tmp.iceberg_001(id bigint,data string)
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' 
LOCATION '/tmp/iceberg_hadoop_db/iceberg_001';
 
hive> select * from tmp.iceberg_001;
OK
100        abc
1001    abcd
Time taken: 0.535 seconds, Fetched: 2 row(s)
```





##  Flink结合Kafka实时写入Iceberg实践笔记

###  创建Hadoop Catalog的Iceberg 表

```java
// create hadoop catalog
        tenv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://nameservice1/tmp',\n" +
                "  'property-version'='1'\n" +
                ")");
 
        // change catalog
        tenv.useCatalog("hadoop_catalog");
        tenv.executeSql("CREATE DATABASE if not exists iceberg_hadoop_db");
        tenv.useDatabase("iceberg_hadoop_db");
        // create iceberg result table
        tenv.executeSql("drop table hadoop_catalog.iceberg_hadoop_db.iceberg_002"); 
        tenv.executeSql("CREATE TABLE  hadoop_catalog.iceberg_hadoop_db.iceberg_002 (\n" +
                "    user_id STRING COMMENT 'user_id',\n" +
                "    order_amount DOUBLE COMMENT 'order_amount',\n" +
                "    log_ts STRING\n" +
                ")");
```

###  使用Hive Catalog创建Kafka流表

```java
  String HIVE_CATALOG = "myhive";
        String DEFAULT_DATABASE = "tmp";
        String HIVE_CONF_DIR = "/xx/resources";
        Catalog catalog = new HiveCatalog(HIVE_CATALOG, DEFAULT_DATABASE, HIVE_CONF_DIR);
        tenv.registerCatalog(HIVE_CATALOG, catalog);
        tenv.useCatalog("myhive");
        // create kafka stream table
        tenv.executeSql("DROP TABLE IF EXISTS ods_k_2_iceberg");
        tenv.executeSql(
                "CREATE TABLE ods_k_2_iceberg (\n" +
                        " user_id STRING,\n" +
                        " order_amount DOUBLE,\n" +
                        " log_ts TIMESTAMP(3),\n" +
                        " WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector'='kafka',\n" +
                        "  'topic'='t_kafka_03',\n" +
                        "  'scan.startup.mode'='latest-offset',\n" +
                        "  'properties.bootstrap.servers'='xx:9092',\n" +
                        "  'properties.group.id' = 'testGroup_01',\n" +
                        "  'format'='json'\n" +
                        ")");
```

### 使用SQL连接kafka流表和iceberg 目标表

```java
 System.out.println("---> 3. insert into iceberg  table from kafka stream table .... ");
        tenv.executeSql(
                "INSERT INTO  hadoop_catalog.iceberg_hadoop_db.iceberg_002 " +
                        " SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd') FROM myhive.tmp.ods_k_2_iceberg");
```

### 数据验证

```sql
bin/kafka-console-producer.sh --broker-list xx:9092 --topic t_kafka_03
{"user_id":"a1111","order_amount":11.0,"log_ts":"2020-06-29 12:12:12"}
{"user_id":"a1111","order_amount":11.0,"log_ts":"2020-06-29 12:15:00"}
{"user_id":"a1111","order_amount":11.0,"log_ts":"2020-06-29 12:20:00"}
{"user_id":"a1111","order_amount":11.0,"log_ts":"2020-06-29 12:30:00"}
{"user_id":"a1111","order_amount":13.0,"log_ts":"2020-06-29 12:32:00"}
{"user_id":"a1112","order_amount":15.0,"log_ts":"2020-11-26 12:12:12"}
 
hive> add jar /home/zmbigdata/iceberg-hive-runtime-0.10.0.jar;
hive> CREATE EXTERNAL TABLE tmp.iceberg_002(user_id STRING,order_amount DOUBLE,log_ts STRING)
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' 
LOCATION '/tmp/iceberg_hadoop_db/iceberg_002';
hive> select * from tmp.iceberg_002  limit 5;
a1111    11.0    2020-06-29
a1111    11.0    2020-06-29
a1111    11.0    2020-06-29
a1111    11.0    2020-06-29
a1111    13.0    2020-06-29
Time taken: 0.108 seconds, Fetched: 5 row(s)
```





