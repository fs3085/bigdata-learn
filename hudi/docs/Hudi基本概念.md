### 业务场景和技术选型

传统的离线数仓，通常数据是T+1的，不能满足对当日数据分析的需求
而流式计算一般是基于窗口，并且窗口逻辑相对比较固定。
有一类特殊的需求，业务分析比较熟悉现有事务数据库的数据结构，并且希望有很多即席分析，这些分析包含当日比较实时的数据。惯常他们是基于Mysql从库，直接通过Sql做相应的分析计算。但很多时候会遇到如下障碍

- 数据量较大、分析逻辑较为复杂时，Mysql从库耗时较长

- 一些跨库的分析无法实现

因此，一些弥合在OLTP和OLAP之间的技术框架出现，典型有TiDB。它能同时支持OLTP和OLAP。而诸如**Apache Hudi**和Apache Kudu则相当于现有OLTP和OLAP技术的桥梁。他们能够以现有OLTP中的数据结构存储数据，支持CRUD，同时提供跟现有OLAP框架的整合(如Hive，Impala)，以实现OLAP分析

Apache Kudu，需要单独部署集群。而Apache Hudi则不需要，它可以利用现有的大数据集群比如HDFS做数据文件存储，然后通过Hive做数据分析，相对来说更适合资源受限的环境



### Hudi表数据结构

Hudi表的数据文件，可以使用操作系统的文件系统存储，也可以使用HDFS这种分布式的文件系统存储。为了后续分析性能和数据的可靠性，一般使用HDFS进行存储。以HDFS存储来看，一个Hudi表的存储文件分为两类。



![img](https://pic3.zhimg.com/v2-d821bd7e7d4f44bbe1b92af32f111a3e_b.jpg)

- 包含`_partition_key`相关的路径是实际的数据文件，按分区存储，当然分区的路径key是可以指定的，我这里使用的是_partition_key

- .hoodie 由于CRUD的零散性，每一次的操作都会生成一个文件，这些小文件越来越多后，会严重影响HDFS的性能，Hudi设计了一套文件合并机制。 .hoodie文件夹中存放了对应的文件合并操作相关的日志文件。

  

  **数据文件**

  Hudi真实的数据文件使用Parquet文件格式存储

  ![img](https://pic4.zhimg.com/v2-18074ff7004ae727be29b0ec042c1b3f_b.jpg)

  

  **.hoodie文件**

  Hudi把随着时间流逝，对表的一系列CRUD操作叫做Timeline。Timeline中某一次的操作，叫做Instant。Instant包含以下信息

  - Instant Action 记录本次操作是一次数据提交（COMMITS），还是文件合并（COMPACTION），或者是文件清理（CLEANS）

  - Instant Time 本次操作发生的时间

  - state 操作的状态，发起(REQUESTED)，进行中(INFLIGHT)，还是已完成(COMPLETED)

  .hoodie文件夹中存放对应操作的状态记录

  ![img](https://pic2.zhimg.com/v2-19f813d0cf3aa47e2f68f118b423c2d1_b.jpg)



### Hudi记录Id

hudi为了实现数据的CRUD，需要能够唯一标识一条记录。hudi将把数据集中的唯一字段(record key ) + 数据所在分区 (partitionPath) 联合起来当做数据的唯一键