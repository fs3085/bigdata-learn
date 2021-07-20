## Flink状态的后端存储

状态数据在流处理中是非常关键的，所以为了保证状态数据不丢失。需要启用Checkpoint机制来进行状态的持久化，从而防止数据丢失、并且保障数据恢复时数据是一致的。而后端存储（State Backends）就决定了状态在进行Checkpoint的时候以什么形式进行持久化、并将数据持久化到什么位置。



Flink中提供了多种State backends，它用于指定状态的存储方式和存储位置。状态可以在JVM的堆或者是堆外内存。Flink job会使用配置文件flink-conf.yaml中指定默认state backend。



在Flink中内置有以下三种State Backends。

MemoryStateBackend

FsStateBackend

RocksDBStateBackend

### MemoryStateBackend

基于内存的状态后端，状态数据是以Java对象的形式存储在堆中。例如：key/value形式的状态、窗口操作持有存储的状态值、窗口触发器的hashmap。

通过checkpoint可以定期对state进行snapshot（快照）。基于内存的状态后端，状态的快照数据是通过Checkpoint机制，将状态快照发送到JobManager（Master/AM）中存储，然后JobManager将状态快照数据存储在JVM堆中。

**「注意：你没看错，是存储在JobManager堆中，所以JobManager需要有足够的内存」**

Flink官方建议使用异步快照，这样可以有效避免流处理阻塞。异步快照当然默认也是开启的。既然是基于内存存储，而且状态在流计算中如此重要，那么针对一些大型复杂的流处理作业，MemoryStateBackend是肯定不适合用在生产环境中的。做一些状态很小的Job、或者用来做个本地开发、调试是可以的。

### FsStateBackend

基于文件系统的状态后端，它是将流处理程序运行中的状态数据存储在TaskManager内存中，Checkpoint时，将状态的快照存储在文件系统目录。

我们可以将状态存在在运行Flink集群的操作系统的文件系统上（file:///），通常将状态指定存储在HDFS上的某个路径。那有个问题来了？如果每次Checkpoint都写HDFS，我们知道HDFS并不是一个low latecy的存储，这样势必会降低流处理系统的处理效率，对数据处理造成堵塞。

所以，FsStateBackend默认也是使用异步快照的方式保存，从而避免流处理阻塞。

FsStateBackend适合用来存储一些状态比较大、窗口时间比较长、key/value状态也很大的Job。

### RocksDBStateBackends

基于RocksDB数据库的状态后端可以将状态存储在RocksDB数据库中。RocksDB是一个C++库，可以用来存储key、value数据，而且重要的一点是RocksDB可以将数据存储在内存中、或者磁盘、HDFS中。

所以不要以为RocksDB只是将数据存储在操作系统本地，它依然可以将数据存储在HDFS中。RocksDBStateBackend可以将数据存储在HDFS上，所以它也适合存储状态**特别**大的、窗口时间**特别**长、key/value状态也可以**特别**大的Job。

因为基于RocksDB的状态后端需要将所有的读写操作都是要将对象进行序列化和反序列化，然后存储在磁盘中。所以自然会降低Flink程序的吞吐量，它在效率上比FsStateBackend要低很多（它是将状态数据存储在TaskManager内存中）。

但是基于数据库的状态后端，所以RocksDBStateBackends是唯一支持增量Checkpoint的Backends。



## state与Checkpoint、snapshot、Savepoint的关系

**state**是流处理数据过程中，某些数据处理操作需要保存数据结果，这些数据结果可以存储在**state**中。

为了保证容错，这些重要的状态数据需要保存下来。所以，**checkpoint**就周期性到将**state**数据持久化。而持久化是以**snapshot**快照地形式保存的，每一次**checkpoint**就是将**state**做一次快照，然后持久化。

**savepoint**是一个镜像（image），是依托**checkpoint**机制创建的，它会将整个流处理的作业流做一个镜像。有了这个镜像，Flink的应用就可以用**savepoint**进行停止和重启了。

**savepoint**相当于是整个Flink流处理应用的状态整体备份，而**checkpoint**是针对如果某个TM或者task出现异常，通过**checkpoint**能够快速恢复回来。**savepoint**针对是如果因为升级、调整并行度，由用户计划性地重启Flink集群使用。**checkpoint**是Flink的内部机制，是为了流处理系统遇到意外情况可以快速恢复回来。