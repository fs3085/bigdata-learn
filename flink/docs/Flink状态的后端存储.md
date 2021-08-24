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



https://zhuanlan.zhihu.com/p/79526638

## 1. Checkpoint，Savepoint 异同

首先，为什么会在文章开头对这两点进行介绍，因为有时候用户在开发实时任务时，会对这两点产生困惑，所以这里直接开门见山对这两点进行讲解。

**Flink Checkpoint 是一种容错恢复机制。**这种机制保证了实时程序运行时，即使突然遇到异常也能够进行自我恢复。Checkpoint 对于用户层面，是透明的，用户会感觉程序一直在运行。Flink Checkpoint 是 Flink 自身的系统行为，用户无法对其进行交互，用户可以在程序启动之前，设置好实时程序 Checkpoint 相关参数，当程序启动之后，剩下的就全交给 Flink 自行管理。当然在某些情况，比如 Flink On Yarn 模式，某个 Container 发生 OOM 异常，这种情况程序直接变成失败状态，此时 Flink 程序虽然开启 Checkpoint 也无法恢复，因为程序已经变成失败状态，所以此时可以借助外部参与启动程序，比如外部程序检测到实时任务失败时，从新对实时任务进行拉起。

**Flink Savepoint 你可以把它当做在某个时间点程序状态全局镜像，以后程序在进行升级，或者修改并发度等情况，还能从保存的状态位继续启动恢复。**Flink Savepoint 一般存储在 HDFS 上面，它需要用户主动进行触发。如果是用户自定义开发的实时程序，比如使用DataStream进行开发，建议为每个算子定义一个 uid，这样我们在修改作业时，即使导致程序拓扑图改变，由于相关算子 uid 没有变，那么这些算子还能够继续使用之前的状态，如果用户没有定义 uid ， Flink 会为每个算子自动生成 uid，如果用户修改了程序，可能导致之前的状态程序不能再进行复用。

**Flink Checkpoint和Savepoint对比：**

1. 概念：Checkpoint 是 自动容错机制 ，Savepoint 程序全局状态镜像 。
2. 目的： Checkpoint 是程序自动容错，快速恢复 。Savepoint是 程序修改后继续从状态恢复，程序升级等。
3. 用户交互:Checkpoint 是 Flink 系统行为 。Savepoint是用户触发。
4. 状态文件保留策略：Checkpoint默认程序删除，可以设置CheckpointConfig中的参数进行保留 。Savepoint会一直保存，除非用户删除 。

## **2. Flink Checkpoint**

## 2.1 Flink Checkpoint 原理

Flink Checkpoint 机制保证 Flink 任务运行突然失败时，能够从最近 Checkpoint 进行状态恢复启动，进行错误容忍。它是一种自动容错机制，而不是具体的状态存储镜像。Flink Checkpoint 受 Chandy-Lamport 分布式快照启发，其内部使用[分布式数据流轻量级异步快照](https://link.zhihu.com/?target=https%3A//arxiv.org/abs/1506.08603)。

Checkpoint 保存的状态在程序取消时，默认会进行清除。Checkpoint 状态保留策略有两种:

1. **DELETE_ON_CANCELLATION 表示当程序取消时，删除 Checkpoint 存储文件。**
2. **RETAIN_ON_CANCELLATION 表示当程序取消时，保存之前的 Checkpoint 存储文件**。用户可以结合业务情况，设置 Checkpoint 保留模式。

默认情况下，Flink不会触发一次 Checkpoint 当系统有其他 Checkpoint 在进行时，也就是说 Checkpoint 默认的并发为1。针对 Flink DataStream 任务，程序需要经历从 **StreamGraph -> JobGraph -> ExecutionGraph -> 物理执行图四个步骤**，其中在 ExecutionGraph 构建时，会初始化 CheckpointCoordinator。ExecutionGraph通过ExecutionGraphBuilder.buildGraph方法构建，在构建完时，会调用 ExecutionGraph 的enableCheckpointing方法创建CheckpointCoordinator。CheckpoinCoordinator 是 Flink 任务 Checkpoint 的关键，针对每一个 Flink 任务，都会初始化一个 CheckpointCoordinator 类，来触发 Flink 任务 Checkpoint。下面是 Flink 任务 Checkpoint 大致流程：

![img](https://pic3.zhimg.com/80/v2-9e035b779ffafcc6ee6a5b5164129aee_720w.jpg)

Flink 会定时在任务的 Source Task 触发 Barrier，Barrier是一种特殊的消息事件，会随着消息通道流入到下游的算子中。只有当最后 Sink 端的算子接收到 Barrier 并确认该次 Checkpoint 完成时，该次 Checkpoint 才算完成。所以在某些算子的 Task 有多个输入时，会存在 Barrier 对齐时间，我们可以在Web UI上面看到各个 Task 的Barrier 对齐时间

## 2.2 Flink Checkpoint 语义

Flink Checkpoint 支持两种语义：**Exactly Once** 和 **At least Once，**默认的 Checkpoint 模式是 Exactly Once. Exactly Once 和 At least Once 具体是针对 Flink **状态** 而言。具体语义含义如下：

**Exactly Once** 含义是：保证每条数据对于 Flink 的状态结果只影响一次。打个比方，比如 WordCount程序，目前实时统计的 "hello" 这个单词数为5，同时这个结果在这次 Checkpoint 成功后，保存在了 HDFS。在下次 Checkpoint 之前， 又来2个 "hello" 单词，突然程序遇到外部异常容错自动回复，从最近的 Checkpoint 点开始恢复，那么会从单词数 5 这个状态开始恢复，Kafka 消费的数据点位还是状态 5 这个时候的点位开始计算，所以即使程序遇到外部异常自我恢复，也不会影响到 Flink 状态的结果。

**At Least Once** 含义是：每条数据对于 Flink 状态计算至少影响一次。比如在 WordCount 程序中，你统计到的某个单词的单词数可能会比真实的单词数要大，因为同一条消息，你可能将其计算多次。

Flink 中 Exactly Once 和 At Least Once 具体是针对 Flink 任务**状态**而言的，并不是 Flink 程序对其处理一次。举个例子，当前 Flink 任务正在做 Checkpoint，该次Checkpoint还么有完成，该次 Checkpoint 时间端的数据其实已经进入 Flink 程序处理，只是程序状态没有最终存储到远程存储。当程序突然遇到异常，进行容错恢复，那么就会从最新的 Checkpoint 进行状态恢复重启，上一部分还会进入 Flink 系统处理：

![img](https://pic1.zhimg.com/80/v2-d43026f18dc39c23a9e2305091045d80_720w.jpg)

上图中表示，在进行 chk-5 Checkpoint 时，突然遇到程序异常，那么会从 chk-4 进行恢复，那么之前chk-5 处理的数据，会再次进行处理。

Exactly Once 和 At Least Once 具体在底层实现大致相同，具体差异表现在 Barrier 对齐方式处理：

![img](https://pic3.zhimg.com/80/v2-ca6460919adae963a492baf8a8d26dde_720w.jpg)

如果是 Exactly Once 模式，某个算子的 Task 有多个输入通道时，当其中一个输入通道收到 Barrier 时，Flink Task 会阻塞处理该通道，其不会处理这些数据，但是会将这些数据存储到内部缓存中，一旦完成了所有输入通道的 Barrier 对齐，才会继续对这些数据进行消费处理。

对于 At least Once,同样针对某个算子的 Task 有多个输入通道的情况下，当某个输入通道接收到 Barrier 时，它不同于Exactly Once,At Least Once 会继续处理接受到的数据，即使没有完成所有输入通道 Barrier 对齐。所以使用At Least Once 不能保证数据对于状态计算只有一次影响。

## 2.3 Flink Checkpoint 参数配置及建议

\1. 当 Checkpoint 时间比设置的 Checkpoint 间隔时间要长时，可以设置 Checkpoint 间最小时间间隔 。这样在上次 Checkpoint 完成时，不会立马进行下一次 Checkpoint，而是会等待一个最小时间间隔，然后在进行该次 Checkpoint。否则，每次 Checkpoint 完成时，就会立马开始下一次 Checkpoint，系统会有很多资源消耗 Checkpoint。

\2. 如果Flink状态很大，在进行恢复时，需要从远程存储读取状态恢复，此时可能导致任务恢复很慢，可以设置 Flink Task 本地状态恢复。任务状态本地恢复默认没有开启，可以设置参数`state.backend.local-recovery`值为`true`进行激活。

\3. Checkpoint保存数，Checkpoint 保存数默认是1，也就是保存最新的 Checkpoint 文件，当进行状态恢复时，如果最新的Checkpoint文件不可用时(比如HDFS文件所有副本都损坏或者其他原因)，那么状态恢复就会失败，如果设置 Checkpoint 保存数2，即使最新的Checkpoint恢复失败，那么Flink 会回滚到之前那一次Checkpoint进行恢复。考虑到这种情况，用户可以增加 Checkpoint 保存数。

\4. 建议设置的 Checkpoint 的间隔时间最好大于 Checkpoint 的完成时间。

下图是不设置 Checkpoint 最小时间间隔示例图，可以看到，系统一致在进行 Checkpoint，可能对运行的任务产生一定影响：

![img](https://pic2.zhimg.com/80/v2-c048a0cb266ef9cb6e770fde781564f9_720w.jpg)

## 3. Flink Savepoint

## 3.1 Flink Savepoint 原理

Flink Savepoint 作为实时任务的全局镜像，其在底层使用的代码和Checkpoint的代码是一样的，因为Savepoint可以看做 Checkpoint在特定时期的一个状态快照。

Flink 在触发Savepoint 或者 Checkpoint时，会根据这次触发的类型计算出在HDFS上面的目录:

![img](https://pic4.zhimg.com/80/v2-6a1f0c7ed204f7f5d18a920fa632003f_720w.png)

如果类型是 Savepoint，那么 其 HDFS 上面的目录为：Savepoint 根目录+savepoint-jobid前六位+随机数字，具体如下格式：

![img](https://pic2.zhimg.com/80/v2-4dc4da4a091235c8b2581c1a7ade098d_720w.png)

Checkpoint 目录为 chk-checkpoint ID,具体格式如下：

![img](https://pic2.zhimg.com/80/v2-28ddfa3e3bee25c5dd6a51a58fb76549_720w.png)

一次 Savepoint 目录下面至少包括一个文件，既 **_metadata** 文件。当然如果实时任务某些算子有状态的话，那么在 这次 Savepoint 目录下面会包含一个 **_metadata** 文件以及多个状态数据文件。**_metadata** 文件以绝对路径的形式指向状态文件的指针。

社区方面，在以前的 Flink 版本，当用户选择不同的状态存储，其底层状态存储的二进制格式都不相同。针对这种情况，目前 [FLIP-41]([FLIP-41: Unify Binary format for Keyed State](https://link.zhihu.com/?target=https%3A//cwiki.apache.org/confluence/display/FLINK/FLIP-41%3A%2BUnify%2BBinary%2Bformat%2Bfor%2BKeyed%2BState)) 对于 Keyed State 使用统一的二进制文件进行存储。这里的 Keyed State 主要是针对 Savepoint 的状态，Checkpoint 状态的存储可以根据具体的状态后端进行存储，允许状态存储底层格式的差异。对于 Savepoint 状态底层格式的统一，应用的状态可以在不同的状态后端进行迁移，更方便应用程序的恢复。重做与状态快照和恢复相关的抽象，当实现实现新状态后端时，可以降低开销，同时减少代码重复。

## 3.2 Flink Savepoint 触发方式

Flink Savepoint 触发方式目前有三种：

1. 使用 **flink savepoint** 命令触发 Savepoint,其是在程序运行期间触发 savepoint。

2. 使用 **flink cancel -s** 命令，取消作业时，并触发 Savepoint。

3. 使用 Rest API 触发 Savepoint，格式为：***\*/jobs/:jobid /savepoints\****

## 3.3 Flink Savepoint 注意点

1. 使用 **flink cancel -s** 命令取消作业同时触发 Savepoint 时，会有一个问题，可能存在触发 Savepoint 失败。比如实时程序处于异常状态(比如 Checkpoint失败)，而此时你停止作业，同时触发 Savepoint,这次 Savepoint 就会失败，这种情况会导致，在实时平台上面看到任务已经停止，但是实际实时作业在 Yarn 还在运行。针对这种情况，需要捕获触发 Savepoint 失败的异常，当抛出异常时，可以直接在 Yarn 上面 Kill 掉该任务。

2. 使用 DataStream 程序开发时，最好为每个算子分配 `uid`,这样即使作业拓扑图变了，相关算子还是能够从之前的状态进行恢复，默认情况下，Flink 会为每个算子分配 `uid`,这种情况下，当你改变了程序的某些逻辑时，可能导致算子的 `uid` 发生改变，那么之前的状态数据，就不能进行复用，程序在启动的时候，就会报错。

3. 由于 Savepoint 是程序的全局状态，对于某些状态很大的实时任务，当我们触发 Savepoint，可能会对运行着的实时任务产生影响，个人建议如果对于状态过大的实时任务，触发 Savepoint 的时间，不要太过频繁。根据状态的大小，适当的设置触发时间。

4. 当我们从 Savepoint 进行恢复时，需要检查这次 Savepoint 目录文件是否可用。可能存在你上次触发 Savepoint 没有成功，导致 HDFS 目录上面 Savepoint 文件不可用或者缺少数据文件等，这种情况下，如果在指定损坏的 Savepoint 的状态目录进行状态恢复，任务会启动不起来。

