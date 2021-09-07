Flink自身是无法保证外部系统精确一次语义的,所以Flink要实现精确一次的话,其外部系统必须支持精确一次语义。然后借助Flink提供的**分布式快照**和**两阶段提交**才能实现。



## 分布式快照

Flink提供了失败恢复的容错机制,而这个容错机制的核心即使持续创建分布式数据流的快照来实现。Flink的快照可以达到算子级别,并且对全局数据也可以做快照。
由于checkpoint(快照)是一个全局状态,用户保存的状态可能非常大,达到G或者T级别。在这种情况下checkpoint的创建会非常慢,而且执行时占用的资源也比较多,因此Flink提出了增量快照的概念。
其含义每次都是进行的全量checkpoint,是基于上次进行更新的。

## 两阶段提交

快照机制能够保证作业出现fail-over后可以从最新的快照进行恢复,即分布式快照机制可以保证Flink系统内部的精确一次处理。
但是我们在实际生产系统中,Flink会对接各种各样的外部系统,例如Kafka、HDFS等,一旦Flink作业出现失败,作业会重新消费旧数据,这时候就会出现重新消费的情况,也就是重复消费消息。
针对这种情况,于是在Flink 1.4 版本引入了一个很重要的功能:两阶段提交:实现类`TwoPhaseCommitSinkFunction`,两阶段搭配特定的source和sink(特别是 0.11 版本 Kafka)使得精确一次处理语义成为可能。
`TwoPhaseCommitSinkFunction`这个抽象类中,只需要实现其中的beginTransaction、preCommit、commit、abort四个方法就可以实现精确一次的处理语义,四个方法的其含义为:

- beginTransaction:在开启事务之前,在目标文件系统的临时目录中创建一个临时文件,后面在处理数据时将数据写入此文件。
- preCommit:在预提交阶段,刷写(flush)文件,然后关闭文件,之后就不能写入到文件了。然后为属于下一个检查点的任何后续写入启动新事务。
- commit:在提交阶段,将预提交的文件原子性移动到真正的目标目录中,但这会增加输出数据可见性的延迟。
- abort:在中止阶段,删除临时文件。

以Kafka-Flink-Kafka为例,整个过程可以总结为以下四个阶段:

- 一旦Flink开始做checkpoint操作,就会进入pre-commit阶段,同时Flink JobManager会将检查点注入数据流中。

- 当所有的数据在算子中成功进行一遍传递,并完成快照后,在pre-commit阶段完成。

- 等所有的算子完成pre-commit,就会发起一个提交(commit)动作,但是任何一个预提交(pre-commit)失败都会导致Flink回滚到最近的checkpoint。

- 预提交(pre-commit)完成,必须确保commit也要成功。

  

### 状态 Exactly-Once 和端到端 Exactly-Once

Flink 提供 exactly-once 的状态（state）投递语义，这为有状态的（stateful）计算提供了准确性保证。其中比较容易令人混淆的一点是状态投递语义和更加常见的端到端（end to end）投递语义，而实现前者是实现后者的前置条件。

Flink 从 0.9 版本开始提供 State API，标志着 Flink 进入了 Stateful Streaming 的时代。State API 简单来说是“不受进程重启影响的“数据结构，其命名规范也与常见的数据结构一致，比如 MapState、ListState。Flink 官方提供的算子（比如 KafkaSource）和用户开发的算子都可以使用 State API 来保存状态信息。和大多数分布式系统一样 Flink 采用快照的方式来将整个作业的状态定期同步到外部存储，也就是将 State API 保存的信息以序列化的形式存储，作业恢复的时候只要读取外部存储即可将作业恢复到先前某个时间点的状态。由于从快照恢复同时会回滚数据流的处理进度，所以 State 是天然的 exactly-once 投递。

而端到端的一致性则需要上下游的外部系统配合，因为 Flink 无法将它们的状态也保存到快照并独立地回滚它们，否则就不叫作外部系统了。通常来说 Flink 的上游是可以重复读取或者消费的 pull-based 持续化存储，所以要实现 source 端的 exactly-once 只需要回滚 source 的读取进度即可（e.g. Kafka 的 offset）。而 sink 端的 exactly-once 则比较复杂，因为 sink 是 push-based 的。所谓覆水难收，要撤回发出去的消息是并不是容易的事情，因为这要求下游根据消息作出的一系列反应都是可撤回的。这就需要用 State API 来保存已发出消息的元数据，记录哪些数据是重启后需要回滚的。



### Exactly-Once Sink 原理

Flink 的 exactly-once sink 均基于快照机制，按照实现原理可以分为幂等（Idempotent） sink 和事务性（Transactional） sink 两种。

### 幂等 Sink

幂等性是分布式领域里十分有用的特性，它意味着相同的操作执行一次和执行多次可以获得相同的结果，因此 at-least-once 自然等同于 exactly-once。如此一来，在从快照恢复的时候幂等 sink 便不需要对外部系统撤回已发消息，相当于回避了外部系统的状态回滚问题。比如写入 KV 数据库的 sink，由于插入一行的操作是幂等的，因此 sink 可以无状态的，在错误恢复时也不需要关心外部系统的状态。从某种意义来讲，上文提到的 TCP 协议也是利用了发送数据包幂等性来保证 exactly-once。

然而幂等 sink 的适用场景依赖于业务逻辑，如果下游业务本来就无法保证幂等性，这时就需要应用事务性 sink。

### 事务性 Sink

事务性 sink 顾名思义类似于传统 DBMS 的事务，将一系列（一般是一个 checkpoint 内）的所有输出包装为一个逻辑单元，理想的情况下提供 ACID 的事务保证。之所以说是“理想的情况下”，主要是因为 sink 依赖于目标输出系统的事务保证，而分布式系统对于事务的支持并不一定很完整，比如 HBase 就不支持跨行事务，再比如 HDFS 等文件系统是不提供事务的，这种情况下 sink 只可以在客户端的基础上再包装一层来尽最大努力地提供事务保证。

然而仅有下游系统本身提供的事务保证对于 exactly-once sink 来说是不够的，因为同一个 sink 的子任务（subtask）会有多个，对于下游系统来说它们是处在不同会话和事务中的，并不能保证操作的原子性，因此 exactly-once sink 还需要实现分布式事务来达到所有 subtask 的一致 commit 或 rollback。由于 sink 事务生命周期是与 checkpoint 一一对应的，或者说 checkpoint 本来就是实现作业状态持久化的分布式事务，sink 的分布式事务也理所当然可以通过 checkpoint 机制提供的 hook 来实现。

Checkpoint 提供给算子的 hook 有 CheckpointedFunction 和 CheckpointListener 两个，前者在算子进行 checkpoint 快照时被调用，后者在 checkpoint 成功后调用。为了简单起见 Flink 结合上述两个接口抽象出 exactly-once sink 的通用逻辑抽象 TwoPhaseCommitSinkFunction 接口，从命名即可看出这是对两阶段提交协议的一个实现，其主要方法如下:

- beginTransaction: 初始化一个事务。在有新数据到达并且当前事务为空时调用。
- preCommit: 预提交数据，即不再写入当前事务并准好提交当前事务。在 sink 算子进行快照的时候调用。
- commit: 正式提交数据，将准备好的事务提交。在作业的 checkpoint 完成时调用。
- abort: 放弃事务。在作业 checkpoint 失败的时候调用。

下面以 Bucketing File Sink 作为例子来说明如何基于异步 checkpoint 来实现事务性 sink。

Bucketing File Sink 是 Flink 提供的一个 FileSystem Connector，用于将数据流写到固定大小的文件里。Bucketing File Sink 将文件分为三种状态，in-progress/pending/committed，分别表示正在写的文件、写完准备提交的文件和已经提交的文件。

![image](https://yqfile.alicdn.com/82db10a17cb2a25df7fa09b19e2b85f1005dcf01.png)

运行时，Bucketing File Sink 首先会打开一个临时文件并不断地将收到的数据写入（相当于事务的 beginTransaction 步骤），这时文件处于 in-progress。直到这个文件因为大小超过阈值或者一段时间内没有新数据写入，这时文件关闭并变为 pending 状态（相当于事务的 pre-commit 步骤）。由于 Flink checkpoint 是异步的，可能有多个并发的 checkpoint，Bucketing File Sink 会记录 pending 文件对应的 checkpoint epoch，当某个 epoch 的 checkpoint 完成后，Bucketing File Sink 会收到 callback 并将对应的文件改为 committed 状态。这是通过原子操作重命名来完成的，因此可以保证 pre-commit 的事务要么 commit 成功要么 commit 失败，不会出现其他中间状态。

Commit 出现错误会导致作业自动重启，重启后 Bucketing File Sink 本身已被恢复为上次 checkpoint 时的状态，不过仍需要将文件系统的状态也恢复以保证一致性。从 checkpoint 恢复后对应的事务会再次重试 commit，它会将记录的 pending 文件改为 committed 状态，记录的 in-progress 文件 truncate 到 checkpoint 记录下来的 offset，而其余未被记录的 pending 文件和 in-progress 文件都将被删除。

上面主要围绕事务保证的 AC 两点（Atomicity 和 Consistency），而在 I（Isolation）上 Flink exactly-once sink 也有不同的实现方式。实际上由于 Flink 的流计算特性，当前事务的未 commit 数据是一直在积累的，根据缓存未 commit 数据的地方的不同，可以将事务性 sink 分为两种实现方式。

- 在 sink 端缓存未 commit 数据，等 checkpoint 完成以后将缓存的数据 flush 到下游。这种方式可以提供 read-committed 的事务隔离级别，但同时由于未 commit 的数据不会发往下游（与 checkpoint 同步），sink 端缓存会带来一定的延迟，相当于退化为与 checkpoint 同步的 micro-batching 模式。
- 在下游系统缓存未 commit 数据，等 checkpoint 完成后通知下游 commit。这样的好处是数据是流式发往下游的，不会在每次 checkpoint 完成后出现网络 IO 的高峰，并且事务隔离级别可以由下游设置，下游可以选择低延迟弱一致性的 read-uncommitted 或高延迟强一致性的 read-committed。

在 Bucketing File Sink 的例子中，处于 in-progress 和 pending 状态的文件默认情况下都是隐藏文件（在实践中是使用下划线作为文件名前缀，HDFS 的 FileInputFormat 会将其过滤掉），只有 commit 成功后文件才对用户是可见的，即提供了 read-committed 的事务隔离性。理想的情况下 exactly-once sink 都应该使用在下游系统缓存未 commit 数据的方式，因为这最为符合流式计算的理念。最为典型的是下游系统本来就支持事务，那么未 commit 的数据很自然地就是缓存在下游系统的，否则 sink 可以选择像上例的 Bucketing File Sink 一样在下游系统的用户层面实现自己的事务，或者 fallback 到等待数据变为 committed 再发出的 micro-batching 模式。



Flink 分布式快照的核心元素

- **Barrier（数据栅栏）**：可以把 Barrier 简单地理解成一个标记，该标记是严格有序的，并且随着数据流往下流动。每个 Barrier 都带有自己的 ID，Barrier 极其轻量，并不会干扰正常的数据处理。
- **异步**：每次在把快照存储到我们的状态后端时，如果是同步进行就会阻塞正常任务，从而引入延迟。因此 **Flink 在做快照存储时，采用异步方式**
- **增量**：由于 checkpoint 是一个全局状态，用户保存的状态可能非常大，多数达 G 或者 T 级别，checkpoint 的创建会非常慢，而且执行时占用的资源也比较多，因此 Flink 提出了增量快照的概念。也就是说，每次进行的全量 checkpoint，是基于上次进行更新的。