1.当运行在yarn集群上时，Yarn的ResourceMananger用来管理集群资源，集群上每个节点上的NodeManager用来管控所在节点的资源，
从yarn的角度来看，每个节点看做可分配的资源池，当向ResourceManager请求资源时，它返回一些NodeManager信息，
这些NodeManager将会提供execution container给你，每个execution container就是满足请求的堆大小的JVM进程，
JVM进程的位置是由ResourceMananger管理的，不能自己控制，如果一个节点有64GB的内存被yarn管理（通过yarn.nodemanager.resource.memory-mb配置),
当请求10个4G内存的executors时，这些executors可能运行在同一个节点上。

2.当在集群上执行应用时，job会被切分成stages,每个stage切分成task,每个task单独调度，
可以把executor的jvm进程看做task执行池，每个executor有 spark.executor.cores / spark.task.cpus execution 个执行槽

3.task基本上就是spark的一个工作单元，作为exector的jvm进程中的一个线程执行，这也是为什么spark的job启动时间快的原因，
在jvm中启动一个线程比启动一个单独的jvm进程块（在hadoop中执行mapreduce应用会启动多个jvm进程）

总结：所以就是一个container对应一个JVM进程（也就是一个executor）



**Spark on YARN资源管理**

通常，生产环境中，我们是把Spark程序在YARN中执行。而Spark程序在YARN中运行有两种模式，一种是Cluster模式、一种是Client模式。这两种模式的关键区别就在于Spark的driver是运行在什么地方。如果运行模式是Cluster模式，Driver运行在Application Master里面的。如果是Client模式，Driver就运行在提交spark程序的地方。Spark Driver是需要不断与任务运行的Container交互的，所以运行Driver的client是必须在网络中可用的，知道应用程序结束。



这两幅图描述得很清楚。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_png/frAIQPLLOycE4plicu0H1xYgyo38XPRIzK3ZeCmnHSdGrjWj92c3Xuq9XKPb7GwicaSCnRq2sP3HUu7FNib7vkV3Q/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

![图片](https://mmbiz.qpic.cn/sz_mmbiz_png/frAIQPLLOycE4plicu0H1xYgyo38XPRIzee9nSm0ghymicXF5Aw4AFclkH482QMSibuW8xoHMsjRu0oxicPLD1QMcg/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

留意一下Driver的位置。



通过上面的分析，我们可以明确，如果是Client模式，Driver和ApplicationMaster运行在不同的地方。ApplicationMaster运行在Container中，而Driver运行在提交任务的client所在的机器上。



因为如果是Standalone集群，整个资源管理、任务执行是由Master和Worker来完成的。而当运行在YARN的时候，就没有这两个概念了。资源管理遵循YARN的资源调度方式。之前在Standalone集群种类，一个worker上可以运行多个executor，现在对应的就是一个NodeManager上可以运行多个container，executor的数量跟container是一致的。可以直接把executor理解为container。



我们再来看看spark-submit的一些参数配置。

配置选项中，有一个是公共配置，还有一些针对spark-submit运行在不同的集群，参数是不一样的。

公共的配置：

--driver-memory、--executor-memory，这是我们可以指定spark driver以及executor运行所需的配置。executor其实就是指定container的内存，而driver如果是cluster模式，就是application master的内置，否则就是client运行的那台机器上申请的内存。



如果运行在Cluster模式，可以指定driver所需的cpu core。

如果运行在Spark Standalone，--total-executor-cores表示一共要运行多少个executor。

如果运行在Standalone集群或者YARN集群，--executor-cores表示每个executor所需的cpu core。

如果运行在yum上，--num-executors表示要启动多少个executor，其实就是要启动多少个container。













**YARN集群资源管理**

###### 集群总计资源

要想知道YARN集群上一共有多少资源很容易，我们通过YARN的web ui就可以直接查看到。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_png/frAIQPLLOycE4plicu0H1xYgyo38XPRIz8yn5D9iclQYVQiaMOYgrUJLEZE4ibHXohYNYbewYf5zoia5WqMyM2JqTWQ/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

通过查看Cluster Metrics，可以看到总共的内存为24GB、虚拟CPU核为24个。我们也可以看到每个NodeManager的资源。很明显，YARN集群中总共能使用的内存就是每个NodeManager的可用内存加载一起，VCORE也是一样。

NodeManager总计资源

NodeManager的可用内存、可用CPU分别是8G、和8Core。这个资源和Linux系统是不一致的。我们通过free -g来查看下Linux操作系统的总计内存、和CPU核。



第一个节点（总计内存是10G，空闲的是8G）



第二个节点（总计内存是7G，空闲是不到6G）



第三个节点（和第二个节点一样）



这说明了，NodeManager的可用内存和操作系统总计内存是没有直接关系的！

那NodeManager的可用内存是如何确定的呢？

在yarn-default.xml中有一项配置为：yarn.nodemanager.resource.memory-mb，它的默认值为：

-1（hadoop 3.1.4）。我们来看下Hadoop官方解释：





这个配置是表示NodeManager总共能够使用的物理内存，这也是可以给container使用的物理内存。如果配置为-1，且yarn.nodemanager.resource.detect-hardware-capabilities配置为true，那么它会根据操作的物理内存自动计算。而yarn.nodemanager.resource.detect-hardware-capabilities默认为false，所以，此处默认NodeManager就是8G。这就是解释了为什么每个NM的可用内存是8G。



还有一个重要的配置：yarn.nodemanager.vmem-pmem-ratio，它的默认配置是2.1

这个配置是针对NodeManager上的container，如果说某个Container的物理内存不足时，可以使用虚拟内存，能够使用的虚拟内存默认为物理内存的2.1倍。



针对虚拟CPU核数，也有一个配置yarn.nodemanager.resource.cpu-vcores配置，它的默认配置也为-1。看一下Hadoop官方的解释：

与内存类似，它也有一个默认值：就是8。



###### scheduler调度资源 

通过YARN的webui，点击scheduler，我们可以看到的调度策略、最小和最大资源分配。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_png/frAIQPLLOycE4plicu0H1xYgyo38XPRIzcquiaO1UaVIZpc61YIvPywgwMAAhywC5CooicHcnMMDK00ZvTfgl5IqA/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

通过web ui，我们可以看到当前YARN的调度策略为容量调度。调度资源的单位是基于MB的内存、和Vcore（虚拟CPU核）。最小的一次资源分配是：1024M（1G）和1个VCORE。最大的一次分配是：4096M（4G）和4个VCORE。

注意：内存资源和VCORE都是以Container承载的。

yarn.scheduler.minimum-allocation-mb

默认值：1024

说明：该配置表示每个容器的最小分配。因为RM是使用scheduler来进行资源调度的，如果请求的资源小于1G，也会设置为1G。这表示，如果我们请求一个256M的container，也会分配1G。

yarn.scheduler.maximum-allocation-mb

默认值：8192

说明：最大分配的内存，如果比这个内存高，就会抛出InvalidResourceRequestException异常。这里也就意味着，最大请求的内存不要超过8G。上述截图显示是4G，是因为我在yarn-site.xml中配置了最大分配4G。



yarn.scheduler.minimum-allocation-vcores

默认值：1

说明：同内存的最小分配

yarn.scheduler.maximum-allocation-vcores

默认值：4

说明：同内存的最大分配



Container总计资源

在YARN中，资源都是通过Container来进行调度的，程序也是运行在Container中。Container能够使用的最大资源，是由scheduler决定的。如果按照Hadoop默认配置，一个container最多能够申请8G的内存、4个虚拟核。例如：我们请求一个Container，内存为3G、VCORE为2，是OK的。考虑一个问题：如果当前NM机器上剩余可用内存不到3G，怎么办？此时，就会使用虚拟内存。不过，虚拟内存，最多为内存的2.1倍，如果物理内存 + 虚拟内存仍然不足3G，将会给container分配资源失败。



根据上述分析，如果我们申请的container内存为1G、1个VCORE。那么NodeManager最多可以运行8个Container。如果我们申请的container内存为4G、4个vcore，那么NodeManager最多可以运行2个Container。



Container是一个JVM进程吗

这个问题估计有很多天天在使用Hadoop的人都不一定知道。当向RM请求资源后，会在NodeManager上创建Container。问题是：Container是不是有自己独立运行的JVM进程呢？还是说，NodeManager上可以运行多个Container？Container和JVM的关系是什么？



此处，明确一下，每一个Container就是一个独立的JVM实例。（此处，咱们不讨论Uber模式）。每一个任务都是在Container中独立运行，例如：MapTask、ReduceTask。当scheduler调度时，它会根据任务运行需要来申请Container，而每个任务其实就是一个独立的JVM。



为了验证此观点，我们来跑一个MapReduce程序。然后我们在一个NodeManager上使用JPS查看一下进程：（这是我处理过的，不然太长了，我们主要是看一下内存使用量就可以了）



我们看到了有MRAppMaster、YarnChild这样的一些Java进程。这就表示，每一个Container都是一个独立运行的JVM，它们彼此之间是独立的。