传统部署模式
Session模式
Session模式是预分配资源的，也就是提前根据指定的资源参数初始化一个Flink集群，并常驻在YARN系统中，拥有固定数量的JobManager和TaskManager（注意JobManager只有一个）。提交到这个集群的作业可以直接运行，免去每次分配资源的overhead。但是Session的资源总量有限，多个作业之间又不是隔离的，故可能会造成资源的争用；如果有一个TaskManager宕机，它上面承载着的所有作业也都会失败。另外，启动的作业越多，JobManager的负载也就越大。所以，Session模式一般用来部署那些对延迟非常敏感但运行时长较短的作业。

Per-Job模式
顾名思义，在Per-Job模式下，每个提交到YARN上的作业会各自形成单独的Flink集群，拥有专属的JobManager和TaskManager。可见，以Per-Job模式提交作业的启动延迟可能会较高，但是作业之间的资源完全隔离，一个作业的TaskManager失败不会影响其他作业的运行，JobManager的负载也是分散开来的，不存在单点问题。当作业运行完成，与它关联的集群也就被销毁，资源被释放。所以，Per-Job模式一般用来部署那些长时间运行的作业。

存在的问题
上文所述Session模式和Per-Job模式可以用如下的简图表示，其中红色、蓝色和绿色的图形代表不同的作业。




Deployer代表向YARN集群发起部署请求的节点，一般来讲在生产环境中，也总有这样一个节点作为所有作业的提交入口（即客户端）。在main()方法开始执行直到env.execute()方法之前，客户端也需要做一些工作，即：

获取作业所需的依赖项；
通过执行环境分析并取得逻辑计划，即StreamGraph→JobGraph；
将依赖项和JobGraph上传到集群中。
只有在这些都完成之后，才会通过env.execute()方法触发Flink运行时真正地开始执行作业。试想，如果所有用户都在Deployer上提交作业，较大的依赖会消耗更多的带宽，而较复杂的作业逻辑翻译成JobGraph也需要吃掉更多的CPU和内存，客户端的资源反而会成为瓶颈——不管Session还是Per-Job模式都存在此问题。为了解决它，社区在传统部署模式的基础上实现了Application模式。

Application模式


可见，原本需要客户端做的三件事被转移到了JobManager里，也就是说main()方法在集群中执行（入口点位于ApplicationClusterEntryPoint），Deployer只需要负责发起部署请求了。另外，如果一个main()方法中有多个env.execute()/executeAsync()调用，在Application模式下，这些作业会被视为属于同一个应用，在同一个集群中执行（如果在Per-Job模式下，就会启动多个集群）。可见，Application模式本质上是Session和Per-Job模式的折衷。

用Application模式提交作业的示例命令如下。

```sehll
bin/flink run-application -t yarn-application \
-Djobmanager.memory.process.size=2048m \
-Dtaskmanager.memory.process.size=4096m \
-Dtaskmanager.numberOfTaskSlots=2 \
-Dparallelism.default=10 \
-Dyarn.application.name="MyFlinkApp" \
/path/to/my/flink-app/MyFlinkApp.jar
```


-t参数用来指定部署目标，目前支持YARN（yarn-application）和K8S（kubernetes-application）。-D参数则用来指定与作业相关的各项参数，具体可参见官方文档。

那么如何解决传输依赖项造成的带宽占用问题呢？Flink作业必须的依赖是发行包flink-dist.jar，还有扩展库（位于$FLINK_HOME/lib）和插件库（位于$FLINK_HOME/plugin），我们将它们预先上传到像HDFS这样的共享存储，再通过yarn.provided.lib.dirs参数指定存储的路径即可。

-Dyarn.provided.lib.dirs="hdfs://myhdfs/flink-common-deps/lib;hdfs://myhdfs/flink-common-deps/plugins"
这样所有作业就不必各自上传依赖，可以直接从HDFS拉取，并且YARN NodeManager也会缓存这些依赖，进一步加快作业的提交过程。同理，包含Flink作业的用户JAR包也可以上传到HDFS，并指定远程路径进行提交。