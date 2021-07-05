提交任务的形式有多种

##### 一、Web页面提交方式

在集群启动之后, 在localhost:8081 可以很清楚的查看到 集群的运行状态, TaskManager、JobManager等等。
打包处理的时候还会涉及并行度的问题,我们可以根据情况自行设置。
并行度问题：env 可以设置环境的并行度,每一步操作都可以设置 setParallelism 并行度。
并行度优先级:  代码 -》全局 -》提交JobWeb -》集群配置文件默认并行度。

show plan 查看执行的



##### 二、命令行提交方式

```shell
./bin/flink run -c com.atguigu.wc.StreamWordCount -p 3 /home/appuser/flink-1.0-SNAPSHOT.jar --host localhost --poer 7777
```

run -c 运行主类 -p 并行度 jar包所在路径 --host localhost --port 7777

取消正在运行的Job, 在命令行中首先显示出来所有的 Job相关信息

```shell
./bin/flink list
```


然后执行取消命令

```shell
./bin/flink cannel +JobID
```


查看被取消的任务

```shell
./bin/flink list -a
```


占据的slot 个数 和设置的最大并行度有关。

standalone模式资源不够用要手动停止,然后再去配置，在实际生产环境中,应该是有一个资源管理平台,任务需要多少资源,向平台去拿。Flink应该部署在资源管理平台上,对于资源的调配和管理不应该再由Flink来完成,最常见的资源管理平台就是 Yarn , K8s 。



##### 三、Yarn模式

以Yarn模式部署Flink 任务的时候, 是要求 Flink 是有 Hadoop 支持的版本, Hadoop 环境需要保证版本在 2.2以上,并且集群中安装有HDFS服务。
**Flink on Yarn** : Session-Cluster 和 Per-Job-Cluster模式。

###### Session-cluster 模式：

![img](https://img-blog.csdnimg.cn/20210108121443652.png)

   Session-Cluster 模式有点像Standalone模式,需要先启动集群,然后再提交作业,接着会向 yarn 申请一块空间后, 资源永远保持不变。如果资源满了,下一个作业就无法提交了,只能等到yarn中的其中一个作业执行完成后,释放了资源,下个作业才会正常提交。所有作业共享 Dispatcher 和 ResourceManager; 共享资源;适合规模小执行时间短的作业。(执行时间短,不至于下一个作业长时间等待)
   在Yarn 中初始化一个 flink 集群,开辟指定的资源,以后提交任务都向这里提交。这个 flink 集群会常驻在 yarn 集群中,除非手工停止。

###### Per-Job-Cluster 模式：

![img](https://img-blog.csdnimg.cn/20210108122045481.png)

   一个Job会对应一个集群,每提交一个作业会根据自身的情况,都会单独向 yarn 申请资源,直到作业执行完成,一个作业的失败与否并不会影响下一个作业的正常提交和运行。独享 Dispatcher 和 ResourceManager， 按需接受资源申请； 适合规模大且长时间运行的作业。
   每次提交都会创建一个新的 flink 集群，任务之间互相独立,互不影响,方便管理。任务执行完成之后创建的集群也会消失。



**Session-Cluster**

启动 hadoop 集群(略)
启动 yarn-session

```shell
./yarn-session.sh -n 2 -s 2 -jm 1024 -tm 1024 -nm test -d

其中：
    -n(--container):    TaskManager的数量。
    -s(slots):    每个TaskManager的slot数量,默认一个slot一个core,默认每个taskmanager 的 slot的个数为1,有时可以多一些 taskManager, 做冗余。
    -jm:    JobManager 的 内存 (单位MB)。
    -tm:    每个 taskManager 的 内存 (单位MB)。
    -nm:    yarn 的 appName (现在yarn的 ui 上的名字)。
    -d:    后台执行。
```

执行任务

```shell
./flink run -c com.bigdata.wc.StreamWordCount
FlinkTutorual-1.0-SNAPSHOT-jar-with-dependencies.jar  --host localhost -port 7777
```

执行任务
./flink run -c com.bigdata.wc.StreamWordCount
FlinkTutorual-1.0-SNAPSHOT-jar-with-dependencies.jar  --host localhost -port 7777


去yarn控制台查看任务状态


取消 yarn-session

```shell
yarn application --kill application_1577588252906_0001
```

 

**Per Job Cluster**

启动hadoop集群
不启动yarn-session,直接执行 Job

```shell
./flink run -m yarn-cluster -c com.bigdata.wc.StreamWordCount
```


