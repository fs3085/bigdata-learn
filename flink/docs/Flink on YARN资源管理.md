**Flink on YARN资源管理**

![图片](https://mmbiz.qpic.cn/sz_mmbiz_png/frAIQPLLOycE4plicu0H1xYgyo38XPRIzGXL6ApqpU16RFN6HNK8FwroZVajKedEfzQkZDaTYF30Y2ysDPYwibbQ/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

Flink在YARN上也有两种模式：一种是yarn-session、还有一个是yarn-per-job。YARN session模式比较有意思，相当于在YARN集群中基于Container运行一套Flink集群。Container有JobManager角色、还有TaskManager角色。然后客户端可以不断地往这套运行在YARN上的Flink Cluster提交作业。

上面这个命令表示，在YARN上分配4个Container，每个Container上运行TaskManager，每个TaskManager对应8个vcore，每个TaskManager 32个G。这就要求YARN上scheduler分配Container最大内存要很大，否则根本无法分配这么大的内存。这种模式比较适合做一些交互性地测试。



第二种模式yarn-per-job，相当于就是单个JOB提交的模式。同样，在YARN中也有JobManager和TaskManager的概念，只不过当前是针对一个JOB，启动则两个角色。JobManager运行在Application Master上，负责资源的申请。



上述命令表示，运行两个TaskManager（即2个Container），job manager所在的container是1G内存、Task Manager所在的Container是3G内存、每个TaskManager使用3个vcore。