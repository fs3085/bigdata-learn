https://my.oschina.net/u/3730932/blog/4267644

传统部署模式
Session模式
Session模式是预分配资源的，也就是提前根据指定的资源参数初始化一个Flink集群，
并常驻在YARN系统中，拥有固定数量的JobManager和TaskManager（注意JobManager只有一个）。
提交到这个集群的作业可以直接运行，免去每次分配资源的overhead。
但是Session的资源总量有限，多个作业之间又不是隔离的，故可能会造成资源的争用；
如果有一个TaskManager宕机，它上面承载着的所有作业也都会失败。
另外，启动的作业越多，JobManager的负载也就越大。
所以，Session模式一般用来部署那些对延迟非常敏感但运行时长较短的作业




