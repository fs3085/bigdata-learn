
1、Checkpoint失败：Checkpoint expired before completing

原因是因为

```scala
checkpointConf.setCheckpointTimeout(8000L)
```


设置的太小了，默认是10min，这里只设置了8sec。

当一个Flink App背压的时候（例如由外部组件异常引起），Barrier会流动的非常缓慢，导致Checkpoint时长飙升。

2、资源隔离建议

在Flink中，资源的隔离是通过Slot进行的，也就是说多个Slot会运行在同一个JVM中，这种隔离很弱，尤其对于生产环境。Flink App上线之前要在一个单独的Flink集群上进行测试，否则一个不稳定、存在问题的Flink App上线，很可能影响整个Flink集群上的App。
3、资源不足导致 container 被 kill

The assigned slot container_container编号 was removed.
Flink App 抛出此类异常，通过查看日志，一般就是某一个 Flink App 内存占用大，导致 TaskManager（在 Yarn 上就是 Container ）被Kill 掉。

但是并不是所有的情况都是这个原因，还需要进一步看 yarn 的日志（ 查看 yarn 任务日志：yarn logs -applicationId  -appOwner），如果代码写的没问题，就确实是资源不够了，其实 1G Slot 跑多个Task（ Slot Group Share ）其实挺容易出现的。

因此有两种选择，可以根据具体情况，权衡选择一个。

将该 Flink App 调度在 Per Slot 内存更大的集群上。
通过 slotSharingGroup("xxx") ，减少 Slot 中共享 Task 的个数
4、启动报错，提示找不到 jersey 的类

```
java.lang.NoClassDefFoundError: com/sun/jersey/core/util/FeaturesAndProperties
```


解决办法进入 yarn中 把 lib 目中的一下两个问价拷贝到 flink 的 lib 中

```
hadoop/share/hadoop/yarn/lib/jersey-client-1.9.jar /hadoop/share/hadoop/yarn/lib/jersey-core-1.9.jar
```

5、Scala版本冲突

```
java.lang.NoSuchMethodError:scala.collection.immutable.HashSet$.empty()Lscala/collection/
```


解决办法，添加

```scala
import org.apache.flink.api.scala._
```

6、没有使用回撤流报错

Table is not an append一only table. Use the toRetractStream() in order to handle add and retract messages.
这个是因为动态表不是 append-only 模式的，需要用 toRetractStream ( 回撤流) 处理就好了.

```scala
tableEnv.toRetractStream[Person](result).print()
```

7、OOM 问题解决思路

```
java.lang.OutOfMemoryError: GC overhead limit exceeded
java.lang.OutOfMemoryError: GC overhead limit exceeded
        at java.util.Arrays.copyOfRange(Arrays.java:3664)
        at java.lang.String.<init>(String.java:207)
        at com.esotericsoftware.kryo.io.Input.readString(Input.java:466)
        at com.esotericsoftware.kryo.serializers.DefaultSerializers$StringSerializer.read(DefaultSerializers.java:177)
......
        at org.apache.flink.streaming.runtime.tasks.OperatorChain$CopyingChainingOutput.collect(OperatorChain.java:524)
```


解决方案：

检查 slot 槽位够不够或者 slot 分配的数量有没有生效
程序起的并行是否都正常分配了(会有这样的情况出现,假如 5 个并行,但是只有 2 个在几点上生效了,另外 3 个没有数据流动)
检查flink程序有没有数据倾斜，可以通过 flink 的 ui 界面查看每个分区子节点处理的数据量
8、解析返回值类型失败报错

```
The return type of function could not be determined automatically
Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException: The return type of function 'main(RemoteEnvironmentTest.java:27)' could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.
 at org.apache.flink.api.java.DataSet.getType(DataSet.java:178)
 at org.apache.flink.api.java.DataSet.collect(DataSet.java:410)
 at org.apache.flink.api.java.DataSet.print(DataSet.java:1652)
```


解决方案：产生这种现象的原因一般是使用 lambda 表达式没有明确返回值类型，或者使用特使的数据结构 flink 无法解析其类型，这时候我们需要在方法的后面添加返回值类型，比如字符串

```scala
input.flatMap((Integer number, Collector<String> out) -> {
 ......
})
// 提供返回值类型
.returns(Types.STRING)
```

9、Hadoop jar 包冲突

```
Caused by: java.io.IOException: The given file system URI (hdfs:///data/checkpoint-data/abtest) did not describe the authority (like for example HDFS NameNode address/port or S3 host). The attempt to use a configured default authority failed: Hadoop configuration did not contain an entry for the default file system ('fs.defaultFS').
        at org.apache.flink.runtime.fs.hdfs.HadoopFsFactory.create(HadoopFsFactory.java:135)
        at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:399)
        at org.apache.flink.core.fs.FileSystem.get(FileSystem.java:318)
        at org.apache.flink.core.fs.Path.getFileSystem(Path.java:298)
```


解决：pom 文件中去掉和 hadoop 相关的依赖就好了

10、时钟不同步导致无法启动

启动Flink任务的时候报错

```
Caused by: java.lang.RuntimeException: Couldn't deploy Yarn cluster
```


然后仔细看发现里面有这么一句

```
system times on machines may be out of sync
```


意思说是机器上的系统时间可能不同步。同步集群机器时间即可。

