Flink 支持两种 watermark 生成方式。第一种是在 SourceFunction 中产生，相当于把整个的 timestamp 分配和 watermark 生成的逻辑放在流处理应用的源头。我们可以在 SourceFunction 里面通过这两个方法产生 watermark：

- 通过 collectWithTimestamp 方法发送一条数据，其中第一个参数就是我们要发送的数据，第二个参数就是这个数据所对应的时间戳；也可以调用 emitWatermark 方法去产生一条 watermark，表示接下来不会再有时间戳小于等于这个数值记录。
- 另外，有时候我们不想在 SourceFunction 里生成 timestamp 或者 watermark，或者说使用的 SourceFunction 本身不支持，我们还可以在使用 DataStream API 的时候指定，调用的 DataStream.assignTimestampsAndWatermarks 这个方法，能够接收不同的 timestamp 和 watermark 的生成器。

总体上而言生成器可以分为两类：第一类是定期生成器；第二类是根据一些在流处理数据流中遇到的一些特殊记录生成的。

![img](https://ververica.cn/wp-content/uploads/2019/09/007.png)

两者的区别主要有三个方面，首先定期生成是现实时间驱动的，这里的“定期生成”主要是指 watermark（因为 timestamp 是每一条数据都需要有的），即定期会调用生成逻辑去产生一个 watermark。而根据特殊记录生成是数据驱动的，即是否生成 watermark 不是由现实时间来决定，而是当看到一些特殊的记录就表示接下来可能不会有符合条件的数据再发过来了，这个时候相当于每一次分配 Timestamp 之后都会调用用户实现的 watermark 生成方法，用户需要在生成方法中去实现 watermark 的生成逻辑。

大家要注意的是就是我们在分配 timestamp 和生成 watermark 的过程，虽然在 SourceFunction 和 DataStream 中都可以指定，但是还是建议生成的工作越靠近 DataSource 越好。这样会方便让程序逻辑里面更多的 operator 去判断某些数据是否乱序。Flink 内部提供了很好的机制去保证这些 timestamp 和 watermark 被正确地传递到下游的节点。



###### 在SourceFunction中产生

​	**collectWithTimestamp(T element, long timestamp)**

​	**emitWatermark(Watermark mark)**



###### 在流程中指定

​	***DataStream.assignTimestampsAndWatermarks(...)**



