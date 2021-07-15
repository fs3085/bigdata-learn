assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T> timestampAndWatermarkAssigner) 被标记为***deprecated\***了。

因为该方法中用到的watermark生成器接口也被标记为***deprecated***了。推荐使用新的接口：

```java
assignTimestampsAndWatermarks(WatermarkStrategy)
```

新的接口支持watermark空闲，并且不再区别periodic和punctuated。

例如：

```java
/**
 * 当没有新数据到达时，WatermarkGenerator是无法生成watermark的，这会导致任务无法继续运行。
 * 例如kafka topic有2个partition，
 * 其中一个partition数据量比较少，可能存在长时间没有新数据的情况，这就导致该watermark一直是
 * 较早数据产生的。即使另一个partition始终有新数据，那么任务也不会往下运行，
 * 因为水印对齐取的是最小值。为了解决这个问题，WatermarkStrategy提供了withIdleness方法，
 * 允许传入一个timeout的时间。
*/
WatermarkStrategy
        .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withIdleness(Duration.ofMinutes(1));
```



```java
WatermarkStrategy接口定义了如何在流数据中生成watermark。WatermarkStrategy是
WatermarkGenerator和TimestampAssigner的构造器，前者用于生成watermark，
后者用于指定一条记录的内部时间戳。
WatermarkStrategy接口包含三个部分：
1）需要实现的方法
/**
* 实例化一个根据此策略生成watermark的WatermarkGenerator。
* Instantiates a WatermarkGenerator that generates watermarks according to this strategy.
*/
@Override
WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);
关于该方法的实现可以查看源码，例如：
WatermarkStrategyWithTimestampAssigner，这是一个final class，实现了WatermarkStrategy接口，并且
重写了TimestampAssigner。
2）WatermarkStrategy基本构造方法
3）WatermarkStrategy普通的内置构造方法以及基于WatermarkGeneratorSupplier的构造方法
```



```java
/**
 * 为乱序的情况创建watermark strategy，设置一个最大延迟时间。
 * Creates a watermark strategy for situations where records are out of order, but you can place
 * an upper bound on how far the events are out of order. An out-of-order bound B means that
 * once the an event with timestamp T was encountered, no events older than {@code T - B} will
 * follow any more.
 *
 * <p>The watermarks are generated periodically. The delay introduced by this watermark
 * strategy is the periodic interval length, plus the out of orderness bound.
 *
 * @see BoundedOutOfOrdernessWatermarks
 */
static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
	return (ctx) -> new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);
}
```

5、关于WatermarkGenerator

5.1 Periodic WatermarkGenerator

这种方式就是定期生成watermark。时间间隔是通过

```java
ExecutionConfig.setAutoWatermarkInterval(Long l)
```

定义的。周期性的调用onPeriodicEmit()方法，如果新的watermark不为null，并且大于之前的watermark，则发出。

```java
env.getConfig().setAutoWatermarkInterval(0)
```

表示禁用watermark。

5.2 内置的watermark Generators

如果不存在乱序的情况，即event的时间戳是单调递增的，则

```java
WatermarkStrategy.forMonotonousTimestamps();
```

如果存在乱序，则设置延迟时间。

```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));
```

例如：

```java
dsOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Map<String,Object>>forBoundedOutOfOrderness(
                        Duration.ofSeconds(jobConfig.getFlinkJobConfigs().getWindowMaxOutOfOrderness()))
                        .withTimestampAssigner(new MyTimeAssigner(task.getTimestamp())))
```