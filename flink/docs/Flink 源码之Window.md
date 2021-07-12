# Window类

Window在这里可理解为元素的一种分组方式。
 Window为一个抽象类，其中仅定义了一个方法`maxTimestamp()`，其意义为该window时间跨度所能包含的最大时间点（用时间戳表示）。
 Window类包含两个子类：GlobalWindow和TimeWindow。GlobalWindow是全局窗口，TimeWindow是具有起止时间的时间段窗口。

Window的代码如下所示：



```java
public abstract class Window {

    /**
     * Gets the largest timestamp that still belongs to this window.
     *
     * @return The largest timestamp that still belongs to this window.
     */
    public abstract long maxTimestamp();
}
```

# GlobalWindow

第一种要介绍的window为GlobalWindow。顾名思义，GlobalWindow为一种全局的window。该window只存在一个实例(单例模式)。

GlobalWindow的部分代码如下所示：



```java
    private static final GlobalWindow INSTANCE = new GlobalWindow();

    private GlobalWindow() { }

        // 由此可知GlobalWindow为单例模式
    public static GlobalWindow get() {
        return INSTANCE;
    }

        // 由于GlobalWindow为单实例，同时根据GlobalWindow的定义，任何元素都属于GlobalWindow，故maxTimestamp返回Long的最大值
    @Override
    public long maxTimestamp() {
        return Long.MAX_VALUE;
    }
```

# TimeWindow

和GlobalWindow不同的是，TimeWindow定义了明确的起止时间(start和end)，TimeWindow是有明确时间跨度的。

TimeWindow还定义了Window的计算方法，比如判断是否有交集，求并集等。代码如下所示：



```java
// 判断两个时间窗口是否有交集
public boolean intersects(TimeWindow other) {
    return this.start <= other.end && this.end >= other.start;
}

/**
    * Returns the minimal window covers both this window and the given window.
    */
// 返回两个window的并集
public TimeWindow cover(TimeWindow other) {
    return new TimeWindow(Math.min(start, other.start), Math.max(end, other.end));
}
```



# WindowAssigner

对一个流进行window操作，元素以它们的key（keyBy函数指定）和它们所属的window进行分组。位于相同key和相同窗口的一组元素称之为窗格（pane）。

WindowAssigner的作用就是规定如何根据一个元素来获取包含它的窗口集合（assignWindows方法）。除此之外windowAssigner还包含窗口的触发机制(何时计算窗口内元素)和时间类型（event time或processing time）





# GlobalWindows

此处的GlobalWindows实际上是一个GlobalWindow的分配器。负责为元素分配所属的GlobalWindow。
 GlobalWindows的一个应用场景为分配Count Window，即每累计够n个元素会触发计算的window。源码如下所示：

```java
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size) {
    // 使用CountTrigger，每隔size个元素，触发一次计算，同时又使用PurgingTrigger，每次触发计算之后将window内容清空
    return window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
}
```

CountTrigger和PurgingTrigger的讲解参见[Flink 源码之Trigger](https://www.jianshu.com/p/586db7a56a0a)。

GlobalWindows的源码如下：

```java
@PublicEvolving
public class GlobalWindows extends WindowAssigner<Object, GlobalWindow> {
    private static final long serialVersionUID = 1L;

    private GlobalWindows() {}

    @Override
    public Collection<GlobalWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        // 由于GlobalWindow为单例，故分配window时直接将GlobalWindow示例返回即可
        return Collections.singletonList(GlobalWindow.get());
    }

    @Override
    public Trigger<Object, GlobalWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        // 默认不会触发任何计算
        return new NeverTrigger();
    }

    @Override
    public String toString() {
        return "GlobalWindows()";
    }

    /**
     * Creates a new {@code GlobalWindows} {@link WindowAssigner} that assigns
     * all elements to the same {@link GlobalWindow}.
     *
     * @return The global window policy.
     */
    public static GlobalWindows create() {
        return new GlobalWindows();
    }

    /**
     * A trigger that never fires, as default Trigger for GlobalWindows.
     */
    @Internal
     // 此处定义了NeverTrigger，该Trigger在任何情况下都返回TriggerResult.CONTINUE，不触发任何计算
    public static class NeverTrigger extends Trigger<Object, GlobalWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public TriggerResult onElement(Object element, long timestamp, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {}

        @Override
        public void onMerge(GlobalWindow window, OnMergeContext ctx) {
        }
    }

    @Override
    public TypeSerializer<GlobalWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new GlobalWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
```



- GlobalWindows继承了WindowAssigner，key类型为Object，窗口类型为GlobalWindow；GlobalWindow继承了Window，它的maxTimestamp方法与TimeWindow不同，TimeWindow有start和end属性，其maxTimestamp方法返回的是end-1；而GlobalWindow的maxTimestamp方法返回的是Long.MAX_VALUE；GlobalWindow定义了自己的Serializer

- GlobalWindows的assignWindows方法返回的是GlobalWindow；getDefaultTrigger方法返回的是NeverTrigger；getWindowSerializer返回的是GlobalWindow.Serializer()；isEventTime返回的为false

- NeverTrigger继承了Trigger，其onElement、onProcessingTime、onProcessingTime返回的TriggerResult均为TriggerResult.CONTINUE；该行为就是不做任何触发操作；如果需要触发操作，则需要在定义window操作时设置自定义的trigger，覆盖GlobalWindows默认的NeverTrigger

  

**Keyed Window和Non-keyed Window有什么区别？**

Keyed Windows是针对KeyedStream，相同的key的事件会放在同一个Window中，而Non-Keyed Window是所有的数据放在一个窗口中，相当于并行度为1。KeyedWindow使用的API是window、而Non-keyed Window使用的是windowAll。



**Flink的窗口模型中有哪4大组件？并做简单介绍。**

Assigner：Assigner是给窗口分配数据的组件，其实就是用它来定义数据是哪个window的。Function：Window是把无界流数据变成了有界流，Function就是对这个有界流进行计算的函数，由用户自己实现。

Triger：Window触发器，就是什么时候触发Window执行Function。

Evictor：可以在Triger触发后，Function执行之前/之后删除数据。



**全局窗口和Non-Keyed Window的区别？**

不是一个概念。全局窗口表示的是把所有的元素都分配给一个Window。而Non-Keyed Window表示针对的是没有Key的Window，我们可以设置Non-keyed Window TumblingEventTimeWindows、SlidingEventTimeWindows、当然也可以设置全局窗口。所以，关键点在于全局窗口是一个Assigner。



**Window中的Reduce/Aggregate和Process的不同点？最佳实践是什么？**

首先，他们都是window的计算函数。Reduce/Aggregate是聚合函数，专门用于window的聚合，而且效率较高。而process相当于自定义处理，可以实现更灵活的窗口操作，但它不擅长聚合。所以有些场景可以将Reduce/Aggregate结合process来处理。