Timer简介
Timer（定时器）是Flink Streaming API提供的用于感知并利用处理时间/事件时间变化的机制。Ververica blog上给出的描述如下：

Timers are what make Flink streaming applications reactive and adaptable to processing and event time changes.

对于普通用户来说，最常见的显式利用Timer的地方就是KeyedProcessFunction。我们在其processElement()方法中注册Timer，然后覆写其onTimer()方法作为Timer触发时的回调逻辑。根据时间特征的不同：

处理时间——调用Context.timerService().registerProcessingTimeTimer()注册；onTimer()在系统时间戳达到Timer设定的时间戳时触发。
事件时间——调用Context.timerService().registerEventTimeTimer()注册；onTimer()在Flink内部水印达到或超过Timer设定的时间戳时触发。



除了KeyedProcessFunction之外，Timer在窗口机制中也有重要的地位。提起窗口自然就能想到Trigger，即触发器。来看下Flink自带的EventTimeTrigger的部分代码，它是事件时间特征下的默认触发器。

```java
@Override
public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
    if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
        return TriggerResult.FIRE;
    } else {
        ctx.registerEventTimeTimer(window.maxTimestamp());
        return TriggerResult.CONTINUE;
    }
}
 
@Override
public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
    return time == window.maxTimestamp() ?
        TriggerResult.FIRE :
        TriggerResult.CONTINUE;
}
```
可见，当水印还没有到达窗口右边沿时，就注册以窗口右边沿为时间戳的Timer。Timer到期后触发onEventTime()方法，进而触发该窗口相关联的Trigger。



TimerService、InternalTimerService
负责实际执行KeyedProcessFunction的算子是KeyedProcessOperator，其中以内部类的形式实现了KeyedProcessFunction需要的上下文类Context，如下所示。

```java
private class ContextImpl extends KeyedProcessFunction<K, IN, OUT>.Context {
    private final TimerService timerService;
    private StreamRecord<IN> element;
 
    ContextImpl(KeyedProcessFunction<K, IN, OUT> function, TimerService timerService) {
        function.super();
        this.timerService = checkNotNull(timerService);
    }
 
    @Override
    public Long timestamp() {
        checkState(element != null);
        if (element.hasTimestamp()) {
            return element.getTimestamp();
        } else {
            return null;
        }
    }
 
    @Override
    public TimerService timerService() {
        return timerService;
    }
 
    // 以下略...
}
```
可见timerService()方法返回的是外部传入的TimerService实例，那么我们就回到KeyedProcessOperator看一下它的实现，顺便放个类图。



```java
public class KeyedProcessOperator<K, IN, OUT>
    extends AbstractUdfStreamOperator<OUT, KeyedProcessFunction<K, IN, OUT>>
    implements OneInputStreamOperator<IN, OUT>, Triggerable<K, VoidNamespace> {
    private static final long serialVersionUID = 1L;
    private transient TimestampedCollector<OUT> collector;
    private transient ContextImpl context;
    private transient OnTimerContextImpl onTimerContext;

public KeyedProcessOperator(KeyedProcessFunction<K, IN, OUT> function) {
    super(function);
    chainingStrategy = ChainingStrategy.ALWAYS;
}
 
@Override
public void open() throws Exception {
    super.open();
    collector = new TimestampedCollector<>(output);
    InternalTimerService<VoidNamespace> internalTimerService =
        getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
    TimerService timerService = new SimpleTimerService(internalTimerService);
    context = new ContextImpl(userFunction, timerService);
    onTimerContext = new OnTimerContextImpl(userFunction, timerService);
}
 
@Override
public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
    collector.setAbsoluteTimestamp(timer.getTimestamp());
    invokeUserFunction(TimeDomain.EVENT_TIME, timer);
}
 
@Override
public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
    collector.eraseTimestamp();
    invokeUserFunction(TimeDomain.PROCESSING_TIME, timer);
}
 
@Override
public void processElement(StreamRecord<IN> element) throws Exception {
    collector.setTimestamp(element);
    context.element = element;
    userFunction.processElement(element.getValue(), context, collector);
    context.element = null;
}
 
private void invokeUserFunction(
    TimeDomain timeDomain,
    InternalTimer<K, VoidNamespace> timer) throws Exception {
    onTimerContext.timeDomain = timeDomain;
    onTimerContext.timer = timer;
    userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
    onTimerContext.timeDomain = null;
    onTimerContext.timer = null;
}
 
// 以下略...
```




TimerService接口的实现类为SimpleTimerService，它实际上又是InternalTimerService的非常简单的代理（真的很简单，代码略去）。
InternalTimerService的实例由getInternalTimerService()方法取得，该方法定义在所有算子的基类AbstractStreamOperator中。它比较重要，后面再提。
KeyedProcessOperator.processElement()方法调用用户自定义函数的processElement()方法，顺便将上下文实例ContextImpl传了进去，所以用户可以由它获得TimerService来注册Timer。
Timer在代码中叫做InternalTimer（是个接口）。
当Timer触发时，实际上是根据时间特征调用onProcessingTime()/onEventTime()方法（这两个方法来自Triggerable接口），进而触发用户函数的onTimer()回调逻辑。后面还会见到它们。



InternalTimerService是如何取得的。

```java
/**
 * Returns a {@link InternalTimerService} that can be used to query current processing time
 * and event time and to set timers. An operator can have several timer services, where
 * each has its own namespace serializer. Timer services are differentiated by the string
 * key that is given when requesting them, if you call this method with the same key
 * multiple times you will get the same timer service instance in subsequent requests.
 *
 * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
 * When a timer fires, this key will also be set as the currently active key.
 *
 * <p>Each timer has attached metadata, the namespace. Different timer services
 * can have a different namespace type. If you don't need namespace differentiation you
 * can use {@link VoidNamespaceSerializer} as the namespace serializer.
 *
 * @param name                The name of the requested timer service. If no service exists under the given
 *                            name a new one will be created and returned.
 * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
 * @param triggerable         The {@link Triggerable} that should be invoked when timers fire
 * @param <N>                 The type of the timer namespace.
 */
public <K, N> InternalTimerService<N> getInternalTimerService(
    String name,
    TypeSerializer<N> namespaceSerializer,
    Triggerable<K, N> triggerable) {
    checkTimerServiceInitialization();
 
    KeyedStateBackend<K> keyedStateBackend = getKeyedStateBackend();
    TypeSerializer<K> keySerializer = keyedStateBackend.getKeySerializer();
    InternalTimeServiceManager<K> keyedTimeServiceHandler = (InternalTimeServiceManager<K>) timeServiceManager;
    TimerSerializer<K, N> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
    return keyedTimeServiceHandler.getInternalTimerService(name, timerSerializer, triggerable);
}
```
该方法的注释描述非常清楚，所以一起粘贴过来。简单来讲：

每个算子可以有一个或多个InternalTimerService。
InternalTimerService的四要素是：名称、命名空间类型N（及其序列化器）、键类型K（及其序列化器），还有上文所述Triggerable接口的实现。
InternalTimerService经由InternalTimeServiceManager.getInternalTimerService()方法取得。
例如，上文KeyedProcessOperator初始化的InternalTimerService，名称为"user-timers"，命名空间类型为空（VoidNamespace），Triggerable实现类则是其本身。如果是WindowOperator的话，其InternalTimerService的名称就是"window-timers"，命名空间类型则是Window。

InternalTimerService在代码中仍然是一个接口，其代码如下。方法的签名除了多了命名空间之外（命名空间对用户透明），其他都与TimerService提供的相同。

```java

}public interface InternalTimerService<N> {
    long currentProcessingTime();
    long currentWatermark();
void registerProcessingTimeTimer(N namespace, long time);
void deleteProcessingTimeTimer(N namespace, long time);
 
void registerEventTimeTimer(N namespace, long time);
void deleteEventTimeTimer(N namespace, long time);
 
// ...
}
```

下面更进一步，看看InternalTimeServiceManager是如何实现的。

InternalTimeServiceManager、TimerHeapInternalTimer
顾名思义，InternalTimeServiceManager用于管理各个InternalTimeService。部分代码如下：



```java
public class InternalTimeServiceManager<K> {
    @VisibleForTesting
    static final String TIMER_STATE_PREFIX = "_timer_state";
    @VisibleForTesting
    static final String PROCESSING_TIMER_PREFIX = TIMER_STATE_PREFIX + "/processing_";
    @VisibleForTesting
    static final String EVENT_TIMER_PREFIX = TIMER_STATE_PREFIX + "/event_";
    
private final KeyGroupRange localKeyGroupRange;
private final KeyContext keyContext;
private final PriorityQueueSetFactory priorityQueueSetFactory;
private final ProcessingTimeService processingTimeService;
private final Map<String, InternalTimerServiceImpl<K, ?>> timerServices;
private final boolean useLegacySynchronousSnapshots;
 
@SuppressWarnings("unchecked")
public <N> InternalTimerService<N> getInternalTimerService(
    String name,
    TimerSerializer<K, N> timerSerializer,
    Triggerable<K, N> triggerable) {
    InternalTimerServiceImpl<K, N> timerService = registerOrGetTimerService(name, timerSerializer);
    timerService.startTimerService(
        timerSerializer.getKeySerializer(),
        timerSerializer.getNamespaceSerializer(),
        triggerable);
    return timerService;
}
 
@SuppressWarnings("unchecked")
<N> InternalTimerServiceImpl<K, N> registerOrGetTimerService(String name, TimerSerializer<K, N> timerSerializer) {
    InternalTimerServiceImpl<K, N> timerService = (InternalTimerServiceImpl<K, N>) timerServices.get(name);
    if (timerService == null) {
        timerService = new InternalTimerServiceImpl<>(
            localKeyGroupRange,
            keyContext,
            processingTimeService,
            createTimerPriorityQueue(PROCESSING_TIMER_PREFIX + name, timerSerializer),
            createTimerPriorityQueue(EVENT_TIMER_PREFIX + name, timerSerializer));
        timerServices.put(name, timerService);
    }
    return timerService;
}
 
private <N> KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> createTimerPriorityQueue(
    String name,
    TimerSerializer<K, N> timerSerializer) {
    return priorityQueueSetFactory.create(
        name,
        timerSerializer);
}
 
// 以下略...
```
}
从上面的代码可以得知：

Flink中InternalTimerService的最终实现实际上是InternalTimerServiceImpl类，而InternalTimer的最终实现是TimerHeapInternalTimer类。
InternalTimeServiceManager会用HashMap维护一个特定键类型K下所有InternalTimerService的名称与实例映射。如果名称已经存在，就会直接返回，不会重新创建。
初始化InternalTimerServiceImpl时，会同时创建两个包含TimerHeapInternalTimer的优先队列（该优先队列是Flink自己实现的），分别用于维护事件时间和处理时间的Timer。
说了这么多，最需要注意的是，**Timer是维护在JVM堆内存中的**，如果频繁注册大量Timer，或者同时触发大量Timer，也是一笔不小的开销。

TimerHeapInternalTimer的实现比较简单，主要就是4个字段和1个方法。为了少打点字，把注释也弄过来。

```java
/**
 * The key for which the timer is scoped.
 */
@Nonnull
private final K key;
/**
 * The namespace for which the timer is scoped.
 */
@Nonnull
private final N namespace;
/**
 * The expiration timestamp.
 */
private final long timestamp;
/**
 * This field holds the current physical index of this timer when it is managed by a timer heap so that we can
 * support fast deletes.
 */
private transient int timerHeapIndex;
 
@Override
public int comparePriorityTo(@Nonnull InternalTimer<?, ?> other) {
    return Long.compare(timestamp, other.getTimestamp());
}
}
```

可见，Timer的scope有两个，一是数据的key，二是命名空间。但是用户不会感知到命名空间的存在，所以我们可以简单地认为Timer是以key级别注册的（Timer四大特点之1）。正确估计key的量可以帮助我们控制Timer的量。

timerHeapIndex是这个Timer在优先队列里存储的下标。优先队列通常用二叉堆实现，而二叉堆可以直接用数组存储（科普文见这里），所以让Timer持有其对应的下标可以较快地从队列里删除它。

comparePriorityTo()方法则用于确定Timer的优先级，显然Timer的优先队列是一个按Timer时间戳为关键字排序的最小堆。下面粗略看看该最小堆的实现。

HeapPriorityQueueSet
上面代码中PriorityQueueSetFactory.create()方法创建的优先队列实际上的类型是HeapPriorityQueueSet。它的基本思路与Java自带的PriorityQueue相同，但是在其基础上加入了按key去重的逻辑（Timer四大特点之2）。不妨列出它的部分代码。

```java
private final HashMap<T, T>[] deduplicationMapsByKeyGroup;
private final KeyGroupRange keyGroupRange;
 
@Override
@Nullable
public T poll() {
    final T toRemove = super.poll();
    return toRemove != null ? getDedupMapForElement(toRemove).remove(toRemove) : null;
}
 
@Override
public boolean add(@Nonnull T element) {
    return getDedupMapForElement(element).putIfAbsent(element, element) == null && super.add(element);
}
 
@Override
public boolean remove(@Nonnull T toRemove) {
    T storedElement = getDedupMapForElement(toRemove).remove(toRemove);
    return storedElement != null && super.remove(storedElement);
}
 
private HashMap<T, T> getDedupMapForKeyGroup(
    @Nonnegative int keyGroupId) {
    return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupId)];
}
 
private HashMap<T, T> getDedupMapForElement(T element) {
    int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
        keyExtractor.extractKeyFromElement(element),
        totalNumberOfKeyGroups);
    return getDedupMapForKeyGroup(keyGroup);
}
 
private int globalKeyGroupToLocalIndex(int keyGroup) {
    checkArgument(keyGroupRange.contains(keyGroup), "%s does not contain key group %s", keyGroupRange, keyGroup);
    return keyGroup - keyGroupRange.getStartKeyGroup();
}
```
要搞懂它，必须解释一下KeyGroup和KeyGroupRange。KeyGroup是Flink内部KeyedState的原子单位，亦即一些key的组合。一个Flink App的KeyGroup数量与最大并行度相同，将key分配到KeyGroup的操作则是经典的取hashCode+取模。而KeyGroupRange则是一些连续KeyGroup的范围，每个Flink sub-task都只包含一个KeyGroupRange。也就是说，KeyGroupRange可以看做当前sub-task在本地维护的所有key。

解释完毕。容易得知，上述代码中的那个HashMap<T, T>[]数组就是在KeyGroup级别对key进行去重的容器，数组中每个元素对应一个KeyGroup。以插入一个Timer的流程为例：

从Timer中取出key，计算该key属于哪一个KeyGroup；
计算出该KeyGroup在整个KeyGroupRange中的偏移量，按该偏移量定位到HashMap<T, T>[]数组的下标；
根据putIfAbsent()方法的语义，只有当对应HashMap不存在该Timer的key时，才将Timer插入最小堆中。
接下来回到主流程，InternalTimerServiceImpl。

InternalTimerServiceImpl
在这里，我们终于可以看到注册和移除Timer方法的最底层实现了。注意ProcessingTimeService是Flink内部产生处理时间的时间戳的服务。

```java
private final ProcessingTimeService processingTimeService;
private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;
private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;
private ScheduledFuture<?> nextTimer;
 
@Override
public void registerProcessingTimeTimer(N namespace, long time) {
    InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
    if (processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
        long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
        if (time < nextTriggerTime) {
            if (nextTimer != null) {
                nextTimer.cancel(false);
            }
            nextTimer = processingTimeService.registerTimer(time, this);
        }
    }
}
 
@Override
public void registerEventTimeTimer(N namespace, long time) {
    eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
}
 
@Override
public void deleteProcessingTimeTimer(N namespace, long time) {
    processingTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
}
 
@Override
public void deleteEventTimeTimer(N namespace, long time) {
    eventTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
}
```
由此可见，注册Timer实际上就是为它们赋予对应的时间戳、key和命名空间，并将它们加入对应的优先队列。特别地，当注册基于处理时间的Timer时，会先检查要注册的Timer时间戳与当前在最小堆堆顶的Timer的时间戳的大小关系。如果前者比后者要早，就会用前者替代掉后者，因为处理时间是永远线性增长的。

Timer注册好了之后是如何触发的呢？先来看处理时间的情况。

InternalTimerServiceImpl类继承了ProcessingTimeCallback接口，表示它可以触发处理时间的回调。该接口只要求实现一个方法，如下。

```java
@Override
private Triggerable<K, N> triggerTarget;
 
public void onProcessingTime(long time) throws Exception {
    nextTimer = null;
    InternalTimer<K, N> timer;
 
    while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
        processingTimeTimersQueue.poll();
        keyContext.setCurrentKey(timer.getKey());
        triggerTarget.onProcessingTime(timer);
    }
 
    if (timer != null && nextTimer == null) {
        nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
    }
}
```
可见，当onProcessingTime()方法被触发回调时，就会按顺序从队列中获取到比时间戳time小的所有Timer，并挨个执行Triggerable.onProcessingTime()方法，也就是在上文KeyedProcessOperator的同名方法，用户自定义的onTimer()逻辑也就被执行了。

最后来到ProcessingTimeService的实现类SystemProcessingTimeService，它是用调度线程池实现回调的。相关的代码如下。

```java
private final ScheduledThreadPoolExecutor timerService;
 
@Override
public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
    long delay = Math.max(timestamp - getCurrentProcessingTime(), 0) + 1;
 
    try {
        return timerService.schedule(
            new TriggerTask(status, task, checkpointLock, target, timestamp), delay, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
        final int status = this.status.get();
        if (status == STATUS_QUIESCED) {
            return new NeverCompleteFuture(delay);
        } else if (status == STATUS_SHUTDOWN) {
            throw new IllegalStateException("Timer service is shut down");
        } else {
            throw e;
        }
    }
}
 
// 注意：这个是TriggerTask线程的run()方法
@Override
public void run() {
    synchronized (lock) {
        try {
            if (serviceStatus.get() == STATUS_ALIVE) {
                target.onProcessingTime(timestamp);
            }
        } catch (Throwable t) {
            TimerException asyncException = new TimerException(t);
            exceptionHandler.handleAsyncException("Caught exception while processing timer.", asyncException);
        }
    }
}
```
可见，onProcessingTime()在TriggerTask线程中被回调，而TriggerTask线程按照Timer的时间戳来调度。到这里，处理时间Timer的情况就讲述完毕了。

再来看事件时间的情况。事件时间与内部时间戳无关，而与水印有关。以下是InternalTimerServiceImpl.advanceWatermark()方法的代码。

```java
public void advanceWatermark(long time) throws Exception {
    currentWatermark = time;
    InternalTimer<K, N> timer;
 
    while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
        eventTimeTimersQueue.poll();
        keyContext.setCurrentKey(timer.getKey());
        triggerTarget.onEventTime(timer);
    }
}
```
该逻辑与处理时间相似，只不过从回调onProcessingTime()变成了回调onEventTime()而已。然后追踪它的调用链，回到InternalTimeServiceManager的同名方法。

```java
public void advanceWatermark(Watermark watermark) throws Exception {
    for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
        service.advanceWatermark(watermark.getTimestamp());
    }
}
```
继续向上追溯，到达终点：算子基类AbstractStreamOperator中处理水印的方法processWatermark()。当水印到来时，就会按着上述调用链流转到InternalTimerServiceImpl中，并触发所有早于水印时间戳的Timer了。

```java
public void processWatermark(Watermark mark) throws Exception {
    if (timeServiceManager != null) {
        timeServiceManager.advanceWatermark(mark);
    }
    output.emitWatermark(mark);
```

