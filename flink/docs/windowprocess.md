1.在窗口函数后面加process函数，会获得一个包含窗口所有元素的可迭代器
ProcessWindowFunction：能够拿到key的信息

ProcessAllWindowFunction：拿不到key的信息

```scala
class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), (String, Long), String, TimeWindow] {

  // 一个窗口结束的时候调用一次（一个分组执行一次），不适合大量数据，全量数据保存在内存中，会造成内存溢出
  override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Long)]): Unit = {
    // 聚合，注意:整个窗口的数据保存到Iterable，里面有很多行数据, Iterable的size就是日志的总行数
    out.collect(key, elements.size)
  }
}
```

     // 一个窗口结束的时候调用一次（一个分组执行一次），不适合大量数据，全量数据保存在内存中，会造成内存溢出
    可以使用布隆过滤器：拿数据存储到redis jedis.setbit(bitmapKey, offset, true),jedis.getbit(bitmapKey, offset, true)
    和trigger()触发器:窗口关闭和计算结果的触发，窗口默认是数据全部到齐之后计算关闭
窗口在处理流数据时，通常会对流进行分区
数据流划分为：
keyed（根据key划分不同数据流区）
non-keyed(指没有按key划分的数据流区，指所有原始数据流)


2.在keyby后面加process函数，是来一条数据就处理一次