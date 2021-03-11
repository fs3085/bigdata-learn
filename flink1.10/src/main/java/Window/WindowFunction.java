package Window;
/**
window function 定义了要对窗口中收集的数据做的计算操作，主要可以分为两类：
增量聚合函数（incremental aggregation functions）
每条数据到来就进行计算，保持一个简单的状态。典型的增量聚合函数有ReduceFunction, AggregateFunction。
全窗口函数（full window functions）
先把窗口所有数据收集起来，等到计算的时候会遍历所有数据。ProcessWindowFunction，WindowFunction就是一个全窗口函数。
* */
public class WindowFunction {
}
