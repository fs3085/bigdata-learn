转换算子是无法访问事件的时间戳信息和水位线信息的。
而这在一些应用场景下，极为重要。例如MapFunction这样的map转换算子就无法访问时间戳或者当前事件的事件时间。
基于此，DataStream API提供了一系列的Low-Level转换算子。可以访问时间戳、watermark以及注册定时事件。
还可以输出特定的一些事件，例如超时事件等。
Process Function用来构建事件驱动的应用以及实现自定义的业务逻辑(使用之前的window函数和转换算子无法实现)。
例如，Flink SQL就是使用Process Function实现的。
Flink提供了8个Process Function：
ProcessFunction
KeyedProcessFunction
CoProcessFunction
ProcessJoinFunction
BroadcastProcessFunction
KeyedBroadcastProcessFunction
ProcessWindowFunction
ProcessAllWindowFunction