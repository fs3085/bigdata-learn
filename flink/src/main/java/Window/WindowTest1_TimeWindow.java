package Window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Collection;

import apps.SensorReading;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("192.168.1.101", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        // 其他可选API
        //lamdal架构
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };
        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
//                  .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
                                                                        .timeWindow(Time.seconds(15))
                                                                        .allowedLateness(Time.minutes(1))
                                                                        .sideOutputLateData(outputTag)
                                                                        .sum("temperature");

        sumStream.getSideOutput(outputTag).print("late");


        // 开窗测试
        dataStream.keyBy("id")
//                  .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
                  .timeWindow(Time.seconds(15))
                  // 1. 增量聚合函数
//                                              .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
//                                        @Override
//                                        public Integer createAccumulator() {
//                                            return 0;
//                                        }
//
//                                        @Override
//                                        public Integer add(SensorReading sensorReading, Integer integer) {
//                                            return integer+1;
//                                        }
//
//                                        @Override
//                                        public Integer getResult(Integer integer) {
//                                            return integer;
//                                        }
//
//                                        @Override
//                                        public Integer merge(Integer integer, Integer acc1) {
//                                            return null;
//                                        }
//                                    })
//                                        .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//                                        })
                  // 2. 全窗口函数
                                        //Tuple key的信息
                                        //TimeWindow 窗口信息
                                        .apply(new WindowFunction<SensorReading, Integer, Tuple, TimeWindow>() {
                                            @Override
                                            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Integer> out) throws Exception {
                                                Integer count = IteratorUtils.toList(input.iterator()).size();
                                                out.collect(count);
                                            }
                                        });


//                  .window(TumblingEventTimeWindows.of(Time.seconds(15)))

//        new WindowAssigner

                //3. 其它可选API
        /**
         .trigger() —— 触发器
         定义 window 什么时候关闭，触发计算并输出结果
         .evitor() —— 移除器
         定义移除某些数据的逻辑
         .allowedLateness() —— 允许处理迟到的数据
         .sideOutputLateData() —— 将迟到的数据放入侧输出流
         .getSideOutput() —— 获取侧输出流
         **/
        dataStream.keyBy("id")
                  .timeWindow(Time.secondes(15))
                  .trigger()
                  .evictor()
                  .allowedLateness(Time.minutes(1))


        env.execute();
    }
}

//flink窗口的概念：分桶 Window是无限数据流处理的核心，Window将一个无限的stream拆分成有限大小的”buckets”桶，我们可以在这些桶上做计算操作。

// window()方法接受的输入参数是一个WindowAssigner
// WindowAssigner负责将每条输入的数据分发到正确的window中
// Flink提供了通用的WindowAssigner

//滚动窗口 (Tumbling Window)
//滑动窗口 (sliding Window)
//会话窗口 (Session Window)
//全局窗口 (Global Window)


/*
滚动时间窗口 (tumbling time window)
        .timeWindow(Time.seconds(15))
滑动时间窗口 (sliding time window)
        .timeWindow(Time.seconds(15),Time.Seconds(5))
会话窗口 (session window)   没有简单的简写形式
   .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
滚动计数窗口 (tumbling count window)
        .countWindow(5)
滑动计数窗口 (sldinging count window)
        .countWindow(10,2)
*/



