package Watermarks;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import apps.SensorReading;
import org.apache.flink.util.OutputTag;

/**
 * 窗口是左闭右开
 * 由于网络、分布式等原因，会导致乱序数据的产生，达不到秒级别
 * watermark是一条特殊的数据记录
 * watermark必须单调递增，以确保任务的事件时间时钟在向前推进，而不是在后退
 * watermark与数据的时间戳相关
 * Event Time - 延迟时间 = watermark watermark等于窗口触发时间
 * watermark的延迟时间默认200ms
 **/


public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> inputStream = env.socketTextStream("192.168.1.101", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                //升序数据设置事件时间和watermark
//            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                @Override
//                public long extractAscendingTimestamp(SensorReading element) {
//                    return element.getSensorTime()*1000L;
//                }
//            })
                //乱序数据设置事时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getSensorTime() * 1000;
                    }
                });

        /**
         * 窗口分配
         * timeWindow -> TumblingEventTimeWindows的assignWindows方法 ，getWindowStartWithOffset方法
         *timestamp - (timestamp - offset + windowSize) % windowSize
         * 199-(199-0+15)%15 = 195   第一个窗口[195,210)
         *
         * offset :
         * Creates a new SlidingEventTimeWindows WindowAssigner that assigns elements to time windows based on the element timestamp and offset.
         * For example, if you want window a stream by hour,but window begins at the 15th minutes of each hour, you can use of(Time.hours(1),Time.minutes(15)),then you will get time windows start at 0:15:00,1:15:00,2:15:00,etc.
         * Rather than that,if you are living in somewhere which is not using UTC±00:00 time, such as China which is using UTC+08:00,and you want a time window with size of one day, and window begins at every 00:00:00 of local time,you may use of(Time.days(1),Time.hours(-8)). The parameter of offset is Time.hours(-8)) since UTC+08:00 is 8 hours earlier than UTC time.
         * Params:
         * size – The size of the generated windows.
         * slide – The slide interval of the generated windows.
         * offset – The offset which window start would be shifted by.
         * Returns:
         * The time policy.
         * **/

        OutputTag<SensorReading> outputTag = new OutputTag<>("late");

        //基于事件时间的开窗聚合,统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1)) //基于watermark延迟一分钟
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
