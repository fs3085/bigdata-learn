package Watermarks;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import apps.SensorReading;

public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        env.execute();
    }
}
