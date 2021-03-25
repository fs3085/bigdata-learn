package ProcessFunctionAPI;

import apps.SensorReading;
import groovy.lang.Tuple;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class Test1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.1.101", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //测试KeyedProcessFunction. 先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new MyProcess())
                .print();



//        dataStream.keyBy(new KeySelector<SensorReading, String>() {
//            @Override
//            public String getKey(SensorReading sensorReading) throws Exception {
//                return sensorReading.getSensorId();
//            }
//        })

        env.execute();
    }
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {

        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer",Long.class));
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {

            //定时器对key有效

            //contex
            ctx.timestamp();    //当前数据时间戳
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timerService().currentProcessingTime(); //当前的处理时间
            ctx.timerService().currentWatermark();  //当前的Watermark
//            ctx.timerService().registerProcessingTimeTimer(10000L);   //处理时间定时器 处理时间达到10000毫秒的时候执行定时操作，从19700101开始
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() +1000L);

            tsTimerState.update(ctx.timerService().currentProcessingTime() + 1000L);

//            ctx.timerService().registerEventTimeTimer((value.getSensorTime()+10)*1000);    //  事件事件定时器

//            ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());

            // ctx.timerService().deleteEventTimeTimer();


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");
            ctx.getCurrentKey();
//            ctx.output();
//            ctx.timeDomain()
        }
    }
}
