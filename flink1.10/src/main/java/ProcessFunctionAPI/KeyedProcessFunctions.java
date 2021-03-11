package ProcessFunctionAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import apps.MySensor;
import apps.SensorReading;

public class KeyedProcessFunctions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> sensorStreamSource = env.addSource(new MySensor());

//        SingleOutputStreamOperator<String> map = sensorStreamSource.map(new MapFunction<SensorReading, String>() {
//            @Override
//            public String map(SensorReading sensorReading) throws Exception {
//                return sensorReading.getSensorId() + "," + sensorReading.getSensorTemp() + "," + sensorReading.getSensorTime();
//            }
//        });

//        map.print();

        DataStream<String> warningStream = sensorStreamSource.keyBy(SensorReading::getSensorId)
                                                     .process( new TempIncreaseWarning(10) );
        warningStream.print();
        env.execute();
    }


    public static class TempIncreaseWarning extends KeyedProcessFunction<String, SensorReading, String> {
        private Integer interval;

        public TempIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        // 声明状态，保存上次的温度值、当前定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

//        processElement(I value, Context ctx, Collector<O> out),
//        流中的每一个元素都会调用这个方法，调用结果将会放在Collector数据类型中输出。
//        Context可以访问元素的时间戳，元素的key，以及TimerService时间服务。
//        Context还可以将结果输出到别的流(side outputs)。

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            // 更新温度状态
            lastTempState.update(value.getSensorTemp());

            if( value.getSensorTemp() > lastTemp && timerTs == null ){
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            else if( value.getSensorTemp() < lastTemp && timerTs != null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
        }

//        onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) 是一个回调函数。
//        当之前注册的定时器触发时调用。
//        参数timestamp为定时器所设定的触发的时间戳。
//        Collector为输出结果的集合。
//        OnTimerContext和processElement的Context参数一样，
//        提供了上下文的一些信息，例如定时器触发的时间信息(事件时间或者处理时间)。

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect( "传感器" + ctx.getCurrentKey() + "的温度连续" + interval + "秒上升" );
            // 清空timer状态
            timerTsState.clear();
        }
    }
}
