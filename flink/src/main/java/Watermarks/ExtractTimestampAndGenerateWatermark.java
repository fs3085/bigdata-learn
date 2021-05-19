package Watermarks;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

/**
 * Summary:
 *    在Source Function中直接指定Timestamp和生成Watermark
 */
public class ExtractTimestampAndGenerateWatermark {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设定时间特征为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //env.setParallelism(1);

        // 在源端(即SourceFunction)中直接指定Timestamp和生成Watermark
        DataStreamSource<Tuple4<String, Long, String, Integer>> source = env.addSource(new ExampleSourceFunction());

        //source.print();

        DataStream<UserBehavior> dataStream = source
                .map(line -> {
                    return new UserBehavior(line.f0, line.f1, line.f2, line.f3);
                });

        //dataStream.print();

        SingleOutputStreamOperator<String> process = dataStream.keyBy(UserBehavior::getUserID)
                .process(new SumWindowUserId());

        process.print();

        process.getSideOutput(new OutputTag<String>("blacklist"){}).print("blacklist-user");


        env.execute();
    }

    public static class ExampleSourceFunction implements SourceFunction<Tuple4<String,Long,String,Integer>>{

        private volatile boolean isRunning = true;
        private static int maxOutOfOrderness = 10 * 1000;

        private static final String[] userIDSample={"user_1","user_2","user_3"};
        private static final String[] eventTypeSample={"click","browse"};
        private static final int[] productIDSample={1,2,3,4,5};

        @Override
        public void run(SourceContext<Tuple4<String,Long,String,Integer>> ctx) throws Exception {
            while (isRunning){

                // 构造测试数据
                String userID=userIDSample[(new Random()).nextInt(userIDSample.length)];
                long eventTime = System.currentTimeMillis();
                String eventType=eventTypeSample[(new Random()).nextInt(eventTypeSample.length)];
                int productID=productIDSample[(new Random()).nextInt(productIDSample.length)];

                Tuple4<String, Long, String, Integer> record = Tuple4.of(userID, eventTime, eventType, productID);

                // 发出一条数据以及数据对应的Timestamp
                ctx.collectWithTimestamp(record,eventTime);

                Watermark watermarks = new Watermark(eventTime - maxOutOfOrderness);
                //System.out.println(eventTime+"对应的"+watermarks);

                // 发出一条Watermark
                ctx.emitWatermark(watermarks);


                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class SumWindowUserId extends KeyedProcessFunction<String,UserBehavior,String>{

        ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class,0L));
        }


        @Override
        public void processElement(UserBehavior value, Context ctx, Collector<String> out) throws Exception {

            Long curCount = countState.value();

            long watermark = ctx.timerService().currentWatermark();

            //System.out.println(value.getEventTime()+"对应的"+watermark);


            if( curCount == 0 ){
                Long ts = (ctx.timerService().currentProcessingTime() / (24*60*60*1000) + 1) * (24*60*60*1000) - 8*60*60*1000;
                ctx.timerService().registerProcessingTimeTimer(ts);
            }

            if(curCount >= 10) {
                ctx.output(new OutputTag<String>("blacklist"){},value.getUserID()+"-->"+watermark+"-->"+curCount);
            }

           countState.update(curCount+1);
            out.collect(value.getUserID()+"-->"+watermark+"-->"+curCount+"-->"+value.getEventTime()+"-->"+(value.getEventTime()-watermark));

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
          countState.clear();
        }

    }
}
