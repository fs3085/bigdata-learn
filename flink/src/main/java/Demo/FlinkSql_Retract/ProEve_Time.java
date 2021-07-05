package Demo.FlinkSql_Retract;


import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class ProEve_Time {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings envSetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env, envSetting);

        DataStreamSource<WebVisit> webVisitDS = env.addSource(new RichSourceFunction<WebVisit>() {

            private Random r;
            private Boolean isCancel;
            private String[] broswerSeed;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                r = new Random();
                isCancel = false;
                broswerSeed = Stream.of("Chrome", "IE", "FireFox", "Safri")
                        .toArray(String[]::new);
            }

            @Override
            public void run(SourceContext<WebVisit> ctx) throws Exception {
                while (!isCancel) {
                    WebVisit webVisit = new WebVisit();
                    webVisit.setBrowser(broswerSeed[r.nextInt(broswerSeed.length)]);
                    webVisit.setCookieId(UUID.randomUUID().toString());
                    webVisit.setOpenTime(new Date());
                    webVisit.setOpenTimestamp(webVisit.getOpenTime().getTime());
                    webVisit.setPageUrl("/pro/goods/" + UUID.randomUUID().toString() + ".html");
                    webVisit.setIp(IntStream
                            .range(1, 4)
                            .boxed()
                            .map(n -> (r.nextInt(255) + 2) % 255 + "")
                            .collect(Collectors.joining(".")));

                    ctx.collect(webVisit);
                    System.out.println(webVisit.toString());
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                isCancel = true;
            }
        });


        // 设置Watermark（此处分配的是一个单调递增的Watermark)
        SingleOutputStreamOperator<WebVisit> webVisitDSWithWatermark = webVisitDS.assignTimestampsAndWatermarks(new WebVisitWatermark());

        tblEnv.createTemporaryView("test", webVisitDSWithWatermark, $("ip")
                , $("cookieId")
                , $("pageUrl")
                , $("openTime")
                , $("openTimestamp").rowtime()
                , $("browser"));

        //5、运行SQL
        String sql = ""
                + "SELECT "
                + "TUMBLE_START(openTimestamp, INTERVAL '10' SECONDS), COUNT(DISTINCT browser) "
                + "FROM "
                + "test GROUP BY TUMBLE(openTimestamp, INTERVAL '10' SECONDS)";



        Table table = tblEnv.sqlQuery(sql);

        // 注意：此处要和DataStream中的字段匹配
//        Table tblWebVisit = tblEnv.fromDataStream(webVisitDS
//                , $("ip")
//                , $("cookieId")
//                , $("pageUrl")
//                , $("openTime")
//                , $("browser")
//                , $("timeProp").proctime());
//
//        GroupWindowedTable windowTable = tblWebVisit.window(
//                Slide.over(lit(10).second())
//                        .every(lit(1).second())
//                        .on($("timeProp"))
//                        .as("tumbleWindow"));
//
//        // 分组聚合
//        Table browserCntTbl = windowTable
//                // 注意基于window的group by必须要包含window的别名字段
//                .groupBy($("tumbleWindow")
//                        ,$("browser"))
//                .select($("browser")
//                        , $("pageUrl").count().as("cnt"));



        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tblEnv.toRetractStream(table, Row.class);
        tuple2DataStream.print();

        env.execute("Flink SQL Exp");
    }
}

class WebVisitWatermark implements WatermarkStrategy<WebVisit> {
    @Override
    public TimestampAssigner<WebVisit> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return (event, currentTime) -> event.getOpenTimestamp();
    }

    @Override
    public WatermarkGenerator<WebVisit> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<WebVisit>() {
            private long MAX_EVENTTIME = 0L;
            private long ALLOW_DELAY = 2000;

            @Override
            public void onEvent(WebVisit event, long eventTimestamp, WatermarkOutput output) {
                long max = Math.max(MAX_EVENTTIME, event.getOpenTimestamp() - ALLOW_DELAY);
                Watermark watermark = new Watermark(max);
                output.emitWatermark(watermark);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {

            }
        };
    }
}
