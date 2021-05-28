package SqlApI.Temporal;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;


/**
 * Temporal Table可提供历史某个时间点上的数据。
 * Temporal Table根据时间来跟踪版本。
 * Temporal Table需要提供时间属性和主键。
 * Temporal Table一般和关键词LATERAL TABLE结合使用。
 * Temporal Table在基于ProcessingTime时间属性处理时，每个主键只保存最新版本的数据。
 * Temporal Table在基于EventTime时间属性处理时，每个主键保存从上个Watermark到当前系统时间的所有版本。
 * 侧Append-Only表Join右侧Temporal Table，本质上还是左表驱动Join，即从左表拿到Key，根据Key和时间(可能是历史时间)去右侧Temporal Table表中查询。
 * Temporal Table Join目前只支持Inner Join & Left Join。
 * Temporal Table Join时，右侧Temporal Table表返回最新一个版本的数据。举个栗子，左侧事件时间如是2016-01-01 00:00:01秒，Join时，只会从右侧Temporal Table中选取<=2016-01-01 00:00:01的最新版本的数据
 */


public class TemporalTable {

    public static final Logger log = LoggerFactory.getLogger(TemporalTable.class);

    public static void main(String[] args) throws Exception {

        args = new String[]{"--application", "D:\\github\\bigdata-learn\\flink\\src\\main\\resources\\application.properties"};

        //1、解析命令行参数
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fromArgs.getRequired("application"));

        //browse log
        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");
        String browseTopic = parameterTool.getRequired("browseTopic");
        String browseTopicGroupID = parameterTool.getRequired("browseTopicGroupID");

        //product history info
        String productInfoTopic = parameterTool.getRequired("productHistoryInfoTopic");
        String productInfoGroupID = parameterTool.getRequired("productHistoryInfoGroupID");

        //设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //ExecutionConfig config = env.getConfig();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        streamEnv.setParallelism(1);

        //3、注册Kafka数据源
        //注意: 为了在北京时间和时间戳之间有直观的认识，这里的UserBrowseLog中增加了一个字段eventTimeTimestamp作为eventTime的时间戳
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers", kafkaBootstrapServers);
        browseProperties.put("group.id", browseTopicGroupID);


        DataStream<UserBrowseLog> inputStream = streamEnv
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction());
//                .assignTimestampsAndWatermarks(new BrowseTimestampExtractor(Time.seconds(0)));

        SerializableTimestampAssigner<UserBrowseLog> timestampAssigner = new SerializableTimestampAssigner<UserBrowseLog>() {
            @Override
            public long extractTimestamp(UserBrowseLog userBrowseLog, long l) {
                return userBrowseLog.getEventTimeTimestamp();
            }
        };


        SingleOutputStreamOperator<UserBrowseLog> browseStream = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBrowseLog>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(timestampAssigner));


        //从给定的DataStream创建一个视图。可以在SQL查询中引用注册的视图。Table的字段名称是自动从DataStream的类型派生的
        //tableEnv.createTemporaryView("browse",browseStream,$("myLong"), $("myString"));


        //通过额外扩展一个自定义字段browseRowtime来作为event timestamp，该字段只能放在最后，此时还需要在browseRowtime后面加上.rowtime()后缀
        //如果使用的是process time定义窗口event time，所以消息中的timestamp字段并没有使用
        //使用timestamp 作为出发时间时间戳，此时必须添加.rowtime后缀
        //tableEnv.registerDataStream("browse", browseStream, "userID,eventTime,eventTimeTimestamp,eventType,productID,productPrice,browseRowtime.rowtime");
        tableEnv.createTemporaryView("browse", browseStream, $("userID"), $("eventTime"), $("eventTimeTimestamp"), $("eventType"), $("productID"), $("productPrice"), $("browseRowtime").rowtime());

        //4、注册时态表(Temporal Table)
        //注意: 为了在北京时间和时间戳之间有直观的认识，这里的ProductInfo中增加了一个字段updatedAtTimestamp作为updatedAt的时间戳
        Properties productInfoProperties = new Properties();
        productInfoProperties.put("bootstrap.servers", kafkaBootstrapServers);
        productInfoProperties.put("group.id", productInfoGroupID);
        DataStream<ProductInfo> productInfoStream = streamEnv
                .addSource(new FlinkKafkaConsumer<>(productInfoTopic, new SimpleStringSchema(), productInfoProperties))
                .process(new ProductInfoProcessFunction())
                .assignTimestampsAndWatermarks(new ProductInfoTimestampExtractor(Time.seconds(0)));

        tableEnv.registerDataStream("productInfo", productInfoStream, "productID,productName,productCategory,updatedAt,updatedAtTimestamp,productInfoRowtime.rowtime");
        //设置Temporal Table的时间属性和主键
        TemporalTableFunction productInfo = tableEnv.from("productInfo").createTemporalTableFunction($("productInfoRowtime"), $("productID"));
        //注册TableFunction
        tableEnv.createTemporaryFunction("productInfoFunc", productInfo);
        tableEnv.toAppendStream(tableEnv.scan("productInfo"), Row.class).print();

        //5、运行SQL
        String sql = ""
                + "SELECT "
                + "browse.userID, "
                + "browse.eventTime, "
                + "browse.eventTimeTimestamp, "
                + "browse.eventType, "
                + "browse.productID, "
                + "browse.productPrice, "
                + "productInfo.productID, "
                + "productInfo.productName, "
                + "productInfo.productCategory, "
                + "productInfo.updatedAt, "
                + "productInfo.updatedAtTimestamp "
                + "FROM "
                + " browse, "
                + " LATERAL TABLE (productInfoFunc(browse.browseRowtime)) as productInfo "
                + "WHERE "
                + " browse.productID=productInfo.productID";

        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Row.class).print();

        //6、开始执行
        streamEnv.execute(TemporalTable.class.getSimpleName());


    }

    /**
     * 提取时间戳生成水印
     */
    private static class BrowseTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserBrowseLog> {

        BrowseTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserBrowseLog element) {
            return element.getEventTimeTimestamp();
        }
    }


    /**
     * 解析Kafka数据
     */
    private static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {

                UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getEventTime(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setEventTimeTimestamp(eventTimeTimestamp);

                out.collect(log);
            } catch (Exception ex) {
                log.error("解析Kafka数据异常...", ex);
            }

        }
    }

    /**
     * 解析Kafka数据
     */
    static class ProductInfoProcessFunction extends ProcessFunction<String, ProductInfo> {
        @Override
        public void processElement(String value, Context ctx, Collector<ProductInfo> out) throws Exception {
            try {

                ProductInfo log = JSON.parseObject(value, ProductInfo.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getUpdatedAt(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setUpdatedAtTimestamp(eventTimeTimestamp);

                out.collect(log);
            } catch (Exception ex) {
                log.error("解析Kafka数据异常...", ex);
            }
        }
    }

    /**
     * 提取时间戳生成水印
     */
    static class ProductInfoTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<ProductInfo> {

        ProductInfoTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(ProductInfo element) {
            return element.getUpdatedAtTimestamp();
        }
    }
}