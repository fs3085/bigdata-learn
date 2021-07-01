package Demo.Flink_Async.Mysql;

import Demo.Flink_Async.Redis.AsyncIOSideTableJoinRedis;
import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AsyncIOSideTableJoinMysql {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9093");
        properties.setProperty("group.id", "AsyncIOSideTableJoinMysql");

        FlinkKafkaConsumer<JSONObject> kafkaConsumer = new FlinkKafkaConsumer<>("jsontest",
                new KafkaEventSchema(),
                properties);

        SingleOutputStreamOperator<Click> clicks = env
                .addSource(kafkaConsumer)
                .process(new KafkaProcessFunction());

        // add async operator to streaming job
        AsyncDataStream
                .unorderedWait(clicks, new JDBCAsyncFunction(), 100, TimeUnit.SECONDS, 10)
                .print();

        env.execute(AsyncIOSideTableJoinMysql.class.getCanonicalName());
    }

    public static class JDBCAsyncFunction extends RichAsyncFunction<Click, Store> {

        private SQLClient client;

        @Override
        public void open(Configuration parameters) throws Exception {
            Vertx vertx = Vertx.vertx(new VertxOptions()
                    .setWorkerPoolSize(10)
                    .setEventLoopPoolSize(10));

            JsonObject config = new JsonObject()
                    .put("url", "jdbc:mysql://172.16.0.205:3306/flinkcdc")
                    .put("driver_class", "com.mysql.cj.jdbc.Driver")
                    .put("max_pool_size", 10)
                    .put("user", "root")
                    .put("password", "xysh1234");

            client = JDBCClient.createShared(vertx, config);
        }

        @Override
        public void close() throws Exception {
            client.close();
        }

        @Override
        public void asyncInvoke(Click input, ResultFuture<Store> resultFuture) throws Exception {
            client.getConnection(conn -> {
                if (conn.failed()) {
                    return;
                }

                final SQLConnection connection = conn.result();
                connection.query("select id, name from t where id = " + input.getId(), res2 -> {
                    ResultSet rs = new ResultSet();
                    if (res2.succeeded()) {
                        rs = res2.result();
                    }

                    List<Store> stores = new ArrayList<>();
                    for (JsonObject json : rs.getRows()) {
                        Store s = new Store();
                        s.setId(json.getInteger("id"));
                        s.setName(json.getString("name"));
                        stores.add(s);
                    }
                    connection.close();
                    resultFuture.complete(stores);
                });
            });
        }
    }

    /**
     * 解析Kafka数据
     */
    private static class KafkaProcessFunction extends ProcessFunction<JSONObject, Click> {
        @Override
        public void processElement(JSONObject value, Context ctx, Collector<Click> out) throws Exception {
            Click click = value.toJavaObject(Click.class);
            out.collect(click);
        }
    }
}

//Tuple2<String, String>输出数据类型,反序列化，kafka中存的是byte字节数组
class KafkaEventSchema implements KafkaDeserializationSchema<JSONObject> {
    @Override
    //nextElement 是否表示流的最后一条元素，我们要设置为 false ,因为我们需要 msg 源源不断的被消费
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    // 反序列化 kafka 的 record，我们直接返回一个 tuple2<kafkaTopicName,kafkaMsgValue>
    public JSONObject deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return JSONObject.parseObject(new String(record.value(), "UTF-8"));
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return null;
    }
}
