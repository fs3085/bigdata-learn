package apps;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class SourceTest_MySensor {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         DataStreamSource<SensorReading> sensorStreamSource = env.addSource(new MySensor());

         // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorStreamSource.keyBy("sensorId");

        //  reduce聚合，取最小的温度值，并输出当前的时间戳
        DataStream<SensorReading> reduceStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(
                    value1.getSensorId(),
                    value2.getSensorTime(),
                    Math.min(value1.getSensorTemp(), value2.getSensorTemp()));
            }
        });

        DataStream<String> map = reduceStream.map(new MapFunction<SensorReading, String>() {

            public String map(SensorReading sensorReading) throws Exception {
                return sensorReading.getSensorId()+","+sensorReading.getSensorTime()+","+sensorReading.getSensorTime();
            }
        });

        //map.addSink(new FlinkKafkaProducer011<String>("hadoop101:9092","test",new SimpleStringSchema()));

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop101")
            .setPort(6379)
            .build();
        sensorStreamSource.addSink( new RedisSink<SensorReading>(config, new MyRedisMapper()));

        // es的httpHosts配置
        //ArrayList<HttpHost> httpHosts = new ArrayList<>();
        //httpHosts.add(new HttpHost("hadoop101",9200));
        //sensorStreamSource.addSink( new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());

        sensorStreamSource.addSink(new MyJdbcSink());

        DataStream<String> filter = map.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                //System.out.println(s.split(",")[0]);
                if(s.split(",")[0].equals("sensor_1")){
                return true;
                } else {
                    return false;
                }
            }
        });

        final SplitStream<SensorReading> splitStream = reduceStream.split(new OutputSelector<SensorReading>() {
            public Iterable<String> select(SensorReading value) {
                return (value.getSensorTemp() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        //打印
        filter.print();

        //执行
        env.execute();

    }

    public static class MyMapFunction extends RichMapFunction <SensorReading, Tuple2<Integer, String>> {

        @Override
        public Tuple2<Integer, String> map(SensorReading value) throws Exception {
            return new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), value.getSensorId());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("my map open");
            // 以下可以做一些初始化工作，例如建立一个和HDFS的连接
        }

        @Override
        public void close() throws Exception {
            System.out.println("my map close");
            // 以下做一些清理工作，例如断开和HDFS的连接
        }
    }

    //定义一个redis的mapper类，用于定义保存到redis时调用的命令
    public static class MyRedisMapper implements RedisMapper<SensorReading>{

        // 保存到redis的命令，存成哈希表
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading data) {
            return data.getSensorId();
        }

        @Override
        public String getValueFromData(SensorReading data) {
            return data.getSensorTemp().toString();
        }
    }

    //ElasitcsearchSinkFunction的实现
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{

        @Override
        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id",element.getSensorId());
            dataSource.put("ts",element.getSensorTime().toString());
            dataSource.put("temp",element.getSensorTemp().toString());

            IndexRequest indexRequest = Requests.indexRequest()
                .index("sensor")
                .type("readingData")
                .source(dataSource);

            indexer.add(indexRequest);
        }
    }

    //添加MyJdbcSink
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        // open 主要是创建连接
        @Override
        public void open(Configuration parameters) throws Exception {

            conn = DriverManager.getConnection("jdbc:mysql://hadoop101:3306/test", "root", "000000");

            // 创建预编译器，有占位符，可传入参数
            insertStmt = conn.prepareStatement("INSERT INTO sensor_temp (id, temp) VALUES (?, ?)");
            updateStmt = conn.prepareStatement("UPDATE sensor_temp SET temp = ? WHERE id = ?");
        }

        // 调用连接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {

            // 执行更新语句，注意不要留super
            updateStmt.setDouble(1, value.getSensorTemp());
            updateStmt.setString(2, value.getSensorId());
            updateStmt.execute();

            // 如果刚才update语句没有更新，那么插入
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getSensorId());
                insertStmt.setDouble(2, value.getSensorTemp());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }


}
