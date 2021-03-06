package apps;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.HashMap;

public class SourceTest_MySensor {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         DataStreamSource<SensorReading> sensorStreamSource = env.addSource(new MySensor());

         // ??????
        KeyedStream<SensorReading, Tuple> keyedStream = sensorStreamSource.keyBy("sensorId");

        //  reduce????????????????????????????????????????????????????????????
        SingleOutputStreamOperator<SensorReading> reduceStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
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

        // es???httpHosts??????
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

        OutputTag<SensorReading> high = new OutputTag<>("high");
        OutputTag<Object> low = new OutputTag<>("low");

        SingleOutputStreamOperator<SensorReading> splitStream = reduceStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context context, Collector<SensorReading> collector) throws Exception {
                if (value.getSensorTemp() > 30) {
                    context.output(high, value);
                } else {
                    context.output(low,value);
                }
            }
        });

        DataStream<SensorReading> highstream = splitStream.getSideOutput(high);

//        SplitStream<SensorReading> splitStream = reduceStream.split(new OutputSelector<SensorReading>() {
//            public Iterable<String> select(SensorReading value) {
//                return (value.getSensorTemp() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
//            }
//        });
//
//        DataStream<SensorReading> highTempStream = splitStream.select("high");
        //??????
        filter.print();

        //??????
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
            // ????????????????????????????????????????????????????????????HDFS?????????
        }

        @Override
        public void close() throws Exception {
            System.out.println("my map close");
            // ?????????????????????????????????????????????HDFS?????????
        }
    }

    //????????????redis???mapper???????????????????????????redis??????????????????
    public static class MyRedisMapper implements RedisMapper<SensorReading>{

        // ?????????redis???????????????????????????
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

    //ElasitcsearchSinkFunction?????????
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

    //??????MyJdbcSink
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        // open ?????????????????????
        @Override
        public void open(Configuration parameters) throws Exception {

            conn = DriverManager.getConnection("jdbc:mysql://hadoop101:3306/test", "root", "000000");

            // ???????????????????????????????????????????????????
            insertStmt = conn.prepareStatement("INSERT INTO sensor_temp (id, temp) VALUES (?, ?)");
            updateStmt = conn.prepareStatement("UPDATE sensor_temp SET temp = ? WHERE id = ?");
        }

        // ?????????????????????sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {

            // ????????????????????????????????????super
            updateStmt.setDouble(1, value.getSensorTemp());
            updateStmt.setString(2, value.getSensorId());
            updateStmt.execute();

            // ????????????update?????????????????????????????????
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
