package FlinkRedis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

public class FlinkKafkaToRedis {
    public static void main(String[] args) throws Exception{

        //传入配置文件路径即可
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        DataStream<String> lines = FlinkUtil.createKafkaStream(parameters, SimpleStringSchema.class);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String words, Collector<String> collector) throws Exception {
                for (String word : words.split(" ")) {
                    collector.collect(word);
                }
            }
        });
        words.map(new RichFunction());
//        SingleOutputStreamOperator<Tuple2<String, Integer>> word = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String word) throws Exception {
//                return new Tuple2<>(word, 1);
//            }
//        });


        //为了保证程序出现问题可以继续累加
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = word.keyBy(0).sum(1);
//
//
//        sum.map(new MapFunction<Tuple2<String, Integer>, Tuple3<String,String,String>>() {
//            @Override
//            public Tuple3<String, String, String> map(Tuple2<String, Integer> tp) throws Exception {
//
//                return Tuple3.of("word_count",tp.f0,tp.f1.toString());
//            }
//        }).addSink(new MyRedisSink());

        FlinkUtil.getEnv().execute("kafkaSource");

    }
}
