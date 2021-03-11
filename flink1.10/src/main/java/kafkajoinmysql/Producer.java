//package kafkajoinmysql;
//
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class Producer {
//
//    public static void main(String[] args) throws Exception{
//        //获取环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        data.addSink(new FlinkKafkaProducer011<Metrics>(
//            parameterTool.get("kafka.sink.brokers"),
//            parameterTool.get("kafka.sink.topic"),
//            new MetricSchema()
//        )).name("flink-connectors-kafka")
//            .setParallelism(parameterTool.getInt("stream.sink.parallelism"));
//
//        env.execute("flink learning connectors kafka");
//    }
//
//}
