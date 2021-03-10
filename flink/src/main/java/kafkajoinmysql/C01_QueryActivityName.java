package kafkajoinmysql;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class C01_QueryActivityName {
    public static void main(String[] args) throws Exception {
        // topic:activity10 分区3，副本2
        // # 创建topic
        // kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 3 --topic activity10

        // # 创建生产者
        // kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic activity10

        // 输入参数：activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
//        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema());
//
//        DataStream<ActivityBean> beans = lines.map(new C01_DataToActivityBeanFunction());
//
//        beans.print();
//
//        FlinkUtilsV1.getEnv().execute("C01_QueryActivityName");


        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka配置
        String topic = "activity10";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.1.101:9092");//多个的话可以指定
        prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("auto.offset.reset","latest");
        prop.setProperty("group.id","consumer1");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(topic, new SimpleStringSchema(), prop);
        //获取数据
        DataStream<String> lines = env.addSource(myConsumer);

        //打印
        //lines.print().setParallelism(1);

        DataStream<ActivityBean> beans = lines.map(new C01_DataToActivityBeanFunction());
        beans.print();
        //执行
        //env.execute("StreamingFormCollection");
        env.execute();
    }
}

