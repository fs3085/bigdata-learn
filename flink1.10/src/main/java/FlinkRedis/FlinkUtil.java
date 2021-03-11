package FlinkRedis;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtil {

    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T>  createKafkaStream(ParameterTool parameters, Class<? extends DeserializationSchema<T>> clazz) throws Exception{
        env.getConfig().setGlobalJobParameters(parameters);
        //本地环境读取hdfs需要设置，集群上不需要
        //System.setProperty("HADOOP_USER_NAME","root");

        //默认情况下，检查点被禁用。要启用检查点
        env.enableCheckpointing(parameters.getLong("checkpoint.interval",5000L),CheckpointingMode.EXACTLY_ONCE);
        //设置重启策略 默认不停重启
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        //设置state存储的后端(建议在flink配置文件里面配置)
        //env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));

        //程序异常退出或人为cancel掉，不删除checkpoint数据(默认是会删除)
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameters.getRequired("bootstrap.servers"));
        properties.setProperty("group.id", parameters.getRequired("group.id"));
        //如果没有记录偏移量  第一次从最开始消费
        properties.setProperty("auto.offset.reset",parameters.get("auto.offset.reset","earliest"));
        //kafka的消费者不自动提交偏移量 而是交给flink通过checkpoint管理
        properties.setProperty("enable.auto.commit",parameters.get("enable.auto.commit","false"));

        String topics = parameters.getRequired("topics");
        List<String> topicList = Arrays.asList(topics.split(","));

        DataStreamSource stringDataStreamSource = env.addSource(new FlinkKafkaConsumer010<>(topicList, new SimpleStringSchema(), properties));

//        //Source : Kafka, 从Kafka中读取数据
//        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(
//            topicList,
//            clazz.newInstance(),
//            properties);
//
//        //flink checkpoint成功后还要向kafka特殊的topic中写入偏移量  默认是true
//        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        return stringDataStreamSource;

    }


    /**
     * 获取执行环境
     * @return
     */
    public static StreamExecutionEnvironment getEnv(){
        return env;
    }
}
