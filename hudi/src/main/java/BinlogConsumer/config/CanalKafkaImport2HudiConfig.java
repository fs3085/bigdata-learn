package BinlogConsumer.config;

import org.apache.hudi.com.beust.jcommander.Parameter;

public class CanalKafkaImport2HudiConfig extends HudiTableSaveConfig{
    @Parameter(names = {"--kafka-server"},description = "kafka server which stored binlog data from alibaba canal, has no default value . " +
            "eg: 192.168.16.237:9092,192.168.16.236:9092")
    private String kafkaServer;
    @Parameter(names = {"--kafka-topic"},description = "kafka topic which for consume binlog from canal,has no default value.")
    private String kafkaTopic;
    @Parameter(names = {"--kafka-group"},description = "kafka group which for consume binlog from canal,suggest use default value ." +
            "default value construct by dbName ,tableName.eg. dbName:crm ,tableName:order, then topic name is :hudi_crm__order")
    private String kafkaGroup;
    @Parameter(names = {"--duration-seconds"},description = "batch time length for spark streaming,default is '10'")
    private Long durationSeconds = 10L;

    public String getKafkaServer() {
        return kafkaServer;
    }

    public void setKafkaServer(String kafkaServer) {
        this.kafkaServer = kafkaServer;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getKafkaGroup() {
        return kafkaGroup;
    }

    public void setKafkaGroup(String kafkaGroup) {
        this.kafkaGroup = kafkaGroup;
    }

    public Long getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(Long durationSeconds) {
        this.durationSeconds = durationSeconds;
    }
}
