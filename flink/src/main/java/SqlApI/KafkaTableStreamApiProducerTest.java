package SqlApI;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class KafkaTableStreamApiProducerTest {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                                                          .inStreamingMode()
                                                          .useBlinkPlanner()
                                                          .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        Long baseTimestamp = 1600855709000L;
//        DataStream<CustomerStatusChangedEvent> eventDataSet = env.fromElements(
//            new CustomerStatusChangedEvent(1010L, 2, baseTimestamp),
//            new CustomerStatusChangedEvent(1011L, 2, baseTimestamp + 100),
//            new CustomerStatusChangedEvent(1011L, 1, baseTimestamp - 100),
//            new CustomerStatusChangedEvent(1010L, 3, baseTimestamp + 150)
//        );

        String sql =
            "create table testOut ( " +
                "name varchar(20) not null, " +
                "age int not null " +
                ") with ( " +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = 'jdbc:JdbcTable://192.168.1.101:3306/test?characterEncoding=UTF-8'," +
                "'connector.table' = 'aa'," +
                "'connector.driver' = 'com.JdbcTable.jdbc.Driver'," +
                "'connector.username' = 'root'," +
                "'connector.password' = '000000')";
        tableEnv.executeSql(sql).print();

        tableEnv.sqlQuery("SELECT * FROM aa").printSchema();

        // table.toAppendStream
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        //env.execute();


    }
}
