package Catalog;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//连接kafka,TableAPI
public class KafkaTable {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

//        tableEnv.getConfig().getConfiguration().setString("parallelism.default", "8");

        String sql =
            "create table testOut ( " +
                "name STRING, " +
                "age STRING " +
                ") with ( " +
                "'connector' = 'kafka'," +
                "'topic' = 'example'," +
                "'properties.bootstrap.servers' = '192.168.1.101:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'json')";

        tableEnv.executeSql(sql);

        Table table = tableEnv.sqlQuery("select * from testOut");

        //通过Sql往topic写数据
        tableEnv.executeSql("insert into testOut values('aa','01')");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute();
    }
}
