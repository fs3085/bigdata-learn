package Catalog;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//通过jdbc方式连接hive
public class JdbcTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.getConfig().getConfiguration().setString("parallelism.default", "8");

        String sql =
            "create table testOut ( " +
                "name varchar(20) not null, " +
                "age int not null " +
                ") with ( " +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://192.168.1.101:3306/test'," +
                "'table-name' = 'aa'," +
                "'driver' = 'com.mysql.jdbc.Driver'," +
                "'username' = 'root'," +
                "'password' = '000000')";


        tableEnv.executeSql(sql);

        Table table = tableEnv.sqlQuery("select * from testOut");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute();

    }
}
