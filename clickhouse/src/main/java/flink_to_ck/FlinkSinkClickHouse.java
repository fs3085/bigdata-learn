package flink_to_ck;

import flink_to_ck.Ck;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;


public class FlinkSinkClickHouse {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:clickhouse://192.168.1.101:8123/default";
        String user = "default";
        String passwd = "";
        String driver = "ru.yandex.clickhouse.ClickHouseDriver";
        // 设置batch size，测试的话可以设置小一点，这样可以立刻看到数据被写入
        int batchsize = 50;

        //创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String kafkaSource11 = "" +
                "CREATE TABLE user_behavior ( " +
                " `userid` STRING, \n" +
                " `items` STRING, \n" +
                " `create_date` STRING \n" +
                ") WITH ( 'connector' = 'kafka',\n" +
                " 'topic' = 'clickhousetest',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'properties.group.id' = 'group1',\n" +
                " 'properties.bootstrap.servers' = 'dxbigdata101:9092,dxbigdata102:9092,dxbigdata103:9092',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'true',\n" +
                " 'json.ignore-parse-errors' = 'false'" +
                ")";

        // Kafka Source
        tEnv.executeSql(kafkaSource11);
        String query = "SELECT userid,items,create_date FROM user_behavior";
        Table table = tEnv.sqlQuery(query);

        //将Table转换为DataStream
        DataStream<Ck> resultDataStream = tEnv.toAppendStream(table, Ck.class);

        String insertIntoCkSql = "INSERT INTO behavior_mergetree(userid,items,create_date)\n" +
                "VALUES(?,?,?)";

        //将数据写入ClickHouse JDBC Sink
        resultDataStream.addSink(
                JdbcSink.sink(
                        insertIntoCkSql,
                        new JdbcStatementBuilder<Ck>() {
                            @Override
                            public void accept(PreparedStatement ps, Ck ck) throws SQLException {
                                ps.setString(1, ck.getUserid());
                                ps.setString(2, ck.getItems());
                                ps.setString(3, ck.getCreate_date());
                            }
                        },
                        new JdbcExecutionOptions.Builder().withBatchSize(batchsize).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                .withUrl(url)
                                .withUsername(user)
                                .withPassword(passwd)
                                .build()
                )
        );

        env.execute("Flink Table API to ClickHouse Example");

    }
}

