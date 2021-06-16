package Flink_CDC;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class MysqlCDC {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 使用blink planner，blink planner是流批统一
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        //
        tEnv.executeSql("CREATE TABLE student(\n" +
                "stu_name STRING,\n" +
                "course STRING,\n" +
                "score int,\n" +
                "proc AS PROCTIME()\n"+
//                "WATERMARK FOR update_time AS update_time\n" +
                ") WITH(\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = '172.16.0.23',\n" +
                "'port' = '3306',\n" +
                "'username' = 'root',\n" +
                "'password' = 'xysh1234',\n" +
                "'database-name' = 'davincidb',\n" +
                "'table-name' = 'stu', \n" +
                "'server-id' = '123456' \n" +
                ")");

        tEnv.executeSql("CREATE TABLE student_count(\n" +
                "stu_name STRING,\n" +
                "counts BIGINT,\n" +
                "PRIMARY KEY(stu_name)NOT ENFORCED\n" +
                ") WITH(\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://172.16.0.23:3306/davincidb',\n" +
                "'username' = 'root',\n" +
                "'password' = 'xysh1234',\n" +
                "'table-name' = 'stu_count'\n" +
                ")");

        String sql ="select stu_name,COALESCE(sum(DISTINCT case when score<100 then 1 else 0 end),0) as counts from student group by stu_name";
        //String sql ="insert into student_count select stu_name,count(1) as counts from student group by stu_name";

        tEnv.executeSql(sql).print();

    }
}

