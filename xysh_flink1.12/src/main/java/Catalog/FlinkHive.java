package Catalog;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;


public class FlinkHive {
    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME","hive");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                                                          .inStreamingMode()
                                                          .useBlinkPlanner()
                                                          .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

//        EnvironmentSettings settings = EnvironmentSettings
//            .newInstance()
//            .useBlinkPlanner()
//            .inBatchMode()
//            .build();
//
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name = "myhive";      // Catalog名称，定义一个唯一的名称表示
        String defaultDatabase = "default";  // 默认数据库名称
        String hiveConfDir = "D:\\MT-bank\\bigdatalearn\\xysh_flink1.12\\src\\main\\resources";  // hive-site.xml路径
        String version = "1.1.0";       // Hive版本号

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, hiveConfDir,version);
        StatementSet statementSet = tableEnv.createStatementSet();

        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);

        //List Available Catalogs
        tableEnv.listCatalogs();

        //List Available Databases
        tableEnv.listDatabases();

        //List Available Tables
        tableEnv.listTables();


        Table sqlResult = tableEnv.sqlQuery("select * from mt.a");
        tableEnv.executeSql("insert overwrite `table` sang partition(`data_date` = '20200122') select id,name from sang where data_date='20201221'");

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(sqlResult, Row.class);


        rowDataStream.print();

//        env.execute();


//        String sql =
//            "CREATE TABLE MyUserTable (\n" +
//                "name varchar(20) not null, " +
//                "age int not null " +
//                ") WITH (\n" +
//                "   'connector' = 'jdbc',\n" +
//                "   'url' = 'jdbc:JdbcTable://192.168.1.101:3306/test',\n" +
//                "   'table-name' = 'aa'\n" +
//                ")";
//        tableEnv.executeSql(sql);
        //statementSet.addInsert("testOut",sqlResult);

       env.execute();
    }
}

