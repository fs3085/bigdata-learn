//package FlinkHive;
//
//
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.StatementSet;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//
///**
// * @Auther WeiJiQian
// * @描述  可行
// */
//public class FlinkReadHiveAndWriteMysql {
//
//
//    public static void main(String[] args) throws Exception {
//
//        EnvironmentSettings settings = EnvironmentSettings
//            .newInstance()
//            .useBlinkPlanner()
//            .inBatchMode()
//            .build();
//
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
//        String name = "myhive";      // Catalog名称，定义一个唯一的名称表示
//        String defaultDatabase = "test";  // 默认数据库名称
//        String hiveConfDir = "/data/apache-hive-2.3.6-bin/conf";  // hive-site.xml路径
//        String version = "2.3.6";       // Hive版本号
//
//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
//        StatementSet statementSet = tableEnv.createStatementSet();
//
//        tableEnv.registerCatalog(name, hive);
//        tableEnv.useCatalog(name);
//
//        Table sqlResult = tableEnv.sqlQuery("select name,age from test.stu77");
//
//        String sql =
//            "create table testOut ( " +
//                "name varchar(20) not null, "+
//                "age varchar(20) not null "+
//                ") with ( "+
//                "'connector.type' = 'jdbc',"+
//                "'connector.url' = 'jdbc:mysql://192.168.1.1:3306/jeecg_boot?characterEncoding=UTF-8',"+
//                "'connector.table' = 'test_stu',"+
//                "'connector.driver' = 'com.mysql.jdbc.Driver',"+
//                "'connector.username' = 'root',"+
//                "'connector.password' = '123456')";
//        tableEnv.executeSql(sql);
//        statementSet.addInsert("testOut",sqlResult);
//
//        statementSet.execute();
//
//    }
//}