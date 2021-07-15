package sparksql_exe_tem.framework;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import sparksql_exe_tem.udfs.udaf.PASStatusA;
import sparksql_exe_tem.utils.ConfigurationManager;
import sparksql_exe_tem.utils.FileUtils;

import java.io.IOException;

public class SparkExecuteFramework {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            args = new String[]{"spark/src/main/java/sparksql_exe_tem/Sql-Flies/test.sql", "20210101"};
        }

        boolean local = ConfigurationManager.getBoolean("spark.local");
        String sqls;
        String filepath = args[0];
        String data_date = args[1];

        SparkConf sparkConf = new SparkConf();
        if (local) {
            sparkConf.setMaster("local[*]");
            sqls = FileUtils.readFromLocalFile(filepath);
        } else {
            sqls = FileUtils.readFromHdfsFile(filepath);
        }

        SparkSession spark = new SparkSession.Builder()
                .config(sparkConf)
                // .config("dfs.client.use.datanode.hostname", "true")
                .config("hive.exec.dynamici.partition",true)
                .config("hive.exec.dynamic.partition.mode","nonstrict")
                .config("hive.exec.max.dynamic.partitions",100)
                .enableHiveSupport()
                .getOrCreate();

        //注册udf函数

        //注册udaf函数
        spark.udf().register("PASStatusA24", new PASStatusA());


        sqls = sqls.replace("${data_date}", data_date);
        for (String sql : sqls.split(";")) {
            if (sql.replace(" ", "").length() > 0) {
                System.out.println("【执行sql】" + sql);
                spark.sql(sql).show(120,false);
            }

        }

    }
}
