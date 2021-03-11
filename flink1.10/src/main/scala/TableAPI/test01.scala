//package TableAPI
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.{EnvironmentSettings, Table}
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//
//object test01 {
//    def main(args: Array[String]): Unit = {
//        val env = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setParallelism(1)
//
//        val inputStream = env.readTextFile("")
//        val dataStream = inputStream
//                .map( data => {
//                    val dataArray = data.split(",")
//                    SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
//                }
//                )
//        // 基于env创建 tableEnv
//        val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
//        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)
//
//        // 从一条流创建一张表
//        val dataTable: Table = tableEnv.fromDataStream(dataStream)
//
//        // 从表里选取特定的数据
//        val selectedTable: Table = dataTable.select("")
//                .filter("id = 'sensor_1'")
//
//        val selectedStream: DataStream[(String, Double)] = selectedTable
//                .toAppendStream[(String, Double)]
//
//        selectedStream.print()
//
//        env.execute("table test")
//    }
//}
