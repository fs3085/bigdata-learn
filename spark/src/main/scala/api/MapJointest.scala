package api

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 这种方法是有假定的前提的条件的，比如有两个rdd进行join操作，其中一个rdd的数据量不是很大，比如低于1个G的情况。
 * 具体操作是就是选择两个rdd中那个比较数据量小的，然后我们把它拉到driver端，再然后通过广播变量的方式给他广播出去，这个时候再进行join 的话，因为数据都是在同一Executor中，
 * 所以shuffle 中不会有数据的传输，也就避免了数据倾斜，所以这种方式很好。
 */

object MapJointest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MapJointest")
    val sc = new SparkContext(conf)
    val lista = Array(
      Tuple2("001", "令狐冲"),
      Tuple2("002", "任盈盈")
    )
    //数据量小一点
    val listb = Array(
      Tuple2("001", "一班"),
      Tuple2("002", "二班")
    )

    val listaRDD = sc.parallelize(lista)
    val listbRDD = sc.parallelize(listb)

//    val result: RDD[(String, (String, String))] = listaRDD.join(listbRDD)
//
//    result.foreach( tuple =>{
//      println("班级号:"+tuple._1 + " 姓名："+tuple._2._1 + " 班级名："+tuple._2._2)
//    })

    //设置广播变量
    val listbBoradcast: Broadcast[Array[(String, String)]] = sc.broadcast(listbRDD.collect())
    val value: RDD[(String, (String, Option[String]))] = listaRDD.map(tuple => {
      val key = tuple._1
      val name = tuple._2
      val map = listbBoradcast.value.toMap
      val className = map.get(key)
      (key, (name, className))
    })

    value.foreach( tuple =>{
      println("班级号:"+tuple._1 + " 姓名："+tuple._2._1 + " 班级名："+tuple._2._2.get)
    })

  }

}
