package spark_offset

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkOffset {
    def main(args: Array[String]): Unit = {
        //1.创建StreamingContext
        val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
        val ssc = new StreamingContext(sc, Seconds(5)) //5表示每5秒对数据进行一次切分形成一个RDD
        //连接Kafka的参数
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "Hadoop001:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "SparkKafkaDemo",
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        //需要进行消费的topic，要和生产者的topic对应
        val topics = Array("hotitem")
        //2.使用OffsetUtil连接Kafak获取数据
        //注意:
        //因为kafka消费有两种方式earliest和latest,两者的区别是，在有已经提交的offset时，两者没有任何区别，都是从提交的offset处开始消费，如果没有提交的offset时，earliest会从头开始消费，也就是会读取旧数据，而latest则会从新产生的数据开始消费
        //所以消费方式的选择要根据数据源和业务需求来决定，我的生产者是读取的文件，当没有offset存储时，也需要消费所有数据，所以没有offset时选取earliest方式
        val offsetMap: mutable.Map[TopicPartition, Long] = OffsetUtil.getOffsetMap("SparkKafkaDemo", "hotitem")
        val recordDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.size > 0) { //有记录offset
            println("MySQL中记录了offset,则从该offset处开始消费")
            KafkaUtils.createDirectStream[String, String](ssc,
                LocationStrategies.PreferConsistent, //位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
                ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetMap)) //消费策略,源码强烈推荐使用该策略
        } else { //没有记录offset
            println("没有记录offset,则直接连接,从earliest开始消费")
            KafkaUtils.createDirectStream[String, String](ssc,
                LocationStrategies.PreferConsistent, //位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
                ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)) //消费策略,源码强烈推荐使用该策略
        }
        //3.操作数据
        //注意:我们的目标是要自己手动维护偏移量,也就意味着,消费了一小批数据就应该提交一次offset
        //而这一小批数据在DStream的表现形式就是RDD,所以我们需要对DStream中的RDD进行操作
        //而对DStream中的RDD进行操作的API有transform(转换)和foreachRDD(动作)两种算子
        recordDStream.foreachRDD(rdd => {
            if (rdd.count() > 0) { //当前这个批次内有数据
                rdd.foreach(record => println("接收到的Kafk发送过来的数据为:" + record))//数据打印
                //接收到的Kafka发送过来的数据为:ConsumerRecord(topic = hotitem, partition = 0, offset = 1458008, CreateTime = 1596678765095, serialized key size = -1, serialized value size = 36, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 654062,2899195,3720767,pv,1511690400)
                //注意:通过打印接收到的消息可以看到,里面有我们需要维护的offset,和要处理的数据
                //接下来可以对数据进行处理....或者使用transform返回和之前一样处理
                //处理数据的代码写完了,就该维护offset了,那么为了方便我们对offset的维护/管理,spark提供了一个类,帮我们封装offset的数据
                val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                for (o <- offsetRanges) {
                    //topic=hotitem,partition=1,fromOffset=0,untilOffset=1458483这里打印的是主题，分区号，开始消费位置，消费完成后的位置，这里面的topic,partition,untilOffset是需要保存到mysql的信息
                    println(s"topic=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},untilOffset=${o.untilOffset}")
                }
                //实际中偏移量可以提交到MySQL/Redis中
                OffsetUtil.saveOffsetRanges("SparkKafkaDemo", offsetRanges)
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
