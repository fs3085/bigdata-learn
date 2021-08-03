package SparkStreaming_kafka

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import java.util.Properties

/**
 *
 */
object Kafka2KafkaStreaming {
  private val LOG = LoggerFactory.getLogger("Kafka2KafkaStreaming")

  private val STOP_FLAG = "TEST_STOP_FLAG"

  def initRedisPool() = {
    // Redis configurations
    val maxTotal = 20
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "47.98.119.122"
    val redisPort = 6379
    val redisTimeout = 30000
    InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
  }

  /**
   * 从redis里获取Topic的offset值
   *
   * @param topicName
   * @param partitions
   * @return
   */
  def getLastCommittedOffsets(topicName: String, partitions: Int): Map[TopicPartition, Long] = {
    if (LOG.isInfoEnabled())
      LOG.info("||--Topic:{},getLastCommittedOffsets from Redis--||", topicName)

    //从Redis获取上一次存的Offset
    val jedis = InternalRedisClient.getPool.getResource
    val fromOffsets = collection.mutable.HashMap.empty[TopicPartition, Long]
    for (partition <- 0 to partitions - 1) {
      val topic_partition_key = topicName + "_" + partition
      val lastSavedOffset = jedis.get(topic_partition_key)
      val lastOffset = if (lastSavedOffset == null) 0L else lastSavedOffset.toLong
      fromOffsets += (new TopicPartition(topicName, partition) -> lastOffset)
    }
    jedis.close()

    fromOffsets.toMap
  }

  def main(args: Array[String]): Unit = {
    //初始化Redis Pool
    initRedisPool()

    val conf = new SparkConf()
      .setAppName("ScalaKafkaStream")
      .setMaster("local[3]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(3))

    val bootstrapServers = "hadoop1:9092,hadoop2:9092,hadoop3:9092"
    val groupId = "kafka-test-group"
    val topicName = "Test"
    val maxPoll = 1000

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // 这里指定Topic的Partition的总数
    val fromOffsets = getLastCommittedOffsets(topicName, 3)

    // 初始化KafkaDS
    val kafkaTopicDS =
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))

    // 初始化KafkaSink,并广播
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", bootstrapServers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      if (LOG.isInfoEnabled)
        LOG.info("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }


    kafkaTopicDS.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 如果rdd有数据
      if (!rdd.isEmpty()) {
        // 在每个Partition里执行
        rdd
          .map(_.value())
          .flatMap(_.split(" "))
          .map(x => (x, 1L))
          .reduceByKey(_ + _)
          .foreachPartition(partition => {

            val jedis = InternalRedisClient.getPool.getResource
            val p = jedis.pipelined()
            p.multi() //开启事务

            // 使用广播变量发送到Kafka
            partition.foreach(record => {
              kafkaProducer.value.send("Test_Json", new Gson().toJson(record))
            })

            val offsetRange = offsetRanges(TaskContext.get.partitionId)
            println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
            val topic_partition_key = offsetRange.topic + "_" + offsetRange.partition
            p.set(topic_partition_key, offsetRange.untilOffset + "")

            p.exec() //提交事务
            p.sync //关闭pipeline
            jedis.close()
          })
      }
    })

    ssc.start()

    // 优雅停止
    stopByMarkKey(ssc)

    ssc.awaitTermination()
  }

  /**
   * 优雅停止
   *
   * @param ssc
   */
  def stopByMarkKey(ssc: StreamingContext): Unit = {
    val intervalMills = 10 * 1000 // 每隔10秒扫描一次消息是否存在
    var isStop = false
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExists(STOP_FLAG)) {
        LOG.warn("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }
    }
  }

  /**
   * 判断Key是否存在
   *
   * @param key
   * @return
   */
  def isExists(key: String): Boolean = {
    val jedis = InternalRedisClient.getPool.getResource
    val flag = jedis.exists(key)
    jedis.close()
    flag
  }
}
