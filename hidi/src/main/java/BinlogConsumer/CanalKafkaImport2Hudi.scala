package BinlogConsumer

import BinlogConsumer.config.CanalKafkaImport2HudiConfig
import BinlogConsumer.constant.Constants
import BinlogConsumer.util.{CanalDataParser, ConfigParser}
import com.typesafe.scalalogging.Logger
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY, _}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.util.control.Breaks.{break, breakable}


object CanalKafkaImport2Hudi {
  def main(args: Array[String]): Unit = {
    val logger = Logger("com.niceshot.hudi.CanalKafkaImport2Hudi")
    val config = ConfigParser.parseHudiDataSaveConfig(args)
    val appName = "hudi_sync_" + config.getMappingMysqlDbName + "__" + config.getMappingMysqlTableName
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(config.getDurationSeconds))
    val spark = SparkSession.builder().config(conf).getOrCreate();

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config.getKafkaServer,
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> config.getKafkaGroup,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "session.timeout.ms"->"30000",
      "max.poll.interval.ms"->"300000"
    )
    val topics = Array(config.getKafkaTopic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD(recordRDD => {
      val offsetRanges = recordRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      //如果出错，
      //超过3次，记录日志
      //没超过3次，记录日志，继续循环
      //不出错，则直接跳出
      //
      breakable {
        for(a <- 1 to 3 ) {
          try {
            val needOperationData = recordRDD.map(consumerRecord => CanalDataParser.parse(consumerRecord.value()))
              .filter(consumerRecord => consumerRecord != null && consumerRecord.getDatabase == config.getMappingMysqlDbName && consumerRecord.getTable == config.getMappingMysqlTableName)
            if (needOperationData.isEmpty()) {
              logger.info("空结果集，不做操作")
            } else {
              logger.info("结果集不为空，开始操作")
              val upsertDf = needOperationData.filter(record=>record.getOperationType != Constants.HudiOperationType.DELETE)
              val deleteDf = needOperationData.filter(record=>record.getOperationType == Constants.HudiOperationType.DELETE)
              if(!upsertDf.isEmpty()) {
                logger.info("开始更新操作")
                val upsertData = upsertDf.map(hudiData => {
                  CanalDataParser.buildJsonDataString(hudiData.getData, config.getPartitionKey)
                }).flatMap(data => data)
                val df = spark.read.json(upsertData)
                hudiDataUpsertOrDelete(config, df, UPSERT_OPERATION_OPT_VAL)
              }
              //基于自增id的crud，insert和update一定是在delete之前。因为一旦delete后，你再insert, 一定是新的id，不会存在反复insert同一个id的情况
              //所以可以把delete操作，统一放到最后操作
              if(!deleteDf.isEmpty()) {
                logger.info("开始删除操作")
                val deleteData = deleteDf.map(hudiData => {
                  CanalDataParser.buildJsonDataString(hudiData.getData, config.getPartitionKey)
                }).flatMap(data => data)
                val df = spark.read.json(deleteData)
                hudiDataUpsertOrDelete(config, df, DELETE_OPERATION_OPT_VAL)
              }
            }
            break
          } catch {
            case exception: Exception => logger.error(exception.getMessage,exception)
          }
          if(a==3) {
            logger.warn("三次循环处理仍失败，数据丢失")
          }
        }
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  private def hudiDataUpsertOrDelete(config: CanalKafkaImport2HudiConfig, data: DataFrame, optType: String): Unit = {

    data.write.format("hudi").
      option(OPERATION_OPT_KEY, optType).
      option(PRECOMBINE_FIELD_OPT_KEY, config.getPrecombineKey).
      option(RECORDKEY_FIELD_OPT_KEY, config.getPrimaryKey).
      option(PARTITIONPATH_FIELD_OPT_KEY, Constants.HudiTableMeta.PARTITION_KEY).
      option(TABLE_NAME, config.getStoreTableName).
      option(TABLE_TYPE_OPT_KEY,COW_TABLE_TYPE_OPT_VAL).
      option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, true).
      /*
        关闭hive数据结构实时同步，新增数据量大时，每次都去连hive metastore，怕扛不住
        option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY,hudiTableName).
        option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, hudiTableName).
        option(DataSourceWriteOptions.META_SYNC_ENABLED_OPT_KEY, true).
        option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default").
        option(DataSourceWriteOptions.HIVE_USER_OPT_KEY, "hive").
        option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY, "hive").
        option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.16.181:10000").
        option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY,Constants.HudiTableMeta.PARTITION_KEY).
        */
      mode(SaveMode.Append).
      save(config.getRealSavePath)
  }
}
