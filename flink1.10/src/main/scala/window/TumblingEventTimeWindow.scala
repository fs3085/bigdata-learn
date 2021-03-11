package window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

object TumblingEventTimeWindow {
    def main(args: Array[String]): Unit = {
        //  环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val dstream: DataStream[String] = env.socketTextStream("192.168.1.101",7777)

        val textWithTsDstream: DataStream[(String, Long, Int)] = dstream.map { text =>
            val arr: Array[String] = text.split(" ")
            (arr(0), arr(1).toLong, 1)
        }
        val textWithEventTimeDstream: DataStream[(String, Long, Int)] = textWithTsDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(2000)) {
            override def extractTimestamp(element: (String, Long, Int)): Long = {
                System.out.print(element._2*1000L)
                return  element._2*1000L
            }
        })

        val textKeyStream: KeyedStream[(String, Long, Int), Tuple] = textWithEventTimeDstream.keyBy(0)
        textKeyStream.print("textkey:")

        val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyStream.window(TumblingEventTimeWindows.of(Time.seconds(3)))

        val groupDstream: DataStream[mutable.HashSet[Long]] = windowStream.fold(new mutable.HashSet[Long]()) { case (set, (key, ts, count)) =>
            set += ts
        }

        groupDstream.print("window::::").setParallelism(1)

        env.execute()
    }

}
