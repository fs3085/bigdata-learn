package spark_offset

import java.sql.DriverManager

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

object OffsetUtil {
    def getOffsetMap(groupId:String,topic:String) = {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/selftest","root","123456")
        val sql = "select * from t_offset where groupid = ? and topic = ?"
        val psmt = conn.prepareStatement(sql)
        psmt.setString(1,groupId)
        psmt.setString(2,topic)
        val rs = psmt.executeQuery()
        val offsetMap = mutable.Map[TopicPartition,Long]()
        while(rs.next()){
            offsetMap += new TopicPartition(rs.getString("topic"),rs.getInt("partition")) -> rs.getLong("offset")
        }
        rs.close()
        psmt.close()
        conn.close()
        offsetMap
    }
    def saveOffsetRanges(groupId:String,offsetRange:Array[OffsetRange]) = {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/selftest","root","123456")
        val sql = "replace into t_offset (`topic`,`partition`,`groupid`,`offset`) values(?,?,?,?)"
        val psmt = conn.prepareStatement(sql)

        for(e <- offsetRange){
            psmt.setString(1,e.topic)
            psmt.setInt(2,e.partition)
            psmt.setString(3,groupId)
            psmt.setLong(4,e.untilOffset)
            psmt.executeUpdate()
        }
        psmt.close()
        conn.close()
    }
}
