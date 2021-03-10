package com.ywf.dao;

import com.ywf.constants.Constants;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 1.发布微博
 * 2.删除微博
 * 3.关注用户
 * 4.取关用户
 * 5.获取用户微博详情
 * 6.获取用户的初始化界面
 */

public class HBaseDao {

    //1.发布微博
    public static void publicWeiBo(String uid, String content) throws IOException {

        //1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作微博内容表
        //1.获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //2.获取当前时间戳
        long ts = System.currentTimeMillis();

        //3.获取RowKey
        String rowKey = uid+"_"+ts;

        //4.创建Put对象
        Put contPut = new Put(Bytes.toBytes(rowKey));

        //5.给put对象赋值
        contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes
            ("content"), Bytes.toBytes(content));

        //6.执行插入数据操作
        contTable.put(contPut);

      //第二部分：操作微博收件箱表
        //1.获取微博内容表对象
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //2.获取当前发布微博人的fans列簇数据
        Get get = new Get(Bytes.toBytes(uid));
        Result result = relaTable.get(get);

        //3.创建一个集合，用于存放微博内容表的put对象




    }
}
