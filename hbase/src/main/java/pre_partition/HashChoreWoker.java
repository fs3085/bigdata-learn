package pre_partition;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HashChoreWoker {

    public static Configuration configuration;
    public static Connection connection;
    public static HBaseAdmin admin;

    //打印日志，static final用来修饰成员变量和成员方法，可简单理解为“全局常量”
    //private static final Logger logger = LogManager.getLogger(HashChoreWoker.class);

    public static final Logger logger = LoggerFactory.getLogger(HashChoreWoker.class);

    //建立连接
    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "master:60000");
        configuration.set("hbase.zookeeper.quorum", "master");
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void doPreRegion(Connection connection, HashMap<String, String> tableName, Integer regionNum, Boolean dropExisTable) throws Exception {

        Admin admin = connection.getAdmin();

        //HTableDescriptor类被TableDescriptorBuilder替代，HColumnDescriptor被ColumnFamilyDescriptor替代
        //支持同时创建多个table
        for (Map.Entry<String, String> entry : tableName.entrySet()) {
            //创建表属性对象,表名需要转字节
            TableName name = TableName.valueOf(entry.getKey());
//            HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(name);

            //创建列簇
//            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(entry.getValue());
//            hTableDescriptor.addFamily(hColumnDescriptor);
//            for(String str : fields) {
            ColumnFamilyDescriptor columnDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(entry.getValue())).build();
            tableDescriptor.setColumnFamily(columnDescriptor);
//            }

            //预分区
            byte[][] bytes = new byte[regionNum][];

            //生成每一个region的边界值 0000|  0001| 0002|。。。
            for (int i = 0; i < regionNum; i++) {
                String leftPad = StringUtils.leftPad(i + "", 4, "0");
                bytes[1] = Bytes.toBytes(leftPad + "|");
            }
            //当表名存在时可选是否drop原有的表
            if (admin.tableExists(name)) {
                if (dropExisTable) {
                    logger.info("table {} exist, will drop it.", name);
                    admin.disableTable(name);
                    admin.deleteTable(name);
                } else {
                    logger.warn("table {} exist！", entry.getKey());
                    continue;
                }
            }

            //指定分区创建table
            admin.createTable(tableDescriptor.build(), bytes);
            logger.info("created hbase table {} completed!, with columnFamily {} and {} regions.", name,
                    entry.getValue(), regionNum);
        }
        admin.close();
    }

    //获取rowkey
    public static String getRowKey(String str, Integer numRegion) {
        int result = (str.hashCode() & Integer.MAX_VALUE) % numRegion;
        String prefix = StringUtils.leftPad(result + "", 4, "0");
        String suffix = DigestUtils.md5Hex(str).substring(0, 12);
        return prefix + suffix;
    }


    public static void main(String[] args) throws Exception {
        init();
//        doPreRegion()
    }


}
