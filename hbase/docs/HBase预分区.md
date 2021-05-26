

### 按照文件中设置的规则预分区

我们可以在本地指定一个文件split-file.txt

0001|
0002|
0003|
0004|
0005|
0006|

```sql
hbase(main):008:0> create 'split-table','f1',{SPLITS_FILE => '/home/hadoopadmin/split-file.txt'}
0 row(s) in 5.1390 seconds

=> Hbase::Table - split-table
hbase(main):009:0>
```



### 手动设定预分区

```sql
hbase> create 'staff1','info',SPLITS => ['1000','2000','3000','4000']
```



### 指明分区个数

15个区，分区策略按照16进制字符串分

所以rowKey也要变为16进制字符串才能匹配

分区键什么样对应的rowKey也要保持什么样

```sql
create 'staff2','info',{NUMREGIONS => 15, SPLITALGO => 'HexStringSplit'}
```





### 使用javaApi创建预分区

```java
bigdata-learn\hbase\src\main\java\pre_partition\HashChoreWoker.java
    
        byte[][] splits=new byte[3][];
        splits[0]=Bytes.toBytes("aaa");
        splits[1]=Bytes.toBytes("bbb");
        splits[2]=Bytes.toBytes("ccc");

        Admin admin = connection.getAdmin();
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("bigdata"));
        ColumnFamilyDescriptorBuilder cfBuilder= ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));
        builder.setColumnFamily(cfBuilder.build());
        admin.createTable(builder.build(),splits);
        admin.close();
```





**统计网站的每分钟的访问次数**，怎么设计预分区和rowKey？

user_id timestamp

rowKey唯一性，保证后期查询时的数据 要写在一起

1.满足业务

2.解决热点问题



yyyyMMddHHmmssSSS. 避免这种单调递增的rowkey

当分裂时都往新的region里写，出现热点问题

违反唯一性

yyyyMMddHHmmssSSS_user_id

mmHHddMMyyyy_user_id 这种可行

加有规律的随机数 %5(0,1,2,3,4)分区键设计1,2,3,4

取前缀 求哈希值 模一个数 （分区键=>按取模的数订就可以了）

“yyyyMMddHHmm”.hashCode()%5_yyyyMMddHHmmssSSS_user_id

-,1

1,2

2,3

3,4

4,+

查询：202003211203

scan("202003211203".hashCode()%5_202003211203,    "202003211203".hashCode()%5_202003211203|)



