### **概述**

HDFS小文件是指文件远远小于HDFS配置的block文件大小的文件。在HDFS上中block的文件目录数、或增删改查操作等都是存储在内存中，以对象的方式存储，每个对象约占150byte。若大量的小文件存储占用一个block，则会占用大量内存。



### **小文件上传时合并上传**

#### **将本地的小文件合并，上传到HDFS**

本地存在多个小文件，需要上传到HDFS,HDFS的appendToFile命令可以实现多个本地文件合并上传HDFS。

```shell
cd /home/hadoop

[hadoop@node01 ~]$ cat world1.txt
aaa
bbb
ccc
ddd
eee
fff
hhh
[hadoop@node01 ~]$ cat world.txt
aaa
bbb
ccc
ddd
eee
fff
hhh

[hadoop@node01 ~]$ hdfs dfs -appendToFile world1.txt world.txt /tmp/world.txt

[hadoop@node01 ~]$ hdfs dfs -cat /tmp/world.txt
aaa
bbb
ccc
ddd
eee
fff
hhh
aaa
bbb
ccc
ddd
eee
fff
hhh
```

#### **下载HDFS的小文件到本地，合并成一个大文件**

HDFS 存在多个小文件，下载合并到本地生成一个大文件。

```shell
[hadoop@node01 ~]$ hdfs dfs -mkdir /baihe
[hadoop@node01 ~]$ hdfs dfs -copyFromLocal world.txt /baihe
[hadoop@node01 ~]$ hdfs dfs -copyFromLocal world1.txt /baihe

[hadoop@node01 ~]$ hdfs dfs -getmerge /baihe/ local_largefile.txt

[hadoop@node01 ~]$ ll
总用量 12
-rw-r--r--. 1 hadoop hadoop 56 5月  30 14:45 local_largefile.txt
-rw-rw-r--. 1 hadoop hadoop 28 5月  27 01:32 world1.txt
-rw-rw-r--. 1 hadoop hadoop 28 5月  27 01:29 world.txt
[hadoop@node01 ~]$ tail -f -n 200 local_largefile.txt
aaa
bbb
ccc
ddd
eee
fff
hhh
aaa
bbb
ccc
ddd
eee
fff
hhh
```

#### **合并HDFS上的小文件**

```shell
[hadoop@node01 ~]$ hdfs dfs -cat /baihe/* | hdfs dfs -appendToFile - /tmp/hdfs_largefile.txt
```

特殊说明：此类型处理方法，数据量非常大的情况下可能不太适合，最好使用MapReduce来合并。

### **Hadoop Archive方式**

Hadoop Archive或者HAR，是一个高效地将小文件放入HDFS块中的文件存档工具，它能够将多个小文件打包成一个HAR文件，这样在减少namenode内存使用的同时，仍然允许对文件进行透明的访问。

```shell
[hadoop@node01 ~]$ hadoop archive
archive -archiveName <NAME>.har -p <parent path> [-r <replication factor>]<src>* <dest>
```

- -archiveName .har 指定归档后的文件名
- -p 被归档文件所在的父目录
- \* 归档的目录结构
- 归档后存在归档文件的目录
- 可以通过参数 `-D har.block.size` 指定HAR的大小
- 创建归档文件，将 hdfs /baihe 目录下的文件归档到 /user/hadoop 目录下的 myhar.har中

```shell
hadoop fs -lsr /test/in
drwxr-xr-x   - root supergroup          0 2015-08-26 02:35 /test/in/har
drwxr-xr-x   - root supergroup          0 2015-08-22 12:02 /test/in/mapjoin
-rw-r--r--   1 root supergroup         39 2015-08-22 12:02 /test/in/mapjoin/address.txt
-rw-r--r--   1 root supergroup        129 2015-08-22 12:02 /test/in/mapjoin/company.txt
drwxr-xr-x   - root supergroup          0 2015-08-25 22:27 /test/in/small
-rw-r--r--   1 root supergroup          1 2015-08-25 22:17 /test/in/small/small.1
-rw-r--r--   1 root supergroup          1 2015-08-25 22:17 /test/in/small/small.2
-rw-r--r--   1 root supergroup          1 2015-08-25 22:17 /test/in/small/small.3
-rw-r--r--   1 root supergroup          3 2015-08-25 22:27 /test/in/small/small_data

hadoop archive -archiveName myhar.har -p /test/in/ small mapjoin /test/in/har
```

- 查看归档文件内容

```shell
hdfs dfs -lsr /test/in/har/myhar.har
hdfs dfs -ls hdfs://node:8020/test/in/har/myhar.har

#在本机，不写协议端口
hdfs dfs -lsr har:///test/in/har/myhar.har
#不在本机
hdfs dfs -ls har://hdfs-node:8020/test/in/har/myhar.har
```

- 解压归档文件

```shell
#按顺序解压存档（串行）
hdfs dfs -cp har:///test/in/har/myhar.har/* [目标路径]
#并行解压文档，大的归档文件可以提供效率
hadoop distcp har:///test/in/har/myhar.har/* [目标路径]
```

- 删除har文件必须使用rmr命令,rm是不行的

```shell
hdfs dfs -rmr /test/in/har/myhar.har
```

特殊说明：

- 存档文件的源文件及目录都不会自动删除，需要手动删除
- 存档过程实际是一个**MapReduce**过程，所以需要hadoop的MapReduce支持，以及启动yarm集群
- 存档文件本身不支持压缩
- 存档文件一旦创建便不可修改，要想从中删除或增加文件，必须重新建立存档文件
- 创建存档文件会创建原始文件的副本，所以至少需要有与存档文件容量相同的磁盘空间
- 使用 HAR 作为MR的输入，MR可以访问其中所有的文件。但是由于InputFormat不会意识到这是个归档文件，也就不会有意识的将多个文件划分到单独的Input-Split中，所以依然是按照多个小文件来进行处理，效率依然不高

### **Sequence file**

sequence file 由一系列的二进制key/value 组成，如果为key 小文件名，value 为文件内容，则可以将大批小文件合并成一个大文件。

这种方法使用小文件名作为 key，并且文件内容作为 value，实践中这种方式非常管用。

和 HAR 不同的是，这种方式还支持压缩。该方案对于小文件的存取都比较自由，不限制用户和文件的多少，但是 SequenceFile 文件不能追加写入，适用于一次性写入大量小文件的操作。

### **CombineFileInputFormat**

CombineFileInputFormat 是一种新的inputformat，用于将多个文件合并成一个单独的split，另外，它会考虑数据的存储位置。



**HBase**

除了上面的方法，其实我们还可以将小文件存储到类似于 HBase 的 KV 数据库里面，也可以将 Key 设置为小文件的文件名，Value 设置为小文件的内容，相比使用 SequenceFile 存储小文件，使用 HBase 的时候我们可以对文件进行修改，甚至能拿到所有的历史修改版本。



**HDFS为什么将Block块设置为128M**

1、如果低于128M，甚至过小。一方面会造成NameNode内存占用率高的问题，另一方面会造成数据的寻址时间较多。

2、如果于高于128M，甚至更大。会造成无法利用多DataNode的优势，数据只能从从一个DN中读取，无法实现多DN同时读取的速率优势



**小文件产生**

**实时流处理**：比如我们使用 Spark Streaming 从外部数据源接收数据，然后经过 ETL 处理之后存储到 HDFS 中。这种情况下在每个 Job 中会产生大量的小文件。

**MapReduce 产生**：我们使用 Hive 查询一张含有海量数据的表，然后存储在另外一张表中，而这个查询只有简单的过滤条件（比如 select * from iteblog where from = 'hadoop'），这种情况只会启动大量的 Map 来处理，这种情况可能会产生大量的小文件。也可能 Reduce 设置不合理，产生大量的小文件，

**数据本身的特点**：比如我们在 HDFS 上存储大量的图片、短视频、短音频等文件，由于这些文件的特点，而且数量众多，也可能给 HDFS 大量灾难。