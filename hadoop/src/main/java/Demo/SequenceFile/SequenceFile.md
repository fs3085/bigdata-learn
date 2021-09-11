通过SequenceFile合并小文件
检验结果
说明：Hadoop集群中，元数据是交由NameNode来管理的，每个小文件就是一个split，会有自己相对应的元数据，如果小文件很多，则会对内存以及NameNode很大的压力，所以可以通过合并小文件的方式来进行优化。合并小文件其实可以有两种方式：一种是通过Sequence格式转换文件来合并，另一种是通过CombineFileInputFormat来实现。

此处选择SequeceFile类型是因为此格式为二进制格式，而且是key-value类型，我们在合并小文件的时候，可以利用此特性，将每个小文件的名称做为key，将每个小文件里面的内容做为value。

0x01 通过SequenceFile合并小文件
1. 准备工作
a. 我的HDFS上有四个文件：

```shell
[hadoop-sny@master ~]$ hadoop fs -ls /files/
Found 4 items
-rw-r--r--   1 hadoop-sny supergroup         39 2019-04-18 21:20 /files/put.txt
-rw-r--r--   1 hadoop-sny supergroup         50 2019-12-30 17:12 /files/small1.txt
-rw-r--r--   1 hadoop-sny supergroup         31 2019-12-30 17:10 /files/small2.txt
-rw-r--r--   1 hadoop-sny supergroup         49 2019-12-30 17:11 /files/small3.txt
```


内容对应如下，其实内容可以随意：

```text
shao nai yi
nai nai yi yi
shao nai nai

hello hi hi hadoop
spark kafka shao
nai yi nai yi

hello 1
hi 1
shao 3
nai 1
yi 3

guangdong 300
hebei 200
beijing 198
tianjing 209
```

b. 除了在Linux上创建然后上传外，还可以直接以流的方式输入进去，如small1.txt：

```shell
hadoop fs -put - /files/small1.txt
```

输入完后，按ctrl + D 结束输入。



代码......



0x02 检验结果
1. 启动HDFS和YARN

  ```shell
  start-dfs.sh
  start-yarn.sh
  ```

  

2. 执行作业
a. 打包并上传到master上执行，需要传入两个参数

```shell
yarn jar ~/jar/hadoop-learning-1.0.jar com.xxx.xxx.xxx.SmallFilesToSequenceFileConverter /files /output
```


3. 查看执行结果
  a. 生成了一份文件

  ![在这里插入图片描述](https://img-blog.csdnimg.cn/20191230172658115.png)

  b. 查看到里面的内容如下，但内容很难看

  ![在这里插入图片描述](https://img-blog.csdnimg.cn/20191230172722436.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3NoYW9jazIwMTg=,size_16,color_FFFFFF,t_70)

  c. 用text查看文件内容，可看到key为文件名，value为二进制的里面的内容。

  ![在这里插入图片描述](https://img-blog.csdnimg.cn/20191230172741586.png)



0xFF 总结
Input的路径有4个文件，默认会启动4个mapTask，其实我们可以通过CombineTextInputFormat设置成只启动一个：

```java
    job.setInputFormatClass(CombineTextInputFormat.class);
```


