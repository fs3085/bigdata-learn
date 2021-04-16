# client:

用到输入格式化：计算切片清单

切片信息序列化

split信息
file哪个文件，start偏移量，length切片大小,host块的位置信息赋给切片

一个切片等于一个map

# map task:

###### input

客户端：把配置信息，切片信息,jar包提交到hdfs

用到输入格式化：给输入的对象创建一个记录读取器

map.run() context上下文 -> NewTrackingRecordReader -> LineRecordReader

map跑起来，在输入的初始化的时候
map的输入来自hdfs,所以准备了一个对hdfs这个切片代表这个文件的流，开启一个输入流，然后将偏移量sink到自己切片的位置
从第二个map开始，读取各自的第一行，只读一下，算出字节数，加到自己切片上，偏移量向下移动一行，这是在初始化准备的时候，调整了切片偏移量，向下移动了一行

因为hdfs做块的切割的时候，有可能将数据切割在上下两个块的尾和头，计算层的切片可以任意调整，调整也是按偏移量调整，即便一个块有两个切片，第二个切片可能切开第一个切片的数据，按字节数

###### output
reducetask的别名就是分区

partitions = jobContext.getNumberReduceTasks()

reduce数量等于分区

combiner相当于每个map端的reduce

如果配置combiner:

在map中,combiner有可能执行一次，有可能执行多次

在环形缓冲区会执行，然后会判断溢写磁盘的文件数量和3(默认)比较，在进行combiner

MapOutputBuffer



map输出 -> k,v -> k,v,p -> 对k,v序列化变成字节数组，放入buffer



环形缓冲区的内存空间 100M

# reduce task:

```java
public void reduce(Text key, Iterable<IntWritable> values,
                  Context context) throws IoException, InterruptedException {
    Iterator<IntWritable> it = values.iterator();	//假迭代器
    while(it.hasNext()){	//nextKeyIsSame	>>>	nextKeyValue()
        
        IntWritable val = it.next();	//	nextKeyValue();>>>input(真迭代器)
        
    }
}
```

分组比较器：

if(自定义分组比较器){

分组比较器

} else if(map段自定义排序比较器){

map端排序比较器

} else {

key比较器

}



真迭代器：reduce的数据输入源，是一个reduce在所有map拉完属于自己的数据迭代器



假迭代器：reduce方法里迭代一组数据的迭代器



两条数据的内存空间，和上一条数据比较是否是相同的key，返回ture/false

