**ProducerRecord** 是 Kafka 中的一个核心类，它代表了一组 Kafka 需要发送的 `key/value` 键值对，它由记录要发送到的主题名称（Topic Name），可选的分区号（Partition Number）以及可选的键值对构成。



在发送 ProducerRecord 时，我们需要将键值对对象由序列化器转换为字节数组，这样它们才能够在网络上传输。然后消息到达了分区器。

如果发送过程中指定了有效的分区号，那么在发送记录时将使用该分区。如果发送过程中未指定分区，则将使用key 的 hash 函数映射指定一个分区。如果发送的过程中既没有分区号也没有，则将以循环的方式分配一个分区。选好分区后，生产者就知道向哪个主题和分区发送数据了。

ProducerRecord 还有关联的时间戳，如果用户没有提供时间戳，那么生产者将会在记录中使用当前的时间作为时间戳。Kafka 最终使用的时间戳取决于 topic 主题配置的时间戳类型。

- 如果将主题配置为使用 `CreateTime`，则生产者记录中的时间戳将由 broker 使用。
- 如果将主题配置为使用`LogAppendTime`，则生产者记录中的时间戳在将消息添加到其日志中时，将由 broker 重写。

然后，这条消息被存放在一个记录批次里，这个批次里的所有消息会被发送到相同的主题和分区上。由一个独立的线程负责把它们发到 Kafka Broker 上。

Kafka Broker 在收到消息时会返回一个响应，如果写入成功，会返回一个 RecordMetaData 对象，**它包含了主题和分区信息，以及记录在分区里的偏移量，上面两种的时间戳类型也会返回给用户**。如果写入失败，会返回一个错误。生产者在收到错误之后会尝试重新发送消息，几次之后如果还是失败的话，就返回错误消息。



首先，创建ProducerRecord必须包含Topic和Value，key和partition可选。然后，序列化key和value对象为ByteArray，并发送到网络。

接下来，消息发送到partitioner。如果创建ProducerRecord时指定了partition，此时partitioner啥也不用做，简单的返回指定的partition即可。如果未指定partition，partitioner会基于ProducerRecord的key生成partition。producer选择好partition后，增加record到对应topic和partition的batch record。最后，专有线程负责发送batch record到合适的Kafka broker。

当broker收到消息时，它会返回一个应答（response）。如果消息成功写入Kafka，broker将返回RecordMetadata对象（包含topic，partition和offset）；相反，broker将返回error。这时producer收到error会尝试重试发送消息几次，直到producer返回error。