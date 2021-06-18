package apps;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * SimpleStringSchema：返回的结果只有Kafka的value，而没有其它信息
 *TypeInformationKeyValueSerializationSchema：返回的结果只有Kafka的key,value，而没有其它信息
 *需要获得Kafka的topic或者其它信息，就需要通过实现KafkaDeserializationSchema接口来自定义返回数据的结构
 */



public class  flinkkafkaschema{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.0.52:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<Tuple2<String, String>> kafkaConsumer = new FlinkKafkaConsumer<>("test006", new CustomKafkaDeserializationSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        env.addSource(kafkaConsumer).flatMap(new FlatMapFunction<Tuple2<String, String>, Object>() {
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<Object> out) throws Exception {
                System.out.println("topic==== " + value.f0 + "----->" + value.f1);
            }
        });


//        FlinkKafkaProducer<Tuple2<String, Integer>> producer = new FlinkKafkaProducer<Tuple2<String, Integer>>(topic,
//                (KafkaSerializationSchema) new ProducerStringSerializationSchema(topic),
//                properties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
//
//        //创建一个List，里面有两个Tuple2元素
//        List<Tuple2<String, Integer>> list = new ArrayList<>();
//        list.add(new Tuple2("aaa", 1));
//        list.add(new Tuple2("bbb", 1));
//        list.add(new Tuple2("ccc", 1));
//        list.add(new Tuple2("ddd", 1));
//        list.add(new Tuple2("eee", 1));
//        list.add(new Tuple2("fff", 1));
//        list.add(new Tuple2("aaa", 1));
//
//        //统计每个单词的数量
//        env.fromCollection(list)
//                .addSink(producer);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

}


//Tuple2<String, String>输出数据类型,反序列化，kafka中存的是byte字节数组
class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<Tuple2<String, String>> {
    @Override
    //nextElement 是否表示流的最后一条元素，我们要设置为 false ,因为我们需要 msg 源源不断的被消费
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }

    @Override
    // 反序列化 kafka 的 record，我们直接返回一个 tuple2<kafkaTopicName,kafkaMsgValue>
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new Tuple2<>(record.topic(), new String(record.value(), "UTF-8"));
    }

    //用于获取反序列化对象的类型
    @Override
    //告诉 Flink 我输入的数据类型, 方便 Flink 的类型推断
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }
}


//Tuple2<String, String>输入数据类型，序列化成byte数组
class ProducerKafkaserializationSchema implements KafkaSerializationSchema<Tuple2<String, Integer>> {

    private String topic;

    public ProducerKafkaserializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> element, Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, element.f0.getBytes(StandardCharsets.UTF_8), element.f1.toString().getBytes(StandardCharsets.UTF_8));
    }
}