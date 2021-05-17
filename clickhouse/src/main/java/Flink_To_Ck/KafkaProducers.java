package Flink_To_Ck;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


public class KafkaProducers {
    public static void main(String[] args) throws Exception{
        SendtoKafka("clickhousetest");
    }
    public static void SendtoKafka(String topic) throws Exception{
        Ck ck = new Ck();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "dxbigdata103:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducers = new KafkaProducer(properties);

        String[] member_id = {"1","2","3","4","5","6","7","8","9"};
        String[] goods = {"Milk","Bread","Rice","Nodles","Cookies","Fish","Meat","Fruit","Drink","Books","Clothes","Toys"};


        while(true) {
            String ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            ck.setUserid(member_id[(int)(Math.random()*9)]);
            ck.setItems(goods[(int)(Math.random()*12)]);
            ck.setCreate_date(ts);
            System.out.println(JSON.toJSONString(ck));
            ProducerRecord<Ck, String> record = new ProducerRecord<>(topic, JSON.toJSONString(ck));
            kafkaProducers.send(record);
            Thread.sleep(2000);
        }
    }
}
