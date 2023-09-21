package cn.wenyi.kafka;

import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        //定义kafka服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "bigdata01:9092");
        //指定consumer  group
        props.put("group.id", "g1");
        //是否自动提交offset
        props.put("enable.auto.commit", "true");
        //自动提交offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //key的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //value的反序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //如果没有消费偏移量记录，则自动重设为起始offset：latest,earliest,none
        props.put("auto.offset.reset", "earliest");
        //定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅的topic,可同时订阅多个
        consumer.subscribe(Arrays.asList("test001", "test002"));
        while (true) {
            //读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
