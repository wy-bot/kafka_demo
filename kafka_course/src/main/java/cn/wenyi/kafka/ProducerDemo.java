package cn.wenyi.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
/**
 * kafka生产者api代码示例
 */
public class ProducerDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        //设置kafka集群地址
        props.put("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092");
        //ack模式，取值有0，1，-1（all），all是最慢但最安全的
        props.put("acks", "all");
        //失败重试次数（有可能会造成数据的乱序）
        props.put("retries", 3);
        //数据发送的批次大小
        props.put("batch.size", 10);
        //一次数据发送请求所能发送的最大数据量
        props.put("max.request.size", 1024);
        //消息再缓冲区保留的时间，超过设置的值就会提交到服务端
        props.put("linger.ms", 10000);
        //整个Producer用到总内存的大小，如果缓冲区满了会提交数据到服务端
        //buffer.memory要大于batch.size，否则会报申请内存不足的错误
        props.put("buffer.memory", 10240);
        //序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("test002", Integer.toString(i), "ff:" + i));
        //Thread.sleep(1000000)
        producer.close();
    }
}
