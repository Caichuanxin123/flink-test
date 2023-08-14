package day10;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Test {

    public static void main(String[] args) {
        int cnt = 0;

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop10:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        while (cnt < 200){
            User user = new User();
            user.setId(cnt);
            user.setUsername("username" + new Random().nextInt((cnt % 5) + 2));
            user.setTimestamp(System.currentTimeMillis());
            kafkaProducer.send(new ProducerRecord("topic1", JSON.toJSONString(user)));
            System.out.println("发送消息：" + cnt + "******" + user.toString());
            cnt = cnt + 1;
        }
    }

}
