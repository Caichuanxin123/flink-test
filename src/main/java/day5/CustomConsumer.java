package day5;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CustomConsumer {

    public static void main(String[] args) {

        /*Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    System.out.println("22222");
                }

            }
        });
        t1.start();

        for (int i = 0; i < 1000; i++) {
            System.out.println("1111");
        }*/


        /*for (int i = 0; i < 2; i++) {
            Thread t1 = new Thread(new Runnable() {   //创建线程对象
                @Override
                public void run() {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getId() );
                }
            });
            t1.start();   //启动线程, 启动线程之后会执行run方法中的代码
        }*/

        /*for (int i = 0; i < 2; i++) {
            Thread t1 = new Thread(new Runnable() {   //创建线程对象
                @Override
                public void run() {
                    Map<String,Object> map = new HashMap<>();
                    map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop10:9092");
                    map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                    map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
                    map.put(ConsumerConfig.GROUP_ID_CONFIG,"g1");
                    //2. 创建Consumer
                    KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer(map);
                    //订阅 topic-user的数据
                    kafkaConsumer.subscribe(Arrays.asList("topicb"));

                    while (true){
                        //3. 消费数据
                        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
                        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                            System.out.println(consumerRecord);
                        }
                    }
                }
            });
            t1.start();   //启动线程, 启动线程之后会执行run方法中的代码
        }*/

        //1. 初始化配置信息
        Map<String,Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop10:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"g1");
        //2. 创建Consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer(map);
        //订阅 topic-user的数据
        kafkaConsumer.subscribe(Arrays.asList("topicb"));

        while (true){
            //3. 消费数据
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }

}
