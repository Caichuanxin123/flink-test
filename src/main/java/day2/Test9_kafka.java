package day2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 准备工作：
 * 1. 启动kafka
 *    zkServer.sh start
 *    kafka-server-start.sh -daemon /opt/installs/kafka0.11/config/server.properties
 * 2. 创建一个topic叫做topic1
 *    kafka-topics.sh --zookeeper hadoop10:2181 --create --topic topic1 --partitions 1 --replication-factor 1
 * 3. 用命令启动一个生产者(producer)，向topic1中发送数据 2020-10-10,success,xxx
 *                                                  2020-10-10,fail,xxx
 *    kafka-console-producer.sh --broker-list hadoop10:9092 --topic topic1
 * ---------------------------------------------------
 * flink 消费kafka中的数据
 * 4. flink消费kafka中的数据,过滤出状态为success的数据
 * 5. 输出过滤后的数据
 *
 * producer  序列化message  --> kafka  ---> 反序列化message  consumer
 */
public class Test9_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop10:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> ds1 = env.addSource(new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> ds2 = ds1.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                String flag = value.split(",")[1];
                if ("success".equals(flag)) {
                    return true;
                }
                return false;
            }
        });

        ds2.print();

        env.execute();
    }
}
