package day5;

import bean.MonitorInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Flink_Kafka_Consumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop10:9092");
        properties.setProperty("group.id", "car-group1");
        properties.setProperty("auto.offset.reset","latest");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "2000");
        properties.setProperty("flink.partition-discovery.interval-millis","5000");

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("topicb", new SimpleStringSchema(), properties);
        DataStream<String> ds1 = env.addSource(consumer).setParallelism(3);

        ds1.print();

        env.execute();
    }
}
