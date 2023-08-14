package day4;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * kafka-console-producer --broker-list hadoop10:9092 --topic topic2
 */
public class Test2_KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("hadoop10", 9999);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop10:9092");
        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer("topic2",new SimpleStringSchema(),properties);
        ds.addSink(flinkKafkaProducer);


        env.execute();
    }
}
