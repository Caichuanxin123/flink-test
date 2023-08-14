package day8;

import bean.MonitorInfo;
import com.alibaba.fastjson.JSON;
import deserialization.JSONDeserializationSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


public class Test3_KafkaConsumer {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //在实际工作中，可能会有很多个topic
        //topica {"username":"zs","age":20}
        //topicb {"empid":10,"empname":"李四"}

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop10:9092");
        properties.setProperty("group.id", "car-group1");

        FlinkKafkaConsumer<A> consumer =
                new FlinkKafkaConsumer<>("topica", new JSONDeserializationSchema<>(A.class), properties);
        consumer.setStartFromLatest();
        DataStream<A> ds1 = env.addSource(consumer);


        FlinkKafkaConsumer<B> consumer2 =
                new FlinkKafkaConsumer<>("topicb", new JSONDeserializationSchema(B.class), properties);
        consumer.setStartFromLatest();
        DataStream<B> ds3 = env.addSource(consumer2);

        env.execute();
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class A{
        private String username;
        private int age;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class B{
        private int empid;
        private String empname;
    }
}
