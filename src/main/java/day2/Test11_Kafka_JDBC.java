package day2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * 准备工作：
 * create table t_student(
 *     id int primary key auto_increment,
 * 	   name varchar(50),
 * 	  age  int
 * )
 *
 */
public class Test11_Kafka_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop10:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> ds1 = env.addSource(new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), properties));
        //zs,20  --> Student对象
        SingleOutputStreamOperator<Student> ds2 = ds1.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Student(arr[0], Integer.parseInt(arr[1]));
            }
        });

        ds2.addSink(JdbcSink.sink(
                "insert into t_student (id, name, age) values (null,?,?)",
                (PreparedStatement ps, Student stu) -> {
                    ps.setString(1, stu.getName());
                    ps.setInt(2, stu.getAge());
                },
                JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(5000).build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop10:3306/test_db?useSSL=false&useUnicode=true&characterEncoding=utf8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Student{
        private Integer id;
        private String name;
        private Integer age;

        public Student(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
    }

}
