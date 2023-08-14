package day2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
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
public class Test10_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> ds1 =
                env.fromElements(new Student(null, "张三", 20), new Student(null, "李四", 21));

        ds1.addSink(JdbcSink.sink(
                "insert into t_student (id, name, age) values (null,?,?)",
                (PreparedStatement ps, Student stu) -> {
                    ps.setString(1, stu.getName());
                    ps.setInt(2, stu.getAge());
                },
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
    }

}
