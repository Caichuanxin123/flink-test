package day4;

import day2.Test11_Kafka_JDBC;
import day3.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Test1_Sink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("hadoop10", 9999);

        ds.map(new MapFunction<String, Student>() {

            @Override
            public Student map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Student(null,arr[0],Integer.parseInt(arr[1]));
            }

        }).addSink(new RichSinkFunction<Student>() {
            Connection connection = null;
            PreparedStatement ps = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://hadoop10:3306/test_db?useSSL=false&userUnicode=true&characterEncoding=utf8",
                        "root","123456");
                ps = connection.prepareStatement("insert into t_student values(null,?,?)");
            }

            @Override
            public void invoke(Student value, Context context) throws Exception {
                System.out.println("invoke被执行了 " + value);
                ps.setString(1,value.getName());
                ps.setInt(2,value.getAge());
                ps.executeUpdate();
            }

            @Override
            public void close() throws Exception {
                if(ps != null) ps.close();
                if(connection != null) connection.close();
            }

        });


        env.execute();
    }
}
