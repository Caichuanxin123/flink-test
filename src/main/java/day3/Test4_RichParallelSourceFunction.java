package day3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

class MySQLSourceFunction extends RichParallelSourceFunction<Student> {

    boolean flag = true;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://hadoop10:3306/test_db?useSSL=false&userUnicode=true&characterEncoding=utf8",
                "root","123456");
        ps = connection.prepareStatement("select * from t_student where id > ? and id <= ?");
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        while (flag){
            //System.out.println( " " + getRuntimeContext().getIndexOfThisSubtask() );
            //System.out.println( "   getNumberOfParallelSubtasks " +getRuntimeContext().getNumberOfParallelSubtasks());
            int total = 8; //总条数
            int totalParallel = getRuntimeContext().getNumberOfParallelSubtasks();  //3
            int j = getRuntimeContext().getIndexOfThisSubtask(); //当前所属并行度(分区) 0  1  2
            /**
             *
             * start= 0   end = 3
             * start= 3   end = 6
             */
            int start = total/totalParallel*j;      // 0   2    4
            int end = start + total/totalParallel;  // 2   4    8

            ps.setInt(1,start);
            if(j == totalParallel - 1){
                ps.setInt(2,total);
            }else{
                ps.setInt(2,end);
            }

            rs = ps.executeQuery();
            while (rs.next()){
                int id = rs.getInt("id");
                String name = rs.getString("name");
                int age = rs.getInt("age");
                //外部类.内部类
                Student s1 = new Student(id,name,age);
                ctx.collect(s1);
            }

            Thread.sleep(5000);
        }
    }

    @Override
    public void close() throws Exception {
        if(rs != null) rs.close();
        if(ps != null) ps.close();
        if(connection != null) connection.close();
    }

    @Override
    public void cancel() {
        flag = false;
    }

}

public class Test4_RichParallelSourceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<Student> ds1 = env.addSource(new MySQLSourceFunction());

        ds1.print();

        env.execute();
    }



}
