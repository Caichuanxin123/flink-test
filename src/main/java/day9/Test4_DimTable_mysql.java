package day9;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class Test4_DimTable_mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);

        //zhangsan,1001      (zhangsan,1001,北京)
        ds1.map(new RichMapFunction<String, Tuple3<String,String,String>>() {

            Connection conn;
            PreparedStatement ps;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = JdbcUtils.getconnection();
                ps = conn.prepareStatement("select * from t_city where id = ?");
            }

            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] arr = value.split(",");
                ps.setInt(1,Integer.parseInt(arr[1]));
                ResultSet resultSet = ps.executeQuery();
                String cityName = "未知";
                if(resultSet.next()){
                    cityName = resultSet.getString("city_name");
                }
                return Tuple3.of(arr[0],arr[1],cityName);
            }
        }).print();

        env.execute();
    }

}
