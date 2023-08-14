package day9;

import avro.shaded.com.google.common.cache.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

public class Test4_DimTable_cache {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);

        //zhangsan,1001      (zhangsan,1001,北京)
        ds1.map(new RichMapFunction<String, Tuple3<String,String,String>>() {

            Connection conn;
            PreparedStatement ps;
            LoadingCache<String, String> cache;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = JdbcUtils.getconnection();
                ps = conn.prepareStatement("select * from t_city where id = ?");

                cache = CacheBuilder.newBuilder()
                        .maximumSize(20)  //缓存中最多可以存储的数据量   LRU算法 最近最少使用 从缓存中淘汰最近最少使用
                        .expireAfterWrite(1, TimeUnit.MINUTES)   //缓存的失效时间
                        .build(new CacheLoader<String, String>() {
                            @Override
                            public String load(String id) throws Exception {
                                System.out.println(" load 方法被执行了 " + id);
                                ps.setInt(1, Integer.parseInt(id));
                                ResultSet resultSet = ps.executeQuery();  //查询数据库
                                String cityName = "未知";
                                if (resultSet.next()) {
                                    cityName = resultSet.getString("city_name");
                                }
                                return cityName;
                            }
                        });
            }

            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] arr = value.split(",");
                String cityName = cache.get(arr[1]);
                return Tuple3.of(arr[0],arr[1],cityName);
            }
        }).print();

        env.execute();
    }

}
