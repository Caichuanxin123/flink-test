package day9;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class Test4_DimTable_map {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);

        //zhangsan,1001      (zhangsan,1001,北京)
        ds1.map(new RichMapFunction<String, Tuple3<String,String,String>>() {

            Map<String,String> map;
            @Override
            public void open(Configuration parameters) throws Exception {
                map = new HashMap<>();
                map.put("1001","北京");
                map.put("1002","上海");
            }

            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] arr = value.split(",");
                String cityName = "未知";
                if(map.containsKey(arr[1])){
                    cityName = map.get(arr[1]);
                }
                return Tuple3.of(arr[0],arr[1],cityName);
            }
        }).print();

        env.execute();
    }

}
