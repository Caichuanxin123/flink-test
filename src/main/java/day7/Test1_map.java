package day7;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;


/**
 * 统计每个城市的最大值
 * 北京,10
 * 北京,6
 * 北京,11
 */
public class Test1_map {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);


        ds1.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple2.of(arr[0],Integer.parseInt(arr[1]));
            }
        }).keyBy(v -> v.f0)
          .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>(){

                    Map<String,Integer> map = new HashMap<>();
                    //{bj=15,zz=20}

                    //bj,10
                    //bj,15
                    //bj,11
                    //zz,20
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        boolean b = map.containsKey(value.f0);
                        if(b){
                            Integer oldValue = map.get(value.f0);
                            if(oldValue < value.f1){
                                map.put(value.f0,value.f1);
                            }
                        }else{
                            map.put(value.f0,value.f1);
                        }
                        return Tuple2.of(value.f0,map.get(value.f0));
                    }

            })
          .print();

        env.execute();
    }

}
