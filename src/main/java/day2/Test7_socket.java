package day2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test7_socket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //.socketTextStream("hadoop10",8888);   nc hadoop10 8888
        DataStreamSource<String> ds1 = env.socketTextStream("134.175.63.88", 8888);
        /**
         *  hello,hello,world
         */
        ds1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] arr = value.split(",");
                for (String s : arr) {
                    out.collect(s);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value,1);
            }
        }).keyBy(t -> t.f0).sum(1).print();

        env.execute();
    }
}
