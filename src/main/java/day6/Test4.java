package day6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * 单词,数字
         */
        DataStreamSource<String> ds = env.socketTextStream("hadoop10", 9999);
        KeyedStream<Tuple3<String, Integer, String>, String> ds2 = ds.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple3.of(arr[0], Integer.parseInt(arr[1]), arr[2]);
            }
        }).keyBy(v -> v.f0);

        /*SingleOutputStreamOperator<Tuple2<String, Integer>> ds3 = ds2.sum("f1");
        ds3.print();*/
        /**
         * maxBy
         * 2> (hello,1,x1)
         * 2> (hello,2,x2)
         * 2> (hello,5,x3)
         * 2> (hello,5,x3)
         */
        ds2.maxBy("f1").print();

        env.execute();
    }
}
