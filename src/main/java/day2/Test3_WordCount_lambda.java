package day2;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Test3_WordCount_lambda {
    public static void main(String[] args)throws Exception {
        //lambda
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds1 = env.fromElements("hello,hello,world", "hello,hello,world");
        ds1.flatMap( (String s1, Collector<String> collector) -> {
            String[] arr = s1.split(",");
            for (String s : arr) {
                collector.collect(s);
            }
        }).returns(Types.STRING)
          .map((s)-> Tuple2.of(s,1)).returns(Types.TUPLE(Types.STRING,Types.INT))
          .keyBy( t -> t.f0)
          .sum("f1")
          .print();

        env.execute("");
    }
}
