package day2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Test6_Parallelism {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);  //设置并行度，可以想象成车道

        DataStreamSource<Long> ds1 = env.fromSequence(1, 20);
        ds1.map(new MapFunction<Long, Tuple2<Long,Integer>>() {
            @Override
            public  Tuple2<Long,Integer> map(Long value) throws Exception {
                return Tuple2.of(value,1);
            }
        }).writeAsCsv("hdfs://hadoop10:8020/aa2");

        env.execute();
    }

}
