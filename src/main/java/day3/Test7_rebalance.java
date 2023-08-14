package day3;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Test7_rebalance {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(5);

        DataStreamSource<Long> ds1 = env.fromSequence(1, 1000000);
        SingleOutputStreamOperator<Long> ds2 = ds1.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 199990;
            }
        });

        ds2.shuffle().map(new RichMapFunction<Long, Tuple2<Integer,Integer>>() {

            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(index,1);
            }

        }).keyBy(v -> v.f0).sum(1).print();

        env.execute();
    }
}
