package day3;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Test6_SideOutput {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> ds1 = env.fromElements(11, 21, 32, 41, 52, 67, 8, 99);
        final OutputTag<Integer> tag1 = new OutputTag<Integer>("偶数"){};  //贴偶数
        final OutputTag<Integer> tag2 = new OutputTag<Integer>("奇数"){};  //贴奇数

        SingleOutputStreamOperator<Integer> ds2 = ds1.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(tag1, value);
                } else {
                    ctx.output(tag2, value);
                }
            }

        });

        DataStream<Integer> ds3 = ds2.getSideOutput(tag1);
        //ds3.print();

        DataStream<Integer> ds4 = ds2.getSideOutput(tag2);
        ds4.print();

        env.execute();
    }
}
