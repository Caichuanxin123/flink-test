package day6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;

public class Test2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);   //1

        DataStreamSource<String> ds1 = env.addSource(new SourceFunction<String>() {

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true){
                    String s = UUID.randomUUID().toString();
                    ctx.collect(s);
                    Thread.sleep(300);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<String> ds2 = ds1.rebalance().map(new RichMapFunction<String, String>() {  //4

            @Override
            public String map(String value) throws Exception {
                int j = getRuntimeContext().getIndexOfThisSubtask() + 1;
                return value + "_" + j;
            }

        });

        ds2.print();  //4

        env.execute();
    }

}
