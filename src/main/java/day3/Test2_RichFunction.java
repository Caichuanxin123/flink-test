package day3;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class Test2_RichFunction {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(2);

        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10",9999);
        ds1.map(new RichMapFunction<String, Tuple2<String,Integer>>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                //数据库连接的创建
                System.out.println("open方法");
            }

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value,getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void close() throws Exception {
                System.out.println("close方法");
            }

        }).print();


        env.execute();
    }

}
