package day3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Test1_RichFunction {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 富函数                 普通函数
         * RichFilterFunction    FilterFunction
         * RichFlatMapFunciton   FlatMapFunction
         * RichMapFunction       MapFunction
         * RichSourceFunction    SourceFunction
         *
         * 问题1：什么时候用带Rich的富函数
         *  getRuntimeContext()方法获取上下文信息
         *  重写open和close方法
         */

        DataStreamSource<String> env1 = env.socketTextStream("hadoop10", 8889);  //SourceFunction
        //env.addSource(new FlinkKafkaConsumer<Object>(""))                                  //RichParallelSourceFunction
        /**
         * kafka consumer组       topic-car
         *   consumer-1           partition-0
         *   consumer-2           partition-1
         *   consumer-3           partition-2
         *   consumer-4           partition-3
         *   consumer-5           partition-4
         */

        env.addSource(new RichParallelSourceFunction<Tuple2<String,Integer>>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println(" --- open ----");
            }

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                for (int i = 0; i < 5; i++) {
                    ctx.collect(Tuple2.of("hello",getRuntimeContext().getIndexOfThisSubtask()));
                    Thread.sleep(2000);
                }
            }

            @Override
            public void close() throws Exception {
                System.out.println(" --- close ----");
            }

            @Override
            public void cancel() {

            }

        }).print();


        env.execute();
    }

}
