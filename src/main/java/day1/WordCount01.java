package day1;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount01 {

    public static void main(String[] args) throws Exception {
        //1.创建一个env对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);   //批处理
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING); //流处理

        //2.加载数据
        //fromElements在flink中作为一种数据源，可以被作为批处理，也可以作为流处理
        DataStream<String> dataStream = env.fromElements("hello,hello,world", "hello,hello,heihei",
                                                                "hello,heihei,hello","hello,haha,hello");

        //3.转换处理
        //flatMap 字符串拆分炸裂     rdd.flatMap(v => v.split(","))
        DataStream<String> dataStream2 = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] arr = s.split(",");
                for (String s1 : arr) {
                    collector.collect(s1);
                }
            }
        });
        //map  映射
        DataStream<Tuple2<String, Integer>> dataStream3 = dataStream2.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });


        //分组、聚合
        /**
         *  (hello,1)
         *  (hello,1)
         *  (hello,1)
         */
        DataStream<Tuple2<String, Integer>> dataStream4 = dataStream3.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }

        }).sum("f1");


        //4.输出
        dataStream4.print();
       // dataStream4.writeAsText("hdfs://hadoop10:8020/out234");

        //5.执行
        env.execute("我的第一个flink程序,嘿嘿");


    }

}
