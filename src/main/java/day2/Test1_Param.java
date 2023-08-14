package day2;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test1_Param {

    public static void main(String[] args)throws Exception {
        /**
         * 方式1：flink run -c day2.Test1_Param  /opt/app/flink.jar hdfs://hadoop10:8020/out06091
         *
         * 方式2：flink run -c day2.Test1_Param  /opt/app/flink-test-1.0-SNAPSHOT.jar --output hdfs://hadoop10:8020/out06092
         *
         * ParameterTool parameterTool = ParameterTool.fromArgs(args); 加args参数交给工具类处理
         * if(parameterTool.has("output")){       //判断是否有这个参数
         * 	path = parameterTool.get("output");   //获取参数值
         * }
         */

        //1.获取参数的工具类
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //1.创建一个env对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度，相当于spark的分区
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

        //map  映射  hello  (hello,1)
        DataStream<Tuple2<String, Integer>> dataStream3 = dataStream2.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s,1);  //new Tuple2<>(s, 1);
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
                return value.f0;  //分组(分流)的依据(字段)
            }

        }).sum("f1");


        //4.输出
        //dataStream4.print();
        if(parameterTool.has("output")){
            dataStream4.writeAsCsv(parameterTool.get("output"));
        }else{
            dataStream4.print();
        }

        //5.执行
        env.execute("我的第一个flink程序,嘿嘿");

    }

}
