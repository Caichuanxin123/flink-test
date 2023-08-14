package day2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class Test2_batch {

    public static void main(String[] args)throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //1.创建一个env对象
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.加载数据
        //fromElements在flink中作为一种数据源，可以被作为批处理，也可以作为流处理
        DataSet<String> dataStream = env.fromElements("hello,hello,world", "hello,hello,heihei",
                "hello,heihei,hello","hello,haha,hello");

        //3.转换处理
        //flatMap 字符串拆分炸裂     rdd.flatMap(v => v.split(","))
        DataSet<String> dataStream2 = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] arr = s.split(",");
                for (String s1 : arr) {
                    collector.collect(s1);
                }
            }
        });
        //map  映射
        DataSet<Tuple2<String, Integer>> dataStream3 = dataStream2.map(new MapFunction<String, Tuple2<String, Integer>>() {
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
        DataSet<Tuple2<String, Integer>> dataStream4 = dataStream3.groupBy(0).sum(1);


        //4.输出
        //dataStream4.print();
        if(parameterTool.has("output")){
            dataStream4.writeAsCsv(parameterTool.get("output"));
            env.execute("我的第一个flink程序,嘿嘿");
        }else{
            dataStream4.print();
        }

        //5.执行

    }

}
