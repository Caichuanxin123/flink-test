package day6;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class Test6_State {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("hadoop10", 9999);
        KeyedStream<Tuple2<String, Integer>, String> ds2 = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple2.of(arr[0], Integer.parseInt(arr[1]));
            }
        }).keyBy(v -> v.f0);

        ds2.flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String,List<Integer>>>() {
            ValueState<Integer> valueState;
            ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("vs1",Integer.class);
                valueState = getRuntimeContext().getState(valueStateDescriptor);

                ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("vs2",Integer.class);
                listState = getRuntimeContext().getListState(listStateDescriptor);
            }

            /**
             * 姓名,温度
             * 输入                      输出
             * 许宁,37
             * 许宁,38
             * 许宁,39
             * 许宁,35
             * 许宁,41               姓名,[39,40,41]
             * 许宁,40               姓名,[39,40,41,40]
             *
             * 题目：如果一个人的体温超过阈值38度，超过3次及以上，则输出姓名,[温度1,温度2,温度3]
             */

            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, List<Integer>>> out) throws Exception {
                if(value.f1 > 38){
                    listState.add(value.f1);
                    Integer value1 = valueState.value();  //存储状态之前的值
                    Integer value2 = value1 == null ? 1 : value1 + 1;  //输入温度超过38度，加1之后的值
                    valueState.update(value2);
                    if(value2 >= 3){
                        List<Integer> list = new ArrayList<>();
                        for (Integer v1 : listState.get()) {
                            list.add(v1);
                        }
                        out.collect(Tuple2.of(value.f0,list));
                    }
                }
            }

        }).print();

        env.execute();
    }
}
