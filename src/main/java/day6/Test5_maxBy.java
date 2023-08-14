package day6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test5_maxBy {
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

        ds2.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String,Integer>>() {

            ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> valueStateDescriptor
                        = new ValueStateDescriptor<Integer>("vs1", Integer.class);
                state = getRuntimeContext().getState(valueStateDescriptor);
            }

            /**
             * 城市名称,数字
             * 输入                   输出
             * 北京,20               北京,20
             * 北京,30               北京,30
             * 北京,10               北京,30
             */
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                //1.获取状态中的值
                //2.状态中有值
                //  输入的值和状态的值进行比较大小,谁大返回谁
                //3.状态中没有值
                //  返回输入的值
                //4.更新状态
                Integer oldValue = state.value();
                if(oldValue == null || oldValue < value.f1){
                    oldValue = value.f1;
                }
                state.update(oldValue);
                return Tuple2.of(value.f0,oldValue);
            }

        }).print();

        env.execute();
    }
}
