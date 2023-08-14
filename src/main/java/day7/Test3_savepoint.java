package day7;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Test3_savepoint {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);
        ds1.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple2.of(arr[0],Integer.parseInt(arr[1]));
            }
        }).keyBy(v -> v.f0)
          .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>(){

              ValueState<Integer> state;
              @Override
              public void open(Configuration parameters) throws Exception {
                  ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("vs1",Integer.class);
                  state = getRuntimeContext().getState(valueStateDescriptor);
              }

              public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                  Integer oldValue = state.value();
                  if(oldValue == null || oldValue < value.f1){
                      oldValue = value.f1;
                  }
                  state.update(oldValue);
                  return Tuple2.of(value.f0,oldValue);
              }

            })
          .print();

        env.execute();
    }

}
