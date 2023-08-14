package day7;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;


/**
 * 统计每个城市的最大值
 * 北京,10
 * 北京,6
 * 北京,11
 */
public class Test2_checkpoint {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //开启快照
        env.enableCheckpointing(10000);  //1s执行一次快照
        //快照的位置  状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop10:8020/flink/checkpoint"));
        //取消作业的时候，不要删除检查点目录
        env.getCheckpointConfig()
           .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //env.setRestartStrategy(RestartStrategies.fallBackRestart()); //无限重启
        //env.setRestartStrategy(RestartStrategies.noRestart());      //不重启

        //可以重启3次，每次重启时间间隔是10s
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(30)));

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.minutes(2),Time.seconds(30)));


        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);
        ds1.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                System.out.println(arr[2]);
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
