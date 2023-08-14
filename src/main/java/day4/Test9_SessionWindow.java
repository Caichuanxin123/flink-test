package day4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class Test9_SessionWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("hadoop10", 9999);
        ds.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple2.of(Integer.parseInt(arr[0]),Integer.parseInt(arr[1]));
            }
        }).keyBy(v -> v.f0)
          .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))  //当10s没有接收数据，则触发窗口计算
          .sum("f1").print();

        env.execute();
    }
}
