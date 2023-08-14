package day4;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 信号灯,车数量
 * 9,2
 * 8,5
 * 9,3
 * 8,1
 *
 *  .countWindow(5)   每个key出现5次，就触发计算，计算最近5条数据
 *  .countWindow(5,2) 每个key出现2次，就触发计算，计算最近5条数据
 */
public class Test8_CountWindow {

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
          //.countWindow(5)
          .countWindow(5,2)
          .sum("f1").print();

        env.execute();
    }
}
