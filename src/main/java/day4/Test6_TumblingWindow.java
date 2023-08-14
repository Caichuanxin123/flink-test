package day4;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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
 * 需求：每隔5s，统计最近5s通过每个信号灯的车的数量
 * 9,5
 * 8,6
 */
public class Test6_TumblingWindow {

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
          .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
          .apply(new WindowFunction<Tuple2<Integer, Integer>, Tuple4<Integer,Integer,String,String>, Integer, TimeWindow>() {
              @Override
              public void apply(Integer key, TimeWindow window, Iterable<Tuple2<Integer, Integer>> input, Collector<Tuple4<Integer, Integer, String, String>> out) throws Exception {
                  int sum = 0;
                  for (Tuple2<Integer, Integer> tuple2 : input) {
                      sum += tuple2.f1;
                  }
                  String start = DateFormatUtils.format(window.getStart(),Constants.D1);
                  String end = DateFormatUtils.format(window.getEnd(),Constants.D1);
                  out.collect(Tuple4.of(key,sum,start,end));
              }
          })
          .print();

        env.execute();
    }
}
