package day4;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Test4_TumblingWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("hadoop10", 9999);
        ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(s);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value,1);
            }

        }).keyBy(v -> v.f0)
          .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))  //设置滚动窗口
          //.sum("f1")
          .apply(new WindowFunction<Tuple2<String, Integer>, Tuple4<String,Integer,String,String>, String, TimeWindow>() {
              @Override
              public void apply(String key,
                                TimeWindow window,
                                Iterable<Tuple2<String, Integer>> input,
                                Collector<Tuple4<String,Integer,String,String>> out) throws Exception {
                  /*System.out.println(" key =  " + key);
                  System.out.println(" windowStart =  " + DateFormatUtils.format(window.getStart(),Constants.D1) );
                  System.out.println(" windowEnd =  " + DateFormatUtils.format(window.getEnd(),Constants.D1) );
                  for (Tuple2<String, Integer> tuple2 : input) {
                      System.out.println(tuple2);
                  }*/

                  String start = DateFormatUtils.format(window.getStart(), Constants.D1);
                  String end = DateFormatUtils.format(window.getEnd(), Constants.D1);
                  int sum = 0;
                  for (Tuple2<String, Integer> tuple2 : input) {
                      sum += tuple2.f1;
                  }

                  out.collect(Tuple4.of(key,sum,start,end));
              }
          }).print();

        /**
         * hello,hello,haha
         *
         * (hello,1)
         * (hello,1)
         *
         * (hello,2,窗口开始时间,窗口结束时间)
         */
        env.execute();
    }
}
