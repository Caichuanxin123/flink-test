package day5;

import day4.Constants;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

public class Test2_EventTimeTumblingWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> ds = env.socketTextStream("hadoop10", 9999);
        /**
         * hello,2023-06-13 14:27:12
         * hello,2023-06-13 14:28:13
         * hello,2023-06-13 14:29:20
         * hello,2023-06-13 14:30:00
         * hello,2023-06-13 14:31:00
         *
         *
         *  key,value,start,end
         * (hello,3,2023-06-13 14:20:00,2023-06-13 14:30:00)
         * (hello,2,2023-06-13 14:30:00,2023-06-13 14:40:00)
         */
        ds.map(new MapFunction<String, Tuple3<String,Long, Integer>>() {

            @Override
            public Tuple3<String,Long, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                Date date = DateUtils.parseDate(arr[1], Constants.D1);
                return Tuple3.of(arr[0],date.getTime(),1);
            }

        }).assignTimestampsAndWatermarks(   //assignTimestampsAndWatermarks
                WatermarkStrategy.<Tuple3<String,Long, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, Long, Integer> element, long recordTimestamp) {
                                return element.f1;   //eventTime
                            }
                        })
        )/*.keyBy(v -> v.f0)
          .window(TumblingEventTimeWindows.of(Time.minutes(10)))  //设置滚动窗口

          .apply(new WindowFunction<Tuple3<String,Long, Integer>, Tuple4<String,Integer,String,String>, String, TimeWindow>() {
              @Override
              public void apply(String key,
                                TimeWindow window,
                                Iterable<Tuple3<String,Long, Integer>> input,
                                Collector<Tuple4<String,Integer,String,String>> out) throws Exception {

                  String start = DateFormatUtils.format(window.getStart(), Constants.D1);
                  String end = DateFormatUtils.format(window.getEnd(), Constants.D1);
                  int sum = 0;
                  for (Tuple3<String,Long, Integer> tuple3: input) {
                      sum += tuple3.f2;
                  }

                  out.collect(Tuple4.of(key,sum,start,end));
              }
          })*/.print();

        env.execute();
    }
}
