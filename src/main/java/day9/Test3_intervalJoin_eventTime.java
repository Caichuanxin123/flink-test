package day9;

import day4.Constants;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

public class Test3_intervalJoin_eventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * hello,1,2
         * hello,1,3
         */
        //hello,1,2023-06-15 17:23:00
        //hello,5,2023-06-15 17:24:00
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);

        //hello,2,2023-06-15 17:21:00
        //hello,3,2023-06-15 17:24:00
        //hello,3,2023-06-15 17:25:00
        DataStreamSource<String> ds2 = env.socketTextStream("hadoop10", 9998);

        SingleOutputStreamOperator<Tuple3<String, Integer,Long>> orangeStream =
                ds1.map(new MapFunction<String, Tuple3<String, Integer,Long>>() {
            @Override
            public Tuple3<String, Integer,Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                Date date = DateUtils.parseDate(arr[2], Constants.D1);
                return Tuple3.of(arr[0], Integer.parseInt(arr[1]),date.getTime());
            }
        }).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple3<String, Integer,Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                             .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                                 @Override
                                 public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                                     return element.f2;
                                 }
                             })
         );

        SingleOutputStreamOperator<Tuple3<String, Integer,Long>> greenStream =
                ds2.map(new MapFunction<String, Tuple3<String, Integer,Long>>() {
                    @Override
                    public Tuple3<String, Integer,Long> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        Date date = DateUtils.parseDate(arr[2], Constants.D1);
                        return Tuple3.of(arr[0], Integer.parseInt(arr[1]),date.getTime());
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer,Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                );

        orangeStream.keyBy(v -> v.f0)
                .intervalJoin(greenStream.keyBy(v -> v.f0))
                .between(Time.minutes(-2),Time.minutes(1))
                .process(new ProcessJoinFunction<Tuple3<String, Integer, Long>,
                                                 Tuple3<String, Integer, Long>,
                                                 Tuple3<String,Integer,Integer>>() {

                    @Override
                    public void processElement(Tuple3<String, Integer, Long> left,
                                               Tuple3<String, Integer, Long> right,
                                               Context ctx,
                                               Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        out.collect(Tuple3.of(left.f0,left.f1,right.f1));
                    }

                }).print();

        env.execute();
    }
}
