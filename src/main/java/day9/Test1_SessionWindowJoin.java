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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;

public class Test1_SessionWindowJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * hello,1,2
         */
        //hello,1,2023-06-15 17:23:10
        //hello,4,2023-06-15 17:33:10
        //hello,4,2023-06-15 17:39:10
        //hello,4,2023-06-15 17:48:10
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);

        //hello,2,2023-06-15 17:25:20
        //hello,2,2023-06-15 17:29:20
        //hello,3,2023-06-15 17:31:10
        //hello,3,2023-06-15 17:36:20
        //hello,3,2023-06-15 17:42:12
        DataStreamSource<String> ds2 = env.socketTextStream("hadoop10", 9998);

        SingleOutputStreamOperator<Tuple3<String, Integer,Long>> ds3 =
                ds1.map(new MapFunction<String, Tuple3<String, Integer,Long>>() {
                    @Override
                    public Tuple3<String, Integer,Long> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        Date date = DateUtils.parseDate(arr[2], Constants.D1);
                        return Tuple3.of(arr[0], Integer.parseInt(arr[1]),date.getTime());
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                );

        SingleOutputStreamOperator<Tuple3<String, Integer,Long>> ds4 =
                ds2.map(new MapFunction<String, Tuple3<String, Integer,Long>>() {
                    @Override
                    public Tuple3<String, Integer,Long> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        Date date = DateUtils.parseDate(arr[2], Constants.D1);
                        return Tuple3.of(arr[0], Integer.parseInt(arr[1]),date.getTime());
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                );

        DataStream<Tuple3<String, Integer, Integer>> ds5 = ds3.join(ds4)
                .where(v -> v.f0)
                .equalTo(v -> v.f0)
                .window(EventTimeSessionWindows.withGap(Time.minutes(5)))
                .apply(new JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Integer>>() {

                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple3<String, Integer, Long> first,
                                                                 Tuple3<String, Integer, Long> second) throws Exception {
                        return Tuple3.of(first.f0,first.f1,second.f1);
                    }

                });

        ds5.print();

        env.execute();
    }
}
