package day7;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Test4_windowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //hello,1
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);
        //hello,2
        DataStreamSource<String> ds2 = env.socketTextStream("hadoop10", 9998);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds3 = ds1.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple2.of(arr[0], Integer.parseInt(arr[1]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds4 = ds2.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple2.of(arr[0], Integer.parseInt(arr[1]));
            }
        });


        DataStream<Tuple3<String, Integer, Integer>> ds5 = ds3.join(ds4)
                .where(v -> v.f0)
                .equalTo(v -> v.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first,
                                                                 Tuple2<String, Integer> second) throws Exception {
                        return Tuple3.of(first.f0, first.f1, second.f1);
                    }
                });

        ds5.print();

        env.execute();
    }
}
