package day9;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Test2_CoGroupJoin {
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

        DataStream<Tuple3<String, Integer, Integer>> ds5 = ds3.coGroup(ds4)
                .where(v -> v.f0)
                .equalTo(v -> v.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {

                    /**
                     *
                     * ds3
                     *      (a,1),(a,2)
                     *
                     * ds4
                     *
                     *  输出：(a,1,null)   (a,2,null)
                     *
                     * //join无法实现,因为ds4中没有对应的key，则join方法不会执行
                     * public Xxx join(Tuple2 first,Tuple2 second){
                     *
                     * }
                     */
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first,
                                        Iterable<Tuple2<String, Integer>> second,
                                        Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        //内连接
                        /*for (Tuple2<String, Integer> x1 : first) {
                            for (Tuple2<String, Integer> x2 : second) {
                                out.collect(Tuple3.of(x1.f0,x1.f1,x2.f1));
                            }
                        }*/
                        /**
                         * first  (a,1) (a,2)
                         * second
                         */
                        for (Tuple2<String, Integer> x1 : first) {
                            boolean flag = true;
                            for (Tuple2<String, Integer> x2 : second) {
                                flag = false;
                                out.collect(Tuple3.of(x1.f0,x1.f1,x2.f1));
                            }
                            if(flag){
                                out.collect(Tuple3.of(x1.f0,x1.f1,null));
                            }
                        }
                    }

                });

        ds5.print();

        env.execute();
    }
}
