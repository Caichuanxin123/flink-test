package day10;

import deserialization.JSONDeserializationSchema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class Test3_FlinkTopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop10:9092");
        properties.setProperty("group.id", "car-group1");

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("topic1", new JSONDeserializationSchema<>(User.class),
                properties);
        DataStream<User> ds1 = env.addSource(consumer);
        //ds1.print();
        /*ds1.assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<User>() {
                    @Override
                    public long extractTimestamp(User element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }))*/

        ds1.keyBy(v -> v.getUsername())
           .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
           .aggregate(new AggregateFunction<User, Long, Long>() {

               @Override
               public Long createAccumulator() {   //创建累加器，并且初始化值
                   return 0L;
               }//创建累加器，给初始值

               @Override
               public Long add(User value, Long accumulator) {
                   return accumulator + 1;
               }


               @Override
               public Long getResult(Long accumulator) {
                   return accumulator;
               }

               @Override
               public Long merge(Long a, Long b) {//批处理使用
                   System.out.println(" ---------------- merge -----------------");
                   return null;
               }
           }, new WindowFunction<Long, Tuple3<String,Long,Long>, String, TimeWindow>() {
               @Override
               public void apply(String s, TimeWindow window, Iterable<Long> input,
                                 Collector<Tuple3<String,Long,Long>> out) throws Exception {
                   //Iterable<Long> input  只有一条数据,因为累加器已经累加了,减少内存存储压力
                   Long cnt = input.iterator().next();
                   out.collect(Tuple3.of(s,cnt,window.getEnd()));
               }
           }).keyBy(v -> v.f2)
             .process(new ProcessFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>>() {

                 ListState<Tuple2<String, Long>> listState;
                 @Override
                 public void open(Configuration parameters) throws Exception {
                     ListStateDescriptor<Tuple2<String,Long>> tListStateDescriptor
                             = new ListStateDescriptor<Tuple2<String,Long>>("xxx",
                             TypeInformation.of(new TypeHint<Tuple2<String,Long>>() { }));
                             /*Types.TUPLE(Types.STRING, Types.LONG)*/
                     listState = getRuntimeContext().getListState(tListStateDescriptor);
                 }

                 //定时器
                 @Override
                 public void onTimer(long timestamp, OnTimerContext ctx,
                                     Collector<Tuple2<String, Long>> out) throws Exception {
                     Iterable<Tuple2<String, Long>> iter = listState.get();
                     List<Tuple2<String,Long>> list = new ArrayList<>();
                     for (Tuple2<String, Long> tuple2 : iter) {
                         list.add(tuple2);
                     }

                     list.sort((v1,v2)->{
                         if(v1.f1 > v2.f1){
                             return -1;
                         }else{
                             return 1;
                         }
                     });


                     int j = list.size() >= 3 ? 3:list.size();
                     for (int i = 0; i < j; i++) {
                         Tuple2<String, Long> tuple2 = list.get(i);
                         out.collect(tuple2);
                         //System.out.println(tuple2);
                     }
                 }

                 //过来一个元素，就会执行一次
                 @Override
                 public void processElement(Tuple3<String, Long, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                     //System.out.println(value + " <--------- ");
                     listState.add(Tuple2.of(value.f0,value.f1));
                     ctx.timerService().registerProcessingTimeTimer(value.f2+1000);  //注册一个定时器
                 }

             }).print();

        env.execute();
    }

}
