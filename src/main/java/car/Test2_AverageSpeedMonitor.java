package car;

import bean.AverageSpeed;
import bean.MonitorInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.util.Properties;

/**
 *
 */
public class Test2_AverageSpeedMonitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 卡口的实时拥堵情况，其实就是通过卡口的车辆平均车速和通过的车辆的数量，为了统计实时的平均车速，
         * 我设定一个滑动窗口，窗口长度是为5分钟，滑动步长为1分钟。平均车速=当前窗口内通过车辆的车速之和 / 当前窗口内通过的车辆数量
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop10:9092");
        /**
         * 讲kafka的时候, group.id  同一个消费者组中的消费者可以同时消费一个topic下的不同分区，但是不能同时消费同一个分区
         *               一个分区不能被同一个组下的多个消费者同时消费，但是一个消费者可以同时消费多个分区
         *                           consumerGroup
         *               topica-0       g1-消费者1
         *               topica-1       g2-消费者1
         *
         * flink [group.id]    kafka-connector
         * 启动flink程序，设置的topic和group.id是一样的,两个程序可以消费到相同的数据。flink自定义kafka-connector管理offset，并没有使用
         * kafka原本的kafkaconsuer管理offset,目的为了 exactly once （精准一次性,不丢失消费的数据，也不重复消费）
         *
         */
        properties.setProperty("group.id", "car-group2");
        properties.setProperty("auto.offset.reset","latest");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "2000");
        properties.setProperty("flink.partition-discovery.interval-millis","5000");

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("topic-car", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        DataStream<String> ds1 = env.addSource(consumer);

        SingleOutputStreamOperator<MonitorInfo> ds2 = ds1.map(new MapFunction<String, MonitorInfo>() {
            @Override
            public MonitorInfo map(String value) throws Exception {
                String[] arr = value.split(",");
                return new MonitorInfo(Long.parseLong(arr[0]),arr[1],arr[2],arr[3],Double.parseDouble(arr[4]),arr[5],arr[6]);
            }
        });


        ds2.keyBy(v -> v.getMonitorId())
           .window(SlidingProcessingTimeWindows.of(Time.minutes(5),Time.minutes(1)))
           .apply(new WindowFunction<MonitorInfo, AverageSpeed, String, TimeWindow>() {
               @Override
               public void apply(String s, TimeWindow window,
                                 Iterable<MonitorInfo> input,
                                 Collector<AverageSpeed> out) throws Exception {
                   double sum = 0; //存储总速度
                   int sum2 = 0;   //存储车数量
                   for (MonitorInfo monitorInfo : input) {
                       sum += monitorInfo.getSpeed();
                       sum2 += 1;
                   }
                   long start = window.getStart();
                   long end = window.getEnd();
                   out.collect(new AverageSpeed(null,start,end,s,sum/sum2,sum2));
               }
               }).addSink(JdbcSink.sink(
                    "insert into t_average_speed(id,start_time,end_time,monitor_id,avg_speed,car_count) values(null,?,?,?,?,?)",
                    (PreparedStatement ps, AverageSpeed averageSpeed) -> {
                        ps.setLong(1,averageSpeed.getStartTime());
                        ps.setLong(2,averageSpeed.getEndTime());
                        ps.setString(3,averageSpeed.getMonitorId());
                        ps.setDouble(4,averageSpeed.getAvgSpeed());
                        ps.setInt(5,averageSpeed.getCarCount());
                    },
                    JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(5000).build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl("jdbc:mysql://hadoop10:3306/car?useSSL=false&useUnicode=true&characterEncoding=utf8")
                            .withDriverName("com.mysql.jdbc.Driver")
                            .withUsername("root")
                            .withPassword("123456")
                            .build()));

        env.execute();
    }

}
