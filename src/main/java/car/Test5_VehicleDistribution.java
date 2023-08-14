package car;

import bean.AreaControl;
import bean.MonitorInfo;
import day4.Constants;
import deserialization.JSONDeserializationSchema;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class Test5_VehicleDistribution {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);  //设置并行度

        //2.设置数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop10:9092");
        properties.setProperty("group.id","g5");

        FlinkKafkaConsumer<MonitorInfo> consumer = new FlinkKafkaConsumer<MonitorInfo>("topic-car",
                new JSONDeserializationSchema<>(MonitorInfo.class),properties);
        DataStreamSource<MonitorInfo> ds1 = env.addSource(consumer);

        ds1.keyBy(v -> v.getAreaId())
           .window(TumblingProcessingTimeWindows.of(Time.minutes(10)))
           .apply(new WindowFunction<MonitorInfo, AreaControl, String, TimeWindow>() {

               @Override
               public void apply(String s, TimeWindow window,
                                 Iterable<MonitorInfo> input, Collector<AreaControl> out) throws Exception {
                   Set<String> set = new HashSet<>();
                   Integer carCount = 0;
                   for (MonitorInfo monitorInfo : input) {
                       set.add(monitorInfo.getCar());
                   }
                   String start = DateFormatUtils.format(window.getStart(), Constants.D1);
                   String end = DateFormatUtils.format(window.getEnd(),Constants.D1);
                   out.collect(new AreaControl(null,s,set.size(),start,end));
               }
           }).addSink(JdbcSink.sink(
                "insert into t_area_control values(null,?,?,?,?)",
                (PreparedStatement ps, AreaControl areaControl) -> {
                    ps.setString(1, areaControl.getAreaId());
                    ps.setInt(2, areaControl.getCarCount());
                    ps.setString(3, areaControl.getWindowStart());
                    ps.setString(4, areaControl.getWindowEnd());
                },
                JdbcExecutionOptions.builder().withBatchSize(100).withBatchIntervalMs(5000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(Constants.URL)
                        .withDriverName(Constants.DRIVER)
                        .withUsername(Constants.USERNAME)
                        .withPassword(Constants.PASSWORD)
                        .build()));

        env.execute();
    }

}
