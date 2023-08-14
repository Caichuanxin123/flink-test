package car;

import bean.MonitorInfo;
import bean.ViolationList;
import day4.Constants;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class Test3_RepetitionCarWarningAnalysis {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);  //设置并行度

        //2.设置数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop10:9092");
        properties.setProperty("group.id","g3");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("topic-car",
                new SimpleStringSchema(),properties);
        DataStreamSource<String> ds1 = env.addSource(consumer);

        SingleOutputStreamOperator<MonitorInfo> ds2 = ds1.map(new MapFunction<String, MonitorInfo>() {
            @Override
            public MonitorInfo map(String value) throws Exception {
                String[] arr = value.split(",");
                return new MonitorInfo(Long.parseLong(arr[0]),arr[1],arr[2],arr[3],Double.parseDouble(arr[4]),arr[5],arr[6]);
            }
        });

        //1686647521,0001,1,豫A99999,50,01,20
        //1686647522,0002,1,豫A99999,60,01,20
        ds2.keyBy(v -> v.getCar())
           .flatMap(new RichFlatMapFunction<MonitorInfo, ViolationList>() {

               ValueState<MonitorInfo> valueState;

               @Override
               public void open(Configuration parameters) throws Exception {
                   ValueStateDescriptor<MonitorInfo> valueStateDescriptor
                           = new ValueStateDescriptor<MonitorInfo>("vs",MonitorInfo.class);
                   valueState = getRuntimeContext().getState(valueStateDescriptor);
               }

               @Override
               public void flatMap(MonitorInfo newInfo, Collector<ViolationList> out) throws Exception {
                   MonitorInfo oldInfo = valueState.value();
                   if(oldInfo == null){
                       oldInfo = newInfo;
                   }else{
                       if(newInfo.getActionTime() - oldInfo.getActionTime() < 10
                               && newInfo.getMonitorId() != oldInfo.getMonitorId()){
                            out.collect(
                                    new ViolationList(null,newInfo.getCar(),"涉嫌套牌车",System.currentTimeMillis())
                            );
                       }
                   }
                   valueState.update(newInfo);
               }

           }).addSink(JdbcSink.sink("INSERT INTO `t_violation_list` VALUES (null,?,?,?)",(ps,value)->{
                    ps.setString(1,value.getCar());
                    ps.setString(2,value.getViolation());
                    ps.setLong(3,value.getCreateTime());
                },
                JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(5000).build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUsername(Constants.USERNAME)
                        .withPassword(Constants.PASSWORD)
                        .withUrl(Constants.URL)
                        .withDriverName(Constants.DRIVER).build()));


        env.execute();
    }

}
