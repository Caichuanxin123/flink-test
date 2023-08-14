package car;

import bean.MonitorInfo;
import pengge.sink.HBaseSinkFunction;
import pengge.sink.HbaseBaseMap;
import deserialization.JSONDeserializationSchema;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class Test4_IllegalVehiclesTrack {


    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);  //设置并行度

        //2.设置数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop10:9092");
        properties.setProperty("group.id","g4");

        /*FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("topic-car",
                new SimpleStringSchema(),properties);
        DataStreamSource<String> ds1 = env.addSource(consumer);

        SingleOutputStreamOperator<MonitorInfo> ds2 = ds1.map(new MapFunction<String, MonitorInfo>() {
            @Override
            public MonitorInfo map(String value) throws Exception {
                String[] arr = value.split(",");
                return new MonitorInfo(Long.parseLong(arr[0]),arr[1],arr[2],arr[3],Double.parseDouble(arr[4]),arr[5],arr[6]);
            }
        });*/

        //1686647524,0005,1,豫A99997,60,01,20
        /*FlinkKafkaConsumer<MonitorInfo> consumer = new FlinkKafkaConsumer<MonitorInfo>("topic-car",
                new CsvDeserializationSchema(),properties);
        DataStreamSource<MonitorInfo> ds1 = env.addSource(consumer);*/

        FlinkKafkaConsumer<MonitorInfo> consumer = new FlinkKafkaConsumer<MonitorInfo>("topic-car",
                new JSONDeserializationSchema<>(MonitorInfo.class),properties);
        DataStreamSource<MonitorInfo> ds1 = env.addSource(consumer);

        //SinkFunction sinkFunction = new HbaseSinkFunction("t_track_info", "cf1");

        ds1.filter(new RichFilterFunction<MonitorInfo>() {

            Connection connection;
            PreparedStatement ps;
            ResultSet rs;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = JdbcUtils.getconnection();
                ps = connection.prepareStatement("select id,car from t_violation_list where car = ?");
            }

            @Override
            public boolean filter(MonitorInfo value) throws Exception {
                ps.setString(1,value.getCar());
                rs = ps.executeQuery();
                boolean flag = false;
                if(rs.next()){
                    flag = true;
                }
                return flag;
            }

            @Override
            public void close() throws Exception {
                JdbcUtils.release(rs,ps,connection);
            }

        }).map(new HbaseBaseMap<>())
          .addSink(new HBaseSinkFunction("t_track_info", "cf1",1));


        env.execute();
    }

}
