package car;

import avro.shaded.com.google.common.cache.CacheBuilder;
import avro.shaded.com.google.common.cache.CacheLoader;
import avro.shaded.com.google.common.cache.LoadingCache;
import bean.MonitorInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

class A extends CacheLoader<String,Integer>{

    @Override
    public Integer load(String s) throws Exception {
        return null;
    }
}

/**
 * DROP TABLE IF EXISTS `t_monitor_info`;
 * CREATE TABLE `t_monitor_info` (
 *   `monitor_id` varchar(255) NOT NULL,
 *   `road_id` varchar(255) NOT NULL,
 *   `speed_limit` int(11) DEFAULT NULL,
 *   `area_id` varchar(255) DEFAULT NULL,
 *    PRIMARY KEY (`monitor_id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * -- 导入数据
 * INSERT INTO `t_monitor_info` VALUES ('0000', '02', 60, '01');
 * INSERT INTO `t_monitor_info` VALUES ('0001', '02', 60, '02');
 * INSERT INTO `t_monitor_info` VALUES ('0002', '03', 80, '01');
 * INSERT INTO `t_monitor_info` VALUES ('0004', '05', 100, '03');
 * INSERT INTO `t_monitor_info` VALUES ('0005', '04', 0, NULL);
 * INSERT INTO `t_monitor_info` VALUES ('0021', '04', 0, NULL);
 * INSERT INTO `t_monitor_info` VALUES ('0023', '05', 0, NULL);
 *
 * DROP TABLE IF EXISTS `t_speeding_info`;
 * CREATE TABLE `t_speeding_info` (
 *   `id` int(11) NOT NULL AUTO_INCREMENT,
 *   `car` varchar(255) NOT NULL,
 *   `monitor_id` varchar(255) DEFAULT NULL,
 *   `road_id` varchar(255) DEFAULT NULL,
 *   `real_speed` double DEFAULT NULL,
 *   `limit_speed` int(11) DEFAULT NULL,
 *   `action_time` bigint(20) DEFAULT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 * kafka-topics.sh --zookeeper hadoop10:2181 --create --topic topic-car --partitions 1 --replication-factor  1
 *
 * [root@hadoop10 ~]# kafka-console-producer.sh --broker-list hadoop10:9092 --topic topic-car
 * >1682219447,0001,1,豫A12345,34.5,01,20
 * >1682219447,0001,1,豫A12346,100,01,20
 *
 * `action_time`long--摄像头拍摄时间戳，精确到秒,
 * `monitor_id`string--卡口号,
 * `camera_id`string--摄像头编号,
 * `car`string--车牌号码,
 * `speed`double--通过卡口的速度,
 * `road_id`string--道路id,
 * `area_id`string--区域id,
 */
public class Test1_OutSpeedMonitor {


    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop10:9092");
        properties.setProperty("group.id", "car-group1");
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

        ds2.filter(new RichFilterFunction<MonitorInfo>() {

            Connection connection;
            PreparedStatement ps;
            ResultSet rs;
            LoadingCache<String, Integer> cache;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = JdbcUtils.getconnection();
                ps = connection.prepareStatement("select speed_limit from t_monitor_info where monitor_id = ?");

                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .expireAfterWrite(100, TimeUnit.MINUTES)
                        .build(new CacheLoader<String, Integer>() {
                            @Override
                            public Integer load(String monitorId) throws Exception {
                                ps.setString(1,monitorId);
                                rs = ps.executeQuery();
                                //如果t_monitor_info无法查询出该卡口的编号，则给定一个60的限速
                                int speed_limit = 60;
                                if(rs.next()){
                                    speed_limit = rs.getInt("speed_limit");
                                }
                                return speed_limit;
                            }
                        });

            }

            @Override
            public boolean filter(MonitorInfo value) throws Exception {
                Integer speed_limit = cache.get(value.getMonitorId());
                value.setSpeedLimit(speed_limit);
                return value.getSpeed() > speed_limit * 1.1; //超速10%，判定为超速
            }

            @Override
            public void close() throws Exception {
                JdbcUtils.release(rs,ps,connection);
            }

        }).addSink(JdbcSink.sink(
                "insert into t_speeding_info values(null,?,?,?,?,?,?)",
                (PreparedStatement ps, MonitorInfo monitorInfo) -> {
                    ps.setString(1, monitorInfo.getCar());
                    ps.setString(2, monitorInfo.getMonitorId());
                    ps.setString(3, monitorInfo.getRoadId());
                    ps.setDouble(4, monitorInfo.getSpeed());
                    ps.setInt(5, monitorInfo.getSpeedLimit());
                    ps.setLong(6, monitorInfo.getActionTime());

                },
                JdbcExecutionOptions.builder().withBatchSize(100).withBatchIntervalMs(5000).build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop10:3306/car?useSSL=false&useUnicode=true&characterEncoding=utf8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));

        env.execute();

    }

}


