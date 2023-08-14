package day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SpeedingvehicleCount_FlinkSql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `action_time` BIGINT,\n" +
                "  `monitor_id` STRING,\n" +
                "  `camera_id` STRING,\n" +
                "  `car` STRING,\n" +
                "  `speed` DOUBLE,\n" +
                "  `road_id` STRING,\n" +
                "  `area_id` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-car',\n" +
                "  'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");


        tenv.executeSql("CREATE TABLE MySQLTable (\n" +
//                "  id INT,\n" +
                "  car STRING,\n" +
                "  monitor_id STRING,\n" +
                "  road_id STRING,\n" +
                "  real_speed DECIMAL(5,2),\n" +
                "  limit_speed INT,\n" +
                "  action_time BIGINT\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop10:3306/car?useSSL=false&characterEncoding=utf8&useUnicode=true',\n" +
                "   'table-name' = 't_speeding_info',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")");

        tenv.executeSql("CREATE TABLE MyUserTable2 (\n" +
                "  monitor_id STRING,\n" +
                "  road_id STRING,\n" +
                "  speed_limit INT,\n" +
                "  area_id STRING,\n" +
                "  PRIMARY KEY (monitor_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop10:3306/car?useSSL=false&characterEncoding=utf8&useUnicode=true',\n" +
                "   'table-name' = 't_monitor_info',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")");

     tenv.executeSql("insert into MySQLTable " +
                "select t1.car,t1.monitor_id,t1.road_id, " +
                " t1.speed,case when t2.speed_limit is not null then t2.speed_limit else 60 end" +
                ",t1.action_time from KafkaTable t1 " +
                " left join MyUserTable2 t2 on t1.monitor_id=t2.monitor_id  " +
                "WHERE t1.speed > case when t2.speed_limit is not null then t2.speed_limit else 60 end");


    }
}
