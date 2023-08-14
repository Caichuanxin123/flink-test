package car;

import bean.MonitorInfo;
import com.pengge.sink.HBaseSinkFunction;
import com.pengge.sink.HbaseBaseMap;
import deserialization.JSONDeserializationSchema;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class Test4_IllegalVehiclesTrackSQL {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE table1 ( \n" +
                "  `actionTime` BIGINT,\n" +
                "  `monitorId`  STRING,\n" +
                "  `cameraId`   STRING,\n" +
                "  `car`        STRING,\n" +
                "  `speed`      double,\n" +
                "  `roadId`     STRING,\n" +
                "  `areaId`     STRING,\n" +
                "   proctime AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-car',\n" +
                "  'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "  'properties.group.id' = 'c1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tenv.executeSql("CREATE TABLE table2 ( \n" +
                "  id    \t  \t\tINT, \n" +
                "  car         \t\tSTRING, \n" +
                "  violation   \t\tSTRING, \n" +
                "  create_time       BIGINT\n" +
                ") WITH ( \n" +
                "   'connector' = 'jdbc', \n" +
                "   'url' = 'jdbc:mysql://hadoop10:3306/car?useSSL=false&useUnicode=true&characterEncoding=utf8', \n" +
                "   'table-name' = 't_violation_list', \n" +
                "   'username' = 'root', \n" +
                "   'password' = '123456' \n" +
                ")");

        tenv.executeSql("create table table3(\n" +
                "   rowkey STRING,\n" +
                "   cf1 ROW<monitorId String,cameraId String,car String,roadId String,areaId String,speed Double,actionTime BIGINT>,\n" +
                "   PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ")with(\n" +
                "  'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 't_track_info',\n" +
                " 'zookeeper.quorum' = 'hadoop10:2181'\n" +
                ")");

        tenv.executeSql("insert into table3\n" +
                "select concat(t1.car,'_',cast(t1.actionTime as string))," +
                "ROW(t1.monitorId,t1.cameraId,t1.car,t1.roadId,t1.areaId,t1.speed,t1.actionTime)\n" +
                "from table1 t1 \n" +
                "inner join table2 for system_time as of t1.proctime as t2\n" +
                "on t1.car = t2.car");


    }

}
