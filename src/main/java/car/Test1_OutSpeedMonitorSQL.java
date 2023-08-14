package car;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 从kafka消费数据 topic-car
 * 0.该项目不属于实时数仓(数仓分层)，因为没有分层所以不叫数仓、属于flink实时项目
 *   flink处理kafka下topic中的数据，处理过程中会涉及到一些维表(hbase)，处理结果存储到外部存储中(hbase/mysql)
 *   简单的说就是一个需求，一块处理代码
 * 1.你们项目的总共多少个topic
 *   我们项目的总共的topic挺多的，但是我负责的需求主要就是处理一个topic中的数据，但是我开发的需求还涉及到很多hbase、mysql的表的数据
 * 2.为什么使用hbase作为维表,因为不想影响业务系统正常运行
 * 3.计算结果：正常写到mysql，但是有些需求计算结果数据量还是比较大，就写到hbase
 *   flink写数据到mysql,提高写入速度
 *       sink.buffer-flush.max-rows
 *       sink.buffer-flush.interval
 *
 */
public class Test1_OutSpeedMonitorSQL {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //1.声明kafka对应flinksql的表   topic-car        source
        /**
         *
         * {"actionTime":1686647522,"monitorId":"0003","cameraId":"1","car":"豫A99999","speed":60,"roadId":"01","areaId":"20"}
         * {"actionTime":1686647523,"monitorId":"0004","cameraId":"1","car":"豫A99999","speed":80,"roadId":"01","areaId":"20"}
         * {"actionTime":1686647523,"monitorId":"0004","cameraId":"1","car":"豫A99999","speed":80,"roadId":"01","areaId":"10"}
         * {"actionTime":1686647524,"monitorId":"0005","cameraId":"1","car":"豫A99998","speed":60,"roadId":"01","areaId":"30"}
         * {"actionTime":1686647524,"monitorId":"0005","cameraId":"1","car":"豫A99997","speed":60,"roadId":"01","areaId":"10"}
         * {"actionTime":1686647522,"monitorId":"0003","cameraId":"1","car":"豫B99999","speed":60,"roadId":"01","areaId":"10"}
         * {"actionTime":1686647523,"monitorId":"0004","cameraId":"1","car":"豫C99999","speed":80,"roadId":"01","areaId":"20"}
         * {"actionTime":1686647523,"monitorId":"0004","cameraId":"1","car":"豫D99999","speed":80,"roadId":"01","areaId":"30"}
         * {"actionTime":1686647524,"monitorId":"0005","cameraId":"1","car":"豫A99998","speed":60,"roadId":"01","areaId":"30"}
         * {"actionTime":1686647524,"monitorId":"0005","cameraId":"1","car":"豫A99997","speed":60,"roadId":"01","areaId":"10"}
         */
        tenv.executeSql("CREATE TABLE table1 (\n" +
                "  `actionTime` BIGINT,\n" +
                "  `monitorId`  STRING,\n" +
                "  `cameraId`   STRING,\n" +
                "  `car`        STRING,\n" +
                "  `speed`      double,\n" +
                "  `roadId`     STRING,\n" +
                "  `areaId`     STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-car',\n" +
                "  'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "  'properties.group.id' = 'c1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");


        //2.声明mysql中的t_monitor_info对应的flinksql的表    维表
        tenv.executeSql("CREATE TABLE table2 (\n" +
                "  monitor_id    STRING,\n" +
                "  road_id       STRING,\n" +
                "  speed_limit   INT,\n" +
                "  area_id       STRING,\n" +
                "  PRIMARY KEY (monitor_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop10:3306/car?useSSL=false&useUnicode=true&characterEncoding=utf8',\n" +
                "   'table-name' = 't_monitor_info',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")");


        //3.声明mysql中的t_speeding_info对应的flinksql中的表       sink
        tenv.executeSql("CREATE TABLE table3 (\n" +
                "  car           STRING,\n" +
                "  monitor_id    STRING,\n" +
                "  road_id       STRING,\n" +
                "  real_speed    DOUBLE,\n" +
                "  limit_speed   INT,\n" +
                "  action_time   BIGINT,\n" +
                "  PRIMARY KEY (car,action_time) NOT ENFORCED" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop10:3306/car?useSSL=false&useUnicode=true&characterEncoding=utf8',\n" +
                "   'table-name' = 't_speeding_info',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")");

        //4.查询
        tenv.executeSql("create view table4 as select car,monitorId,roadId,speed,speed_limit,actionTime " +
                "from( " +
                "   select t1.*,if(t2.monitor_id is null,60,t2.speed_limit) speed_limit " +
                "   from table1 t1 left join table2 t2 " +
                "   on t1.monitorId = t2.monitor_id " +
                ")t3 where speed > speed_limit * 1.1");

        //5.写入
        tenv.executeSql("insert into table3 select * from table4");

        //tenv.sqlQuery("select * from table4").execute().print();
    }

}
