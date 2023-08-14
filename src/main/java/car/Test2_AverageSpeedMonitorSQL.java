package car;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 */
public class Test2_AverageSpeedMonitorSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //读取kafka中topic-car下的数据
        tenv.executeSql("CREATE TABLE table1 (\n" +
                "  `actionTime`     BIGINT,\n" +
                "  `monitorId`      STRING,\n" +
                "  `cameraId`       STRING,\n" +
                "  `car`            STRING,\n" +
                "  `speed`          double,\n" +
                "  `roadId`         STRING,\n" +
                "  `areaId`         STRING,\n"  +
                "  `event_time`   as  TO_TIMESTAMP(FROM_UNIXTIME( actionTime, 'yyyy-MM-dd HH:mm:ss')), \n"  +
                "   watermark for event_time as event_time - interval '0' second"  +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-car',\n" +
                "  'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "  'properties.group.id' = 'c1',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")");
        /**
         * select window_start,window_end,monitorId,sum(speed)/count(*) avg_speed,count(*) car_count
         * from table(hop(table table1,descriptor(event_time),interval '1' minutes,interval '5' minutes))
         * group by window_start,window_end,monitorId
         */

        //上述查询结果写到mysql表中
        tenv.executeSql("CREATE TABLE table3 (\n" +
                "  start_time    BIGINT,\n" +
                "  end_time      BIGINT,\n" +
                "  monitor_id    String,\n" +
                "  avg_speed     double,\n" +
                "  car_count     INT\n"     +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop10:3306/car?useSSL=false&useUnicode=true&characterEncoding=utf8',\n" +
                "   'table-name' = 't_average_speed',\n" +
                "   'username' = 'root',\n"  +
                "   'password' = '123456'\n" +
                ")");

        tenv.executeSql("CREATE TABLE table4 (\n" +
                "  start_time    BIGINT,\n" +
                "  end_time      BIGINT,\n" +
                "  monitor_id    String,\n" +
                "  avg_speed     double,\n" +
                "  car_count     INT,\n"     +
                "  rn            INT,\n"     +
                "  PRIMARY KEY (start_time,monitor_id) NOT ENFORCED" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop10:3306/car?useSSL=false&useUnicode=true&characterEncoding=utf8',\n" +
                "   'table-name' = 't_average_speed_topn',\n" +
                "   'username' = 'root',\n"  +
                "   'password' = '123456'\n" +
                ")");

        //HOP滑动窗口 窗口大小是5分钟 窗口间隔1分钟，统计每个卡口通过车的数量，平均车速
        tenv.executeSql("create view t1 as select " +
                "UNIX_TIMESTAMP(cast(window_start as string))*1000 window_start," +
                "UNIX_TIMESTAMP(cast(window_end as string))*1000 window_end,monitorId,avg(speed) avg_speed," +
                "cast(count(*) as int) car_count\n" +
                "from table(hop(table table1,descriptor(event_time),interval '1' minutes,interval '5' minutes))\n" +
                "group by window_start,window_end,monitorId");

        tenv.executeSql("insert into table3 select * from t1");
        /**
         * insert into table4
         * select window_start,window_end,monitorId,avg_speed,car_count,rn
         * from(
         *  select window_start,window_end,monitorId,avg_speed,car_count,
         *         row_number() over(partition by window_end order by avg_speed,car_count desc) rn
         *  from t1
         * )t2 where rn <= 2
         */
        tenv.executeSql(" insert into table4\n" +
                "select window_start,window_end,monitorId,avg_speed,car_count,cast(rn as int)\n" +
                "from(\n" +
                " select window_start,window_end,monitorId,avg_speed,car_count,\n" +
                "        row_number() over(partition by window_end order by avg_speed,car_count desc) rn\n" +
                " from t1\n" +
                ")t2 where rn <= 2  ");

    }

}
