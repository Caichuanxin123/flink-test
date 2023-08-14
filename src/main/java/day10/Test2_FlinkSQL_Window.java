package day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 滚动窗口大小1分钟 延迟时间3秒
 *
 * {"username":"zs","price":20}
 * {"username":"lisi","price":15}
 * {"username":"lisi","price":20}
 * {"username":"zs","price":20}
 * {"username":"zs","price":20}
 * {"username":"zs","price":20}
 * {"username":"zs","price":20}
 *
 */
public class Test2_FlinkSQL_Window {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `username`    STRING,\n" +
                "  `price`    \tINT,\n" +
                "  `event_time`  as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic1',\n" +
                "  'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")");

        tenv.executeSql("select window_start,window_end,username,count(*) cnt,sum(price) total_price " +
                "from table(TUMBLE(TABLE KafkaTable, DESCRIPTOR(event_time), INTERVAL '60' second)) " +
                "group by window_start,window_end,username").print();

        //tenv.executeSql("select * from KafkaTable").print();
    }

}
