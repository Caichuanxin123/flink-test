package day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:10:10"}
 * {"username":"zs","price":15,"event_time":"2023-07-17 10:10:30"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:10:40"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:11:03"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:11:04"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:12:04"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 11:12:04"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 11:12:04"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 12:12:04"}
 */
public class Test1_FlinkSQL_Window {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `username`    STRING,\n" +
                "  `price`    \tINT,\n" +
                "  `event_time`  TIMESTAMP(3),\n" +
                "   watermark for event_time as event_time - interval '3' second\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topicx',\n" +
                "  'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")");

        //TUMBLE 滚动窗口大小1分钟 延迟时间3秒
        /*tenv.executeSql("select window_start,window_end,username,count(*) cnt,sum(price) total_price " +
                "from table(TUMBLE(TABLE KafkaTable, DESCRIPTOR(event_time), INTERVAL '60' second)) " +
                "group by window_start,window_end,username").print();*/

        // HOP 滑动窗口大小5分钟,窗口间隔1分钟 延迟时间3秒
        // table(HOP(TABLE KafkaTable, DESCRIPTOR(event_time),INTERVAL '1' minutes, INTERVAL '5' minutes))
        // 窗口大小是1天,窗口间隔1小时
        /*tenv.executeSql("select window_start,window_end,username,count(*) cnt,sum(price) total_price " +
                "from table(HOP(TABLE KafkaTable, DESCRIPTOR(event_time),INTERVAL '1' hours, INTERVAL '1' days)) " +
                "group by window_start,window_end,username").print();*/

        System.out.println("=====================================================================================");

        //CUMULATE
        tenv.executeSql("select window_start,window_end,username,count(*) cnt,sum(price) total_price " +
                "from table(CUMULATE(TABLE KafkaTable, DESCRIPTOR(event_time),INTERVAL '1' second, INTERVAL '1' second)) " +
                "group by window_start,window_end,username").print();

        //+----+-------------------------+-------------------------+--------------------------------+----------------------+-------------+
        //| op |            window_start |              window_end |                       username |                  cnt | total_price |
        //+----+-------------------------+-------------------------+--------------------------------+----------------------+-------------+
        //| +I | 2023-07-17 00:00:00.000 | 2023-07-17 11:00:00.000 |                             zs |                    6 |         115 |

        //tenv.executeSql("select * from KafkaTable").print();
    }

}
