package day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test3 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //{"ts":"2018-04-13 12:00:00","cardid":"1","location":"Beijing","action":"Consumption"}
        //{"ts":"2018-04-13 12:01:00","cardid":"1","location":"ZhengZhou","action":"Xxxx"}
        //{"ts":"2018-04-13 12:03:00","cardid":"1","location":"Shanghai","action":"Consumption"}
        //{"ts":"2018-04-13 12:03:16","cardid":"1","location":"ZhengZhou","action":"Consumption"}
        //{"ts":"2018-04-13 12:03:17","cardid":"1","location":"Shenzhen","action":"Consumption"}
        //{"ts":"2018-04-13 12:07:00","cardid":"1","location":"Beijing","action":"Consumption"}
        //{"ts":"2018-04-13 12:21:00","cardid":"1","location":"Shanghai","action":"Consumption"}
        //{"ts":"2018-04-13 12:41:00","cardid":"1","location":"Beijing","action":"Consumption"}
        tenv.executeSql("CREATE TABLE table1 (\n" +
                "  `ts`             timestamp(3),\n" +
                "  `cardid`         STRING,\n" +
                "  `location`       STRING,\n" +
                "  `action`         STRING,\n" +
             // "   ts2             as proctime()\n" +
                "   WATERMARK FOR ts as ts - INTERVAL '0' SECOND"  +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic3',\n" +
                "  'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")");
        /*
         *
         * select *
         * from table1
         * match_recognize(
         *   partition by cardid
         *   order by ts
         *   measures
         *      e1.ts as `start_ts`,
         *      last(e2.ts) as `end_ts`,
         *      e1.action as `event`
         *   one row per match
         *   after match skip to next row
         *   pattern(e1 e2) within interval '10' minute
         *   define
         *      e1 as e1.action = 'Consumption',
         *      e2 as e2.action = 'Consumption' and e1.location != e2.location
         * )
         *
         */
        tenv.executeSql("select *\n" +
                "from table1\n" +
                "match_recognize(\n" +
                "  partition by cardid\n" +
                "  order by ts\n" +
                "  measures\n" +
                "     e1.ts as `start_ts`,\n" +
                "     last(e2.ts) as `end_ts`,\n" +
                "     e1.action as `event`\n" +
                "  one row per match\n" +
                "  AFTER MATCH SKIP PAST LAST ROW\n" +    //AFTER MATCH SKIP TO NEXT ROW
                "  pattern(e1 B* e2) within interval '10' minute \n" +
                "  define\n" +
                "     e1 as e1.action = 'Consumption',\n" +
                "     B  as B.action <> 'Consumption',\n" +
                "     e2 as e2.action = 'Consumption' and e1.location <> e2.location\n" +
                ")").print();
    }

}
