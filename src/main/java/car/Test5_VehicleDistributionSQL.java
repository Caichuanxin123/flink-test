package car;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test5_VehicleDistributionSQL {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);  //设置并行度

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("CREATE TABLE table1 (\n" +
                "  `actionTime`     BIGINT,\n" +
                "  `monitorId`      STRING,\n" +
                "  `cameraId`       STRING,\n" +
                "  `car`            STRING,\n" +
                "  `speed`          double,\n" +
                "  `roadId`         STRING,\n" +
                "  `areaId`         STRING,\n"  +
                "  `ts`             as proctime()\n"  +   // processing time
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-car',\n" +
                "  'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "  'properties.group.id' = 'c1',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")");


    }

}
