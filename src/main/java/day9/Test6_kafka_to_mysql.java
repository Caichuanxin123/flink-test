package day9;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test6_kafka_to_mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //zs,18,success
        //lisi,19,fail
        /**
         *CREATE TABLE table1 (
         *   `name`   STRING,
         *   `age`    INT,
         *   `status` STRING
         * ) WITH (
         *   'connector' = 'kafka',
         *   'topic' = 'topic1',
         *   'properties.bootstrap.servers' = 'hadoop10:9092',
         *   'properties.group.id' = 'x1',
         *   'scan.startup.mode' = 'earliest-offset',
         *   'format' = 'csv'
         * )
         *
         *  {"name":"张三","age":25,"status":"success"}
         *  'format' = 'json'
         */

        tenv.executeSql("CREATE TABLE table1 (\n" +
                "   `name`   STRING,\n" +
                "   `age`    INT,\n" +
                "   `status` STRING\n" +
                " ) WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "   'topic' = 'topic1',\n" +
                "   'properties.bootstrap.servers' = 'hadoop10:9092',\n" +
                "   'properties.group.id' = 'x1',\n" +
                "   'scan.startup.mode' = 'latest-offset',\n" +
                "   'format' = 'json'\n" +
                " )");

        tenv.executeSql("CREATE TABLE table2 (\n" +
                "  name STRING,\n" +
                "  age  INT,\n" +
                "  status STRING\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop10:3306/car?useSSL=false&useUnicode=true&characterEncoding=utf8',\n" +
                "   'table-name' = 't_person',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")");

        tenv.executeSql("insert into table2 select name,age,status from table1 where status = 'success'");

        //env.execute();
    }

}
