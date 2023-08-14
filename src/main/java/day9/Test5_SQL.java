package day9;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Test5_SQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> ds1 = env.socketTextStream("hadoop10", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = ds1.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple2.of(arr[0], Integer.parseInt(arr[1]));
            }
        });

        /**
         * hello,1
         * hello,2
         */
        //1.SQL语法
        /*tenv.createTemporaryView("table1",ds2, $("word"),$("cnt"));

        Table table = tenv.sqlQuery("select word,sum(cnt) c1 " +
                "from table1  " +
                "group by word");*/

        //2.table API
        Table table = tenv.fromDataStream(ds2, $("word"),$("cnt"));
        Table table2 = table.groupBy($("word")).select($("word"), $("cnt").sum().as("cnt2"));

        /**
         * Tuple2<Boolean,类型>
         *     如果元组中的第一个元素是true，代表该元素为新增元素
         *     如果元组中的第一个元素是false，代表是从状态中移除的元素
         */
        DataStream<Tuple2<Boolean, Tuple2<String, Integer>>> ds3 =
                tenv.toRetractStream(table2, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        ds3.print();

        env.execute();
    }

}
