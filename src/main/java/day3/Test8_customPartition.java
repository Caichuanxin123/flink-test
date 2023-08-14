package day3;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test8_customPartition {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> ds1 = env.fromElements("北京","上海","北京","郑州","武汉","重庆","成都");

        ds1.partitionCustom(new Partitioner<String>() {

            int i = 0;
            @Override
            public int partition(String key, int numPartitions) {
                if("北京".equals(key) || "上海".equals(key) || "重庆".equals(key) || "天津".equals(key)){
                    return 0;
                }else{
                    return 1;
                }
            }

        }, new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }

        }).print();


        env.execute();
    }
}
