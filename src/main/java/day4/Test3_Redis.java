package day4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * kafka-console-producer --broker-list hadoop10:9092 --topic topic2
 */
public class Test3_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop10").setPort(6379).setPassword("123").build();

        DataStreamSource<String> ds = env.socketTextStream("hadoop10", 9999);
        ds.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value,1);
            }
        }).keyBy(v -> v.f0).sum(1)
          .addSink(new RedisSink<>(conf, new RedisMapper<Tuple2<String, Integer>>() {

                  /**
                   * 需求：将wordcount计算结果写入redis
                   *      hash结果  key1   key2   value
                   *               a1     hello   2
                   *                      haha    3
                   *
                   *      hset a1 hello  2
                   **/
              @Override
              public RedisCommandDescription getCommandDescription() {
                  return new RedisCommandDescription(RedisCommand.HSET, "a1");
              }

              @Override
              public String getKeyFromData(Tuple2<String, Integer> tuple2) {
                  return tuple2.f0;
              }

              @Override
              public String getValueFromData(Tuple2<String, Integer> tuple2) {
                  return tuple2.f1 + "";
              }

          }));



        env.execute();
    }
}