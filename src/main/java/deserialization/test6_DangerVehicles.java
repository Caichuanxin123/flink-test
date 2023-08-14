package deserialization;

        import cep.test02;
        import deserialization.JSONDeserializationSchema;
        import lombok.AllArgsConstructor;
        import lombok.Data;
        import lombok.NoArgsConstructor;
        import lombok.SneakyThrows;
        import org.apache.commons.lang3.time.DateUtils;
        import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
        import org.apache.flink.api.common.eventtime.WatermarkStrategy;
        import org.apache.flink.api.common.functions.MapFunction;
        import org.apache.flink.api.common.functions.RichMapFunction;
        import org.apache.flink.api.common.serialization.SimpleStringSchema;
        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.api.java.tuple.Tuple3;
        import org.apache.flink.api.java.tuple.Tuple5;
        import org.apache.flink.cep.CEP;
        import org.apache.flink.cep.PatternSelectFunction;
        import org.apache.flink.cep.PatternStream;
        import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
        import org.apache.flink.cep.pattern.Pattern;
        import org.apache.flink.cep.pattern.conditions.SimpleCondition;
        import org.apache.flink.configuration.Configuration;
        import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
        import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
        import org.apache.flink.connector.jdbc.JdbcSink;
        import org.apache.flink.streaming.api.datastream.DataStreamSource;
        import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
        import org.apache.flink.streaming.api.windowing.time.Time;
        import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
        import org.apache.yetus.audience.InterfaceAudience;
        import sun.rmi.runtime.Log;
        import util.JdbcUtils;

        import javax.servlet.http.Part;
        import java.security.PrivateKey;
        import java.sql.Connection;
        import java.sql.PreparedStatement;
        import java.sql.ResultSet;
        import java.time.Duration;
        import java.util.Date;
        import java.util.List;
        import java.util.Map;
        import java.util.Properties;


public class test6_DangerVehicles {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers", "hadoop10:9092");
        pro.setProperty("group.id", "g1");

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("topicA", new SimpleStringSchema(), pro);

        DataStreamSource dataSource = env.addSource(consumer);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> ds1 = dataSource.map(new RichMapFunction() {

            @Override
            public void open(Configuration parameters) throws Exception {

            }

            @Override
            public Object map(Object value) throws Exception {
                return null;
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Long, String, Integer>>() {
                            @SneakyThrows
                            @Override
                            public long extractTimestamp(Tuple3<Long, String, Integer> element, long recordTimestamp) {
                                return element.f0;
                            }
                        })
        );


        //2.定义Pattern
        Pattern<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>> pattern = Pattern.<Tuple3<Long, String, Integer>>begin("first",
                AfterMatchSkipStrategy.skipPastLastEvent())     //匹配一次在匹配下一次
                .where(new SimpleCondition<Tuple3<Long, String, Integer>>() {



                    @Override
                    public boolean filter(Tuple3<Long, String, Integer> value) throws Exception {
                        Connection conn = JdbcUtils.getconnection();
                        PreparedStatement ps = conn.prepareStatement("select limit_speed from t_speeding_info where car=?");
                        ps.setString(1, value.f1);
                        ResultSet rs = ps.executeQuery();
                        int nowspeed = value.f2;
                        int limit_speed = 0;
                        if (rs.next()) {
                            limit_speed = rs.getInt("limit_speed");
                        }

                        return nowspeed > limit_speed * 1.2;
                    }
//                        conn.close();
//                        ps.close();
//                        rs.close();

                }).times(3).consecutive().within(Time.minutes(2));//consecutive连续

        //3.Pattern应用在事件流上检测（模式匹配）
        PatternStream<Tuple3<Long, String, Integer>> patternStream = CEP.pattern(ds1.keyBy(v -> v.f1), pattern);

        //4.选取结果
        patternStream.select(new PatternSelectFunction<Tuple3<Long, String, Integer>, Tuple3<Long,String,String>>() {
            @Override
            public Tuple3<Long,String,String> select(Map<String, List<Tuple3<Long, String, Integer>>> map) throws Exception {
                Long time = map.get("first").get(0).f0;
                String car = map.get("first").get(0).f1;
                String danger = "涉嫌危险驾驶";
                return Tuple3.of(time,car,danger);
            }
        }) .addSink(     //将超速车辆信息写入到关系型数据库超速表中
                JdbcSink.sink(
                        "insert into t_violation_list (id,car,violation,create_time) values (null, ?, ?, ?)",
                        (PreparedStatement statement, Tuple3<Long,String,String> value) -> {
                            statement.setString(1,value.f1 );
                            statement.setString(2, value.f2);
                            statement.setLong(3, value.f0);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(200)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://hadoop10:3306/car?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                ));


        env.execute();
    }

}

