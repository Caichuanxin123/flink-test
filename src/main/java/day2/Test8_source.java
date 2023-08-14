package day2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
 * 要求:
 * - 随机生成订单ID(UUID)
 * - 随机生成用户ID(0-2)
 * - 随机生成订单金额(0-100)
 * - 时间戳为当前系统时间
 */
public class Test8_source {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Order> ds1 = env.addSource(new SourceFunction<Order>() {
            @Override
            public void run(SourceContext<Order> out) throws Exception {
                Random random = new Random();
                while (true){
                    Order order = new Order();
                    order.setOrderId(UUID.randomUUID().toString());
                    order.setUserId(random.nextInt(3));
                    order.setMoney(random.nextInt(101));
                    order.setTs(System.currentTimeMillis());
                    out.collect(order);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        ds1.print();

        env.execute();
    }

}
