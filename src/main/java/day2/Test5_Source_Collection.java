package day2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Test5_Source_Collection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds1 = env.fromElements("hello", "haha", "gaga");

        /**
         *  Arrays : java操作数组的工具类
         *         String[] arr = new String[]{"zs","lisi","www"};
         *         System.out.println(Arrays.toString(arr));
         *
         *         List<String> list = Arrays.asList("zs", "lisi", "ww");
         */
        List<String> list = new ArrayList<>();
        list.add("张三");
        list.add("李四");
        list.add("王五");
        DataStreamSource<String> ds2 = env.fromCollection(list);
        DataStreamSource<Long> ds3 = env.fromSequence(1, 10);


        ds1.print("ds1 ");
        ds2.print("ds2 ");
        ds3.print("ds3 ");


        env.execute();
    }

}
