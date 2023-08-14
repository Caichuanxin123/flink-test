package day8;

import org.apache.flink.api.java.tuple.Tuple2;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * java面试题
 * 有哪些方式可以调用一个类中的非静态方法
 * A a = new A();
 * a.m1();
 *
 * 1. 要理解
 * 2. 理解 + 记忆
 *
 */
public class Test1 {

    public static void main(String[] args) throws Exception {
        //java的泛型是编译器泛型，运行的时候就没有了
        //运行的时候擦除
        List<String> list = new ArrayList<String>();
        list.add("张三");

        //通过反射可以在代码运行时添加非字符串元素，证明了java的泛型是编译阶段泛型
        Class clz = Class.forName("java.util.ArrayList");                        //1.找到类 ArrayList jvm
        Method method = clz.getMethod("add",Object.class);                 //2.找到方法 add
        method.invoke(list, Tuple2.of("hello",1));                               //3.调用add方法  语法

        for (Object s : list) {
            System.out.println(s);
        }
    }

}
