package day2;

class A{

    public void m1(T1 t1){
        System.out.println("1111");
        //调用接口中的方法
        t1.m2("");
        System.out.println("2222");
    }

}

@FunctionalInterface  //函数式接口
interface T1{

    public void m2(String s1);

}

public class Test4_lambda {
    public static void main(String[] args) {
        A a = new A();
       /* a.m1(new T1() {
            @Override
            public void m2() {
                System.out.println("333");
            }
        });*/

        a.m1(
            s1 -> {
                System.out.println("333");
            }
        );
    }
}
