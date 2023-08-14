package day4;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test5_Date {
    public static void main(String[] args) throws ParseException {
        //JDK自带的日期对象和字符串之间的转换
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String s = sdf.format(new Date(1686639970000L));
        System.out.println(s);

        Date date2 = sdf.parse("2023-06-13 15:06:10");
        System.out.println(date2.getTime());


        Date date3 = DateUtils.parseDate("2023-06-13 15:06:10", "yyyy-MM-dd HH:mm:ss");
        System.out.println(date3.getTime());

        String s2 = DateFormatUtils.format(1686639970000L, "yyyy-MM-dd HH:mm:ss");
        System.out.println(s2);

    }
}
