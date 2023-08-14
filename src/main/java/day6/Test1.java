package day6;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test1 {

    public static void main(String[] args) throws ParseException {
        String s1 = "2023-10-05";
        String s2 = "2023-09-26";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = sdf.parse(s1);
        Date d2 = sdf.parse(s2);

        System.out.println((d1.getTime() - d2.getTime())/1000/60/60/24);
    }

}
