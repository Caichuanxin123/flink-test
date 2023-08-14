package day8;

import bean.MonitorInfo;
import com.alibaba.fastjson.JSON;

public class Test2_JSON {


    public static void main(String[] args) {
        //1686647524,0005,1,豫A99997,60,01,20
        //{"actionTime":1686647524,"monitorId":"0005","cameraId":"1","car":"豫A99997","speed":60,"roadId":"01","areaId":"20"}
        /**
         * json  {"road_id":"111"}
         * java   private String roadId;
         */
        String s1 =
                "{\"actionTime\":1686647524,\"monitor_id\":\"0005\",\"cameraId\":\"1\",\"car\":\"豫A99997\",\"speed\":60,\"roadId\":\"01\",\"areaId\":\"20\"}";
        MonitorInfo monitorInfo = JSON.parseObject(s1, MonitorInfo.class);
        System.out.println(monitorInfo);
    }

}
