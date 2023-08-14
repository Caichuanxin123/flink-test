package bean;

import com.pengge.table.HTableBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/**
 * t1  cf1
 *
 * rowkey   cf1
 *          name = zhangsan
 * 值       age  = 20
 */
public class MonitorInfo implements HTableBase {  //HTableBase

    private Long actionTime;
    private String monitorId;
    private String cameraId;
    private String car;
    private Double speed;  //车辆通过卡口的实际车速
    private String roadId;
    private String areaId;

    private Integer speedLimit;  //卡口的限速


    public MonitorInfo() {
    }

    public MonitorInfo(Long actionTime, String monitorId, String cameraId, String car, Double speed, String roadId, String areaId) {
        this.actionTime = actionTime;
        this.monitorId = monitorId;
        this.cameraId = cameraId;
        this.car = car;
        this.speed = speed;
        this.roadId = roadId;
        this.areaId = areaId;
    }

    /**
     * 获取
     * @return actionTime
     */
    public Long getActionTime() {
        return actionTime;
    }

    /**
     * 设置
     * @param actionTime
     */
    public void setActionTime(Long actionTime) {
        this.actionTime = actionTime;
    }

    /**
     * 获取
     * @return monitorId
     */
    public String getMonitorId() {
        return monitorId;
    }

    /**
     * 设置
     * @param monitorId
     */
    public void setMonitorId(String monitorId) {
        this.monitorId = monitorId;
    }

    /**
     * 获取
     * @return cameraId
     */
    public String getCameraId() {
        return cameraId;
    }

    /**
     * 设置
     * @param cameraId
     */
    public void setCameraId(String cameraId) {
        this.cameraId = cameraId;
    }

    /**
     * 获取
     * @return car
     */
    public String getCar() {
        return car;
    }

    /**
     * 设置
     * @param car
     */
    public void setCar(String car) {
        this.car = car;
    }

    /**
     * 获取
     * @return speed
     */
    public Double getSpeed() {
        return speed;
    }

    /**
     * 设置
     * @param speed
     */
    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    /**
     * 获取
     * @return roadId
     */
    public String getRoadId() {
        return roadId;
    }

    /**
     * 设置
     * @param roadId
     */
    public void setRoadId(String roadId) {
        this.roadId = roadId;
    }

    /**
     * 获取
     * @return areaId
     */
    public String getAreaId() {
        return areaId;
    }

    /**
     * 设置
     * @param areaId
     */
    public void setAreaId(String areaId) {
        this.areaId = areaId;
    }

    /**
     * 获取
     * @return speedLimit
     */
    public Integer getSpeedLimit() {
        return speedLimit;
    }

    /**
     * 设置
     * @param speedLimit
     */
    public void setSpeedLimit(Integer speedLimit) {
        this.speedLimit = speedLimit;
    }

    public String toString() {
        return "MonitorInfo{actionTime = " + actionTime + ", monitorId = " + monitorId + ", cameraId = " + cameraId + ", car = " + car + ", speed = " + speed + ", roadId = " + roadId + ", areaId = " + areaId + ", speedLimit = " + speedLimit + "}";
    }

    @Override
    public byte[] rowkey() {
        return Bytes.toBytes(car+ "_" + actionTime);
    }

}
