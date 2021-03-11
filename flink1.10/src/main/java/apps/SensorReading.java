package apps;

public class SensorReading {
    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public Long getSensorTime() {
        return sensorTime;
    }

    public void setSensorTime(Long sensorTime) {
        this.sensorTime = sensorTime;
    }

    public Double getSensorTemp() {
        return sensorTemp;
    }

    public void setSensorTemp(Double sensorTemp) {
        this.sensorTemp = sensorTemp;
    }

    public SensorReading(String sensorId, Long sensorTime, Double sensorTemp) {
        this.sensorId = sensorId;
        this.sensorTime = sensorTime;
        this.sensorTemp = sensorTemp;
    }

    public SensorReading() {
    }

    private String sensorId;
    private Long sensorTime;
    private Double sensorTemp;


}
