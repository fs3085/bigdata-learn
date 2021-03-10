package apps;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class MySensor implements SourceFunction<SensorReading> {

    public boolean running = true;

    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random random = new Random();
        HashMap<String, Double> sensorTempMap = new HashMap<String, Double>();
        for(int i=0; i<10; i++){
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }
        while (running) {

            for(String sensorId: sensorTempMap.keySet()){
                Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                sensorTempMap.put(sensorId, newTemp);
                ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
            }

            Thread.sleep(1000L);
        }
    }

    public void cancel() {
        this.running = false;
    }
}
