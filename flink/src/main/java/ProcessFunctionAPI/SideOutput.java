package ProcessFunctionAPI;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import apps.MySensor;
import apps.SensorReading;

public class SideOutput {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> sensorStreamSource = env.addSource(new MySensor());

        final OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {
        };

        SingleOutputStreamOperator<SensorReading> highTempStream = sensorStreamSource.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getSensorTemp() < 30) {
                    ctx.output(lowTempTag, value);
                } else {
                    out.collect(value);
                }
            }
        });

        DataStream<SensorReading> lowTempStream = highTempStream.getSideOutput(lowTempTag);

        highTempStream.print("high");
        lowTempStream.print("low");

        env.execute();
    }
}