package RichFunctionAPI;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import apps.MySensor;
import apps.SensorReading;
import scala.Tuple3;

public class RichFlatMapFunctions {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> sensorStreamSource = env.addSource(new MySensor());

    }

    public static class TempIncreaseWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        private Double threshold;

        TempIncreaseWarning(Double threshold) {
            this.threshold = threshold;
        }

        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();

            lastTempState.update(value.getSensorTemp());

            if (lastTemp != Double.MIN_VALUE) {
                // 跟最新的温度值计算差值，如果大于阈值，那么输出报警
                Double diff = Math.abs(value.getSensorTemp() - lastTemp);
                if (diff > threshold) {
                    out.collect(new Tuple3<>(value.getSensorId(), lastTemp, value.getSensorTemp()));
                }
            }
        }
    }
}
