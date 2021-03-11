package StateBackend;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FsStateBackends {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

// 1. 状态后端配置
        env.setStateBackend(new FsStateBackend(""));

// 2. 检查点配置
        env.enableCheckpointing(1000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);


// 3. 重启策略配置

// 固定延迟重启（隔一段时间尝试重启一次）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3,  // 尝试重启次数
            100000 // 尝试重启的时间间隔，也可org.apache.flink.api.common.time.Time
        ));
    }
}
