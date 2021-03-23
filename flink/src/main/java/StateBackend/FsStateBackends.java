package StateBackend;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * mangered state flink机制管理 和 raw state 自己底层原理管理
 *状态始终与特定算子相关联
 *
 * 两种类型的状态： 1.算子状态 2.键控状态 key
 *
 * Flink为算子状态提供三种基本数据结构：
 * 列表状态（List state）
 * 将状态表示为一组数据的列表。
 * 联合列表状态（Union list state）
 * 也将状态表示为数据的列表。它与常规列表状态的区别在于，在发生故障时，或者从保存点（savepoint）启动应用程序时如何恢复。
 * 广播状态（Broadcast state）
 * 如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特殊情况最适合应用广播状态。
 *
 *
 * Flink的Keyed State支持以下数据类型：
 * ValueState<T>保存单个的值，值的类型为T。
 * oget操作: ValueState.value()
 * oset操作: ValueState.update(T value)
 * ListState<T>保存一个列表，列表里的元素的数据类型为T。基本操作如下：
 * oListState.add(T value)
 * oListState.addAll(List<T> values)
 * oListState.get()返回Iterable<T>
 * oListState.update(List<T> values)
 * MapState<K, V>保存Key-Value对。
 * oMapState.get(UK key)
 * oMapState.put(UK key, UV value)
 * oMapState.contains(UK key)
 * oMapState.remove(UK key)
 * ReducingState<T>
 * AggregatingState<I, O>
 * State.clear()是清空操作。
 *
 * **/

/**
 * MemoryStateBackend
 * 内存级的状态后端，会将键控状态作为内存中的对象进行管理，将它们存储在TaskManager的JVM堆上；而将checkpoint存储在JobManager的内存中。
 * FsStateBackend
 * 将checkpoint存到远程的持久化文件系统（FileSystem）上。而对于本地状态，跟MemoryStateBackend一样，也会存在TaskManager的JVM堆上。
 * RocksDBStateBackend
 * 将所有状态序列化后，存入本地的RocksDB中存储。
 * **/

public class FsStateBackends {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

// 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend(""));

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
