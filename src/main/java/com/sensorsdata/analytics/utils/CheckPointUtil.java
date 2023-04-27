package com.sensorsdata.analytics.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static com.sensorsdata.analytics.constants.PropertiesConstants.*;

/**
 * @author zhangshk_
 * @since 2020-05-25
 **/
@Slf4j
public class CheckPointUtil {

    CheckPointUtil() {
    }

    public static StreamExecutionEnvironment
    setCheckpointConfig(StreamExecutionEnvironment env, ParameterTool parameterTool, boolean failOnCPErrors)
            throws URISyntaxException, IOException {
        log.info("State backend configuration . [ \n" +
                        "STREAM_CHECKPOINT_ENABLE = {} ,\n" +
                        "STREAM_CHECKPOINT_TYPE = {} ,\n" +
                        "STREAM_CHECKPOINT_DIR = {} ,\n" +
                        "STREAM_CHECKPOINT_INTERVAL ={} , \n" +
                        "]", parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false)
                , parameterTool.get(STREAM_CHECKPOINT_TYPE, CHECKPOINT_MEMORY)
                , parameterTool.get(STREAM_CHECKPOINT_DIR)
                , parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));
        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false)
                && CHECKPOINT_MEMORY.equalsIgnoreCase(parameterTool.get(STREAM_CHECKPOINT_TYPE, CHECKPOINT_MEMORY))) {
            //1、state 存放在内存中，默认是 5M
            StateBackend stateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 100);
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));
            env.setStateBackend(stateBackend);
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false)
                && CHECKPOINT_FS.equalsIgnoreCase(parameterTool.get(STREAM_CHECKPOINT_TYPE, CHECKPOINT_MEMORY))) {
            //fs
            StateBackend stateBackend = new FsStateBackend(new URI(parameterTool.get(STREAM_CHECKPOINT_DIR)), 0);
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));
            env.setStateBackend(stateBackend);
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false)
                && CHECKPOINT_ROCKETSDB.equalsIgnoreCase(parameterTool.get(STREAM_CHECKPOINT_TYPE, CHECKPOINT_MEMORY))) {
            //rocksdb
            StateBackend rocksDBStateBackend = new RocksDBStateBackend(parameterTool.get(STREAM_CHECKPOINT_DIR),
                    parameterTool.get(ENABLE_INCREMENTAL_CHECKPOINTING).equalsIgnoreCase("true"));
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));
            env.setStateBackend(rocksDBStateBackend);
        }
        //设置 checkpoint 周期时间
        env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 600));
        // 设置 exactly-once 模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Sets the minimal pause between checkpointing attempts
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(parameterTool.getLong(CHECKPOINT_PAUSE_BETWEEN_CHECKPOINTS, 500));
        // Sets the maximum time that a checkpoint may take before being discarded
        env.getCheckpointConfig().setCheckpointTimeout(parameterTool.getLong(CHECKPOINT_TIME_OUT, 60000));
        // Sets the maximum number of checkpoint attempts that may be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(parameterTool.getInt(CHECKPOINT_MAX_CONCURRENT, 1));
        //Enables checkpoints to be persisted externally ， when the owning job fails or is suspended
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // Set whether need to throw exception and fail the task when cp failed
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(failOnCPErrors ? 0 : Integer.MAX_VALUE);

        env.getConfig().enableObjectReuse();

        return env;
    }
}
