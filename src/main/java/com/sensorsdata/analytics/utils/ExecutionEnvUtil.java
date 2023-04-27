package com.sensorsdata.analytics.utils;

import com.sensorsdata.analytics.constants.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhangshk_
 * @since 2020-05-25
 **/

public class ExecutionEnvUtil {

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Sets the parallelism for operations
//        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        //Sets the restart strategy to be used for recovery
//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 60000));
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        //Enables checkpointing for the streaming job
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 10000));
        }
        //Register a custom, serializable user configuration object
        env.getConfig().setGlobalJobParameters(parameterTool);
        return env;
    }
}