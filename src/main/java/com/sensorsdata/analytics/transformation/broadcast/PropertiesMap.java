package com.sensorsdata.analytics.transformation.broadcast;

import com.sensorsdata.analytics.utils.MetaDataUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.util.Map;

/**
 * 广播流，每分钟执行一次,可配置<metadata.query.interval>
 *
 * @author zhangshk_
 * @since 2020-07-13
 **/

@Slf4j
public class PropertiesMap implements SourceFunction<Map<String, Object>>, Serializable {
    private ParameterTool parameters;

    public PropertiesMap(ParameterTool parameters) {
        this.parameters = parameters;
    }

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Map<String, Object>> ctx) throws Exception {

        while (isRunning) {
            Thread.sleep(parameters.getInt("metadata.query.interval"));
            Map<String, Object> map = MetaDataUtil.loadAllMetaData(parameters);
            //TODO 移除非必要广播
            ctx.collect(map);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
