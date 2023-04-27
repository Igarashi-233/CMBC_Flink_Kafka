package com.sensorsdata.analytics.transformation.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.sensorsdata.analytics.bean.format.SourceBaseEntity;

import java.util.List;

@FunctionalInterface
public interface JsonProcessor {

    /**
     * 用于处理各类自定义 Json 格式的每一条记录
     * 返回 SourceBaseEntity 可存单条亦可存复数条结果数据，但需要在后续处理中解开成单条进行处理
     *
     * @param fullRecord 被处理的记录
     */
    List<SourceBaseEntity> process(JsonNode fullRecord);

}
