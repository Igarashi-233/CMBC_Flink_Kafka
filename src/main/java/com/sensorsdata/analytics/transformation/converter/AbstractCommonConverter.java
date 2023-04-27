package com.sensorsdata.analytics.transformation.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.sensorsdata.analytics.bean.format.SensorsBaseAttributesBean;
import com.sensorsdata.analytics.bean.format.SourceBaseEntity;
import com.sensorsdata.analytics.bean.format.SourceEventBean;
import com.sensorsdata.analytics.constants.CommConst;
import com.sensorsdata.analytics.constants.DataTypeConst;
import com.sensorsdata.analytics.utils.JsonUtil;
import com.sensorsdata.analytics.utils.MetaDataUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author igarashi233
 **/

@Slf4j
public abstract class AbstractCommonConverter implements Converter {

    String doPropertiesMapping(SourceBaseEntity sourceBaseEntity,
                               SensorsBaseAttributesBean sensorsBaseAttributesBean,
                               String propertyType,
                               Map<String, Object> cacheMap) {
        Map<String, Tuple2<Integer, String>> mappings;
        String errorLog = "";
        try {
            mappings = MetaDataUtil.getMappingByProject(cacheMap, sourceBaseEntity.getProjectId());
        } catch (Exception e) {
            mappings = new HashMap<>();
            errorLog = "getMappingByProject failed.";
        }
        JsonNode properties = null;
        if (propertyType.equals(CommConst.PropertyType.MAIN_BODY.getType())) {
            properties = ((SourceEventBean) sourceBaseEntity).getMainObj();
        }
        if (null != properties) {
            Tuple2<String, Map<String, Object>> stringMapTuple2 = doMapping(properties, mappings);
            sensorsBaseAttributesBean.getProperties().putAll(stringMapTuple2.f1);
            if (stringMapTuple2.f0 != null || !errorLog.isEmpty())
                return errorLog + stringMapTuple2.f0;
        }
        return null;
    }

    /**
     * mapping映射
     *
     * @param jsonNode 埋点map对象
     * @param maps     元数据映射map
     * @return
     */
    private Tuple2<String, Map<String, Object>> doMapping(JsonNode jsonNode,
                                                          Map<String, Tuple2<Integer, String>> maps) {

        Map<String, Object> newMaps = Maps.newHashMap();
        String errorReason = null;
        try {
            putToMap(jsonNode, newMaps, maps);
        } catch (Exception e) {
            errorReason = e.getMessage();
            log.error("Error: ", e);
        }
        return Tuple2.of(errorReason, newMaps);
    }

    //fixme：性能损耗点，待优化
    private void putToMap(JsonNode inputNode,
                          Map<String, Object> outputMap,
                          Map<String, Tuple2<Integer, String>> mappingRules) {

        inputNode.fields().forEachRemaining(stringJsonNodeEntry -> {
            String key = stringJsonNodeEntry.getKey().toLowerCase();
            JsonNode value = stringJsonNodeEntry.getValue();
            if (mappingRules.containsKey(key)) {
                if (value.isObject())
                    log.error("Key {} is a Json Object {}, not acceptable! Please remove corresponding rule",
                            key, value.toString());
                else {
                    Object result = dataTypeVerify(value, mappingRules.get(key));
                    outputMap.put(mappingRules.get(key).f1, result);
                }
            } else if (value.isTextual() &&
                    JsonUtil.getBean(value.asText()).isObject() &&
                    !JsonUtil.getBean(value.asText()).isValueNode()) {
                JsonNode insideValue = JsonUtil.getBean(value.asText());
                putToMap(insideValue, outputMap, mappingRules);
            } else if (value.isValueNode()) {
                outputMap.put(key, value);
            } else if (value.isObject()) {
                putToMap(value, outputMap, mappingRules);
            }
        });
    }

    private Object dataTypeVerify(JsonNode value, Tuple2<Integer, String> ruleValue) {
        if (null == value) {
            log.error("value is null");
            return null;
        }
        Object result = null;
        switch (ruleValue.f0) {
            case DataTypeConst.NUMBER:
                result = value.decimalValue();
                break;
            case DataTypeConst.BOOL:
                result = value.asBoolean();
                break;
            case DataTypeConst.STRING:
                result = value.asText();
                break;
            case DataTypeConst.DATETIME:
                result = value.asText();
                break;
            case DataTypeConst.LIST:
                if (value.isArray()) {
                    AtomicBoolean valueFlag = new AtomicBoolean(true);
                    value.forEach(eachNode -> {
                        if (eachNode.isObject()) {
                            log.error("The data {} is not List, is Map", value);
                            valueFlag.set(false);
                        }
                    });
                    result = valueFlag.get() ? value : null;
                }
                break;
            default:
                log.error("type unknown. invalid rule.");
                break;
        }
        return result;
    }

}
