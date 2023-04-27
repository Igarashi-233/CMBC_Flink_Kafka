package com.sensorsdata.analytics.transformation.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.sensorsdata.analytics.bean.format.SourceBaseEntity;
import com.sensorsdata.analytics.bean.format.SourceEventBean;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import com.sensorsdata.analytics.utils.JsonUtil;
import com.sensorsdata.analytics.utils.TransUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessorFamily {

    public static final Map<String, JsonProcessor> JSON_PROCESSOR_MAP = new HashMap<>();
    public static final Map<String, PostProcessor> POST_PROCESSOR_MAP = new HashMap<>();

    public static final JsonProcessor corpBankOldEvent = fullRecord -> {
        List<SourceBaseEntity> resultList = new ArrayList<>();
        JsonNode dataRecord = MissingNode.getInstance();
        if (fullRecord.has("LOG")) {
            String fullString = fullRecord.get("LOG").asText();
            dataRecord = JsonUtil.getBean(fullString.substring(fullString.indexOf('[')));
        }
        if (dataRecord.isArray()) {
            dataRecord.elements().forEachRemaining(singleRecord -> {
                        SourceEventBean sourceEventBean = new SourceEventBean();
                        sourceEventBean.setAppVersionFieldName("AppVersion");
                        sourceEventBean.setDeviceIdFieldName("DeviceID");
                        sourceEventBean.setEventNameFieldName("LogType");
                        sourceEventBean.setEventTimeFieldName("Time");
                        sourceEventBean.setLoginIdFieldName("CustID");
                        resultList.add(TransUtil.sourceRowToEventBean(singleRecord, sourceEventBean));
                    }
            );
        } else if (!dataRecord.isMissingNode()) {
            SourceEventBean sourceEventBean = new SourceEventBean();
            sourceEventBean.setAppVersionFieldName("AppVersion");
            sourceEventBean.setDeviceIdFieldName("DeviceID");
            sourceEventBean.setEventNameFieldName("LogType");
            sourceEventBean.setEventTimeFieldName("Time");
            sourceEventBean.setLoginIdFieldName("CustID");
            resultList.add(TransUtil.sourceRowToEventBean(fullRecord, sourceEventBean));
        }
        return resultList;
    };

    public static final JsonProcessor corpBankNewEvent = fullRecord -> {
        List<SourceBaseEntity> resultList = new ArrayList<>();
        if (fullRecord.isArray()) {
            fullRecord.elements().forEachRemaining(singleRecord -> {
                        SourceEventBean sourceEventBean = new SourceEventBean();
                        sourceEventBean.setAppVersionFieldName("AppVersion");
                        sourceEventBean.setDeviceIdFieldName("DeviceID");
                        sourceEventBean.setEventNameFieldName("LogType");
                        sourceEventBean.setEventTimeFieldName("Time");
                        sourceEventBean.setLoginIdFieldName("CustID");
                        resultList.add(TransUtil.sourceRowToEventBean(singleRecord, sourceEventBean));
                    }
            );
        } else if (!fullRecord.isMissingNode()) {
            SourceEventBean sourceEventBean = new SourceEventBean();
            sourceEventBean.setAppVersionFieldName("AppVersion");
            sourceEventBean.setDeviceIdFieldName("DeviceID");
            sourceEventBean.setEventNameFieldName("LogType");
            sourceEventBean.setEventTimeFieldName("Time");
            sourceEventBean.setLoginIdFieldName("CustID");
            resultList.add(TransUtil.sourceRowToEventBean(fullRecord, sourceEventBean));
        }
        return resultList;
    };

    public static final JsonProcessor uniLogPlatformEvent = fullRecord -> {
        List<SourceBaseEntity> resultList = new ArrayList<>();
        fullRecord.has("SystemID");
        if (fullRecord.isArray()) {
            fullRecord.elements().forEachRemaining(singleRecord -> {
                        SourceEventBean sourceEventBean = new SourceEventBean();
                        sourceEventBean.setAppVersionFieldName("AppVersion");
                        sourceEventBean.setDeviceIdFieldName("DeviceID");
                        sourceEventBean.setEventNameFieldName("EventID");
                        sourceEventBean.setEventTimeFieldName("Time");
                        sourceEventBean.setLoginIdFieldName("LoginID");
                        sourceEventBean.setProjectIdFieldName(PropertiesConstants.PROJECT);
                        resultList.add(TransUtil.sourceRowToEventBean(singleRecord, sourceEventBean));
                    }
            );
        } else if (!fullRecord.isMissingNode()) {
            SourceEventBean sourceEventBean = new SourceEventBean();
            sourceEventBean.setAppVersionFieldName("AppVersion");
            sourceEventBean.setDeviceIdFieldName("DeviceID");
            sourceEventBean.setEventNameFieldName("EventID");
            sourceEventBean.setEventTimeFieldName("Time");
            sourceEventBean.setLoginIdFieldName("LoginID");
            sourceEventBean.setProjectIdFieldName(PropertiesConstants.PROJECT);
            resultList.add(TransUtil.sourceRowToEventBean(fullRecord, sourceEventBean));
        }
        return resultList;
    };

    public static final PostProcessor uniLogPlatformPost =
            sensorsEvent -> ("Login".equals(sensorsEvent.getEvent()) || "register".equals(sensorsEvent.getEvent()))
                    && StringUtils.isNotBlank(sensorsEvent.getProperties().getString("device_id"));

    static {
        JSON_PROCESSOR_MAP.put("corpBankOldEvent", corpBankOldEvent);
        JSON_PROCESSOR_MAP.put("corpBankNewEvent", corpBankNewEvent);
        JSON_PROCESSOR_MAP.put("uniLogPlatformEvent", uniLogPlatformEvent);
        POST_PROCESSOR_MAP.put("uniLogPlatformPost", uniLogPlatformPost);
    }

    private ProcessorFamily() {
    }

}
