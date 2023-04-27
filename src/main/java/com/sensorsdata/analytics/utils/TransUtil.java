package com.sensorsdata.analytics.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sensorsdata.analytics.bean.format.SourceEventBean;
import com.sensorsdata.analytics.bean.format.SourceUserBean;
import com.sensorsdata.analytics.bean.mysql.SinkProjectBean;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author igarashi233
 **/
@Slf4j
public class TransUtil {

    TransUtil() {
    }

    public static SourceEventBean sourceRowToEventBean(JsonNode row, SourceEventBean fieldNameEventBean) {
        String fieldName = fieldNameEventBean.getAppVersionFieldName();
        if (row.has(fieldName)) {
            fieldNameEventBean.setAppVersion(row.get(fieldName).asText());
        }
        fieldName = fieldNameEventBean.getLoginIdFieldName();
        if (row.has(fieldName)) {
            fieldNameEventBean.setLoginId(row.get(fieldName).asText());
        }
        fieldName = fieldNameEventBean.getDeviceIdFieldName();
        if (row.has(fieldName)) {
            fieldNameEventBean.setDeviceId(row.get(fieldName).asText());
        }
        fieldName = fieldNameEventBean.getEventNameFieldName();
        if (row.has(fieldName)) {
            fieldNameEventBean.setEventName(row.get(fieldName).asText());
        }
        fieldName = fieldNameEventBean.getEventTimeFieldName();
        if (row.has(fieldName)) {
            fieldNameEventBean.setEventTime(row.get(fieldName).asText());
        }
        fieldName = fieldNameEventBean.getProjectIdFieldName();
        if (row.has(fieldName)) {
            fieldNameEventBean.setProjectId(row.get(fieldName).asText());
        }
        if (!row.isValueNode()) {
            fieldNameEventBean.setMainObj(row);
        }
        return fieldNameEventBean;
    }

    public static SourceUserBean sourceRowToUserBean(JsonNode row, SourceUserBean fieldNameUserBean) {
        String fieldName = fieldNameUserBean.getEventTimeFieldName();
        if (row.has(fieldName)) {
            fieldNameUserBean.setEventTime(row.get(fieldName).asText());
        }
        fieldName = fieldNameUserBean.getUserIdFieldName();
        if (row.has(fieldName)) {
            fieldNameUserBean.setUserId(row.get(fieldName).asText());
        }
        if (!row.isValueNode()) {
            ObjectNode mainObj = row.deepCopy();
            fieldNameUserBean.setUserPropertyObj(mainObj);
        }
        return fieldNameUserBean;
    }

    public static List<JsonNode> addProjectId(ObjectNode resultNode,
                                              List<SinkProjectBean> sinkProjectList,
                                              String topic) {
        List<JsonNode> result = new ArrayList<>();
        for (SinkProjectBean sinkProjectBean : sinkProjectList) {
            if (sinkProjectBean.getSourceTopic().equals(topic))
                for (String sinkProject : sinkProjectBean.getSinkProjects().split(",")) {
                    result.add(resultNode.put("project", sinkProject));
                }
        }
        if (result.isEmpty())
            result.add(resultNode);
        return result;
    }

}
