package com.sensorsdata.analytics.utils;

import com.sensorsdata.analytics.bean.mysql.CounterBean;
import com.sensorsdata.analytics.bean.mysql.PropertyMappingBean;
import com.sensorsdata.analytics.bean.mysql.SinkProjectBean;
import com.sensorsdata.analytics.bean.mysql.SourceSinkBean;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author igarashi233
 **/
@Slf4j
public class MetaDataUtil implements Serializable {

    public static Map<String, Object> loadAllMetaData(ParameterTool parameters) {
        //强制重新加载map
        Map<String, Object> data = new HashMap<>();

        //查询 Kafka 源信息
        String queryProjectTopicSql = parameters.get("query.project.topic.sql");
        List<SourceSinkBean> projectTopicMapping = DBTools.queryProjectTopicMapping(queryProjectTopicSql, parameters);
        data.put(PropertiesConstants.TOPIC_PROJECT_MAP, projectTopicMapping);

        //查询目标项目信息
        String querySinkProjectSql = parameters.get("query.sink.project.sql");
        List<SinkProjectBean> sinkProjectMapping = DBTools.querySinkProjectMapping(querySinkProjectSql, parameters);
        data.put(PropertiesConstants.SINK_PROJECT_MAP, sinkProjectMapping);

        //查询计数统计记录
        String queryCountSql = parameters.get("query.counter.sql");
        List<CounterBean> counterBeanList = DBTools.queryCounterBeanList(queryCountSql, parameters);
        data.put(PropertiesConstants.COUNTER, counterBeanList);

        //查询事件映射表的相关映射属性
        String queryPropertyMappingDBSql = parameters.get("query.property.mapping.db.sql");
        List<PropertyMappingBean> queryPropertyMapping =
                DBTools.queryPropertyMapping(queryPropertyMappingDBSql, parameters);
        data.put(PropertiesConstants.PROPERTY_MAP, queryPropertyMapping);

        return data;
    }

    public static Map<String, Tuple2<Integer, String>> getMappingByProject(Map<String, Object> cacheMap,
                                                                           String projectId) {

        List<PropertyMappingBean> propertyMappingList =
                (List<PropertyMappingBean>) cacheMap.getOrDefault(PropertiesConstants.PROPERTY_MAP, Collections.EMPTY_LIST);
        Map<String, Tuple2<Integer, String>> mappings = new HashMap<>();
        for (PropertyMappingBean mappingBean : propertyMappingList) {
            if (projectId.contains(mappingBean.getProjectId()))
                mappings.put(mappingBean.getPropertyName(),
                        Tuple2.of(mappingBean.getPropertyType(), mappingBean.getPropertyNameSa()));
        }
        return mappings;
    }
}