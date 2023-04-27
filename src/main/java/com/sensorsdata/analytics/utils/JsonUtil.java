package com.sensorsdata.analytics.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

/**
 * @author zhangshk_
 * @since 2020-05-25
 **/
@Slf4j
public class JsonUtil {

    private static final ObjectMapper mapper;

    static {
        //问题修复：
        // 之前问题，存在"" 数据出现，原因：NON_NULL 在 NON_EMPTY之后。
        // 解决， NON_EMPTY改为在NON_NULL之后。
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    }

    public static ObjectMapper objectMapper() {
        return mapper;
    }
    // 定义ObjectMapper对象，用于数据转换

    /**
     * getJSON:(对象转换成JSON). <br/>
     *
     * @param object
     * @return
     */
    public static String toJson(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * getBean:(JSON字符串转对象). <br/>
     *
     * @param json
     * @param t
     * @return T
     */
    static <T> T getBean(String json, Class<T> t) {
        try {
            return mapper.readValue(json, t);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static JsonNode getBean(String json) {
        try {
            JsonNode result = mapper.readTree(json);
            return null == result ? MissingNode.getInstance() : result;
        } catch (Exception e) {
//            log.error("getBean failed by {}", json);
            return MissingNode.getInstance();
        }
    }

    static com.fasterxml.jackson.databind.node.ObjectNode map2JsonNode(Map<String, Object> map) throws IOException {
        return (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(mapper.writeValueAsString(map));
    }

}