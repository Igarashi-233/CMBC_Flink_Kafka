package com.sensorsdata.analytics.transformation.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sensorsdata.analytics.utils.JsonUtil;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConverterFamily {

    public static final Map<String, KafkaJsonConverter> CONVERTER_MAP = new HashMap<>();

    public static final KafkaJsonConverter emptyConverter = consumerRecord -> JsonUtil.objectMapper().createObjectNode();

    public static final KafkaJsonConverter csvConverter = consumerRecord -> {
        String fullRecord = new String(consumerRecord.value());
        String[] fields = fullRecord.split("\u0001");
        InputStream propertyStream = ConverterFamily.class.getResourceAsStream("csv_converter.properties");
        Properties properties = new Properties();
        properties.load(propertyStream);

        ObjectNode resultNode = JsonUtil.objectMapper().createObjectNode();
        for (String propertyName : properties.stringPropertyNames()) {
            int index = Integer.parseInt(propertyName);
            if (index < fields.length)
                resultNode.put(properties.getProperty(propertyName), fields[index]);
        }
        return resultNode;
    };

    static {
        CONVERTER_MAP.put("csvConverter", csvConverter);
        CONVERTER_MAP.put("emptyConverter", emptyConverter);
    }

    private ConverterFamily() {
    }

}
