package com.sensorsdata.analytics.bean.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.sensorsdata.analytics.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
public class InvalidEventDataSchema
        implements KafkaDeserializationSchema<ConsumerRecord<byte[], byte[]>>, KafkaSerializationSchema<JsonNode> {
    ParameterTool parameterTool;
    String topic;

    public InvalidEventDataSchema() {
    }

    //构造方法传入要写入的topic和字符集，默认使用UTF-8
    public InvalidEventDataSchema(ParameterTool parameterTool, String topic) {
        this.parameterTool = parameterTool;
        this.topic = topic;
    }

    @Override
    public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        return consumerRecord;
    }

    @Override
    public boolean isEndOfStream(ConsumerRecord<byte[], byte[]> consumerRecord) {
        return false;
    }

    @Override
    public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
        Class<ConsumerRecord<byte[], byte[]>> type = (Class<ConsumerRecord<byte[], byte[]>>) (Class<?>) ConsumerRecord.class;
        return TypeInformation.of(type);
    }

    /**
     * 写出数据封装
     *
     * @param record
     * @param aLong
     * @return
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(JsonNode record, @Nullable Long aLong) {

        byte[] value = Objects.requireNonNull(JsonUtil.toJson(record)).getBytes(StandardCharsets.UTF_8);
        //返回ProducerRecord
        return new ProducerRecord<>(topic, value);
    }
}
