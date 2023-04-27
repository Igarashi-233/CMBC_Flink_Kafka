package com.sensorsdata.analytics.transformation.converter;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

@FunctionalInterface
public interface KafkaJsonConverter {
    /**
     * 用于处理不符合 Json 格式的每一条 Kafka 记录
     * 返回 JsonNode 可存单条亦可存复数条结果数据，但需要在后续处理中解开成单条进行处理
     *
     * @param consumerRecord 被处理的记录
     */
    JsonNode process(ConsumerRecord<byte[], byte[]> consumerRecord) throws IOException;
}