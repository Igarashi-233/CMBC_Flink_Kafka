package com.sensorsdata.analytics.bean.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<byte[], byte[]>> {
    @Override
    //nextElement 是否表示流的最后一条元素，我们要设置为 false ,因为我们需要 msg 源源不断的被消费
    public boolean isEndOfStream(ConsumerRecord<byte[], byte[]> nextElement) {
        return false;
    }

    @Override
    // 反序列化 kafka 的 record，我们直接将 record 返回
    public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> record) {
        return record;
    }

    @Override
    //告诉 Flink 我输入的数据类型是 ConsumerRecord<byte[], byte[]> , 方便 Flink 的类型推断
    public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
        Class<ConsumerRecord<byte[], byte[]>> type = (Class<ConsumerRecord<byte[], byte[]>>) (Class<?>) ConsumerRecord.class;
        return TypeInformation.of(type);
    }
}