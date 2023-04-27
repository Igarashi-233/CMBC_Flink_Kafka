package com.sensorsdata.analytics.connectors.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import com.sensorsdata.analytics.operator.sink.OffsetWriter;
import com.sensorsdata.analytics.transformation.converter.ConverterFamily;
import com.sensorsdata.analytics.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * flink消费kafka和生产数据到kafka的连接器
 * @author igarashi233
 **/
@Slf4j
public class KafkaGenerateFactory {

    KafkaGenerateFactory() {
    }

    public static <T> DataStream<JsonNode> createKafkaDataStream(StreamExecutionEnvironment env,
                                                                 ParameterTool params,
                                                                 Class<? extends KafkaDeserializationSchema<T>> clazz,
                                                                 Map<Tuple3<String, String, String>,
                                                                         Map<String, Map<Integer, Long>>>
                                                                         serverTopicPartitionOffset)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        DataStream<JsonNode> result = null;

        for (Map.Entry<Tuple3<String, String, String>, Map<String, Map<Integer, Long>>> entry : serverTopicPartitionOffset.entrySet()) {
            Tuple3<String, String, String> kafkaTuple = entry.getKey();
            Map<String, Map<Integer, Long>> topicIsFromSpecPartitionOffset = entry.getValue();
            List<String> topics = new ArrayList<>(topicIsFromSpecPartitionOffset.keySet());

            Properties properties = new Properties();
            //消费语义
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, params.getRequired(PropertiesConstants.AUTO_OFFSET_RESET));
            //默认最大间隔为 500ms
            properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                    StringUtils.defaultIfEmpty(params.get(PropertiesConstants.FETCH_MAX_WAIT_MS_CONFIG), "500"));
            // 默认 5*1024*1024
            properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                    StringUtils.defaultIfEmpty(params.get(PropertiesConstants.FETCH_MAX_BYTES_CONFIG), 5 * 1024 * 1024 + ""));
            properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                    StringUtils.defaultIfEmpty(params.get(PropertiesConstants.FETCH_MIN_BYTES_CONFIG), "1"));
            //请求超时时间
            properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                    StringUtils.defaultIfEmpty(params.get(PropertiesConstants.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG), "30000"));

            // 消费者认证
            final String securityProtocol = params.get(PropertiesConstants.CONSUMER_SECURITY_PROTOCOL, "");
            if (!"".equals(securityProtocol)) {
                properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
                if (securityProtocol.contains("SASL")) {
                    properties.setProperty(SaslConfigs.SASL_MECHANISM, params.getRequired(PropertiesConstants.CONSUMER_SASL_MECHANISM));
                }
            }
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTuple.f0);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaTuple.f1);
            // 消费者认证
            if (securityProtocol.contains("SASL")) {
                properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, kafkaTuple.f2);
            }

//            log.info("kafka consumer properties: {}", properties);
            Class[] classes = new Class[]{};
            KafkaDeserializationSchema<T> kafkaSerializationSchema = clazz.getConstructor(classes).newInstance();
            FlinkKafkaConsumer<T> kafkaConsumer =
                    new FlinkKafkaConsumer<>(topics, kafkaSerializationSchema, properties);
            //指定从指定的partition的指定的offset消费数据
            Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
            topicIsFromSpecPartitionOffset.forEach((topic, partitionOffsetMap) -> {
                if (null == partitionOffsetMap)
                    return;
                for (Map.Entry<Integer, Long> integerLongEntry : partitionOffsetMap.entrySet()) {
                    if (null == integerLongEntry)
                        continue;
                    Integer partition = integerLongEntry.getKey();
                    Long offset = integerLongEntry.getValue();
                    KafkaTopicPartition kafkaTopicPartition = new KafkaTopicPartition(topic, partition);
                    specificStartupOffsets.put(kafkaTopicPartition, offset);
                }

                log.info("start consumer from spec partition-offset {}", partitionOffsetMap);
            });
            if (!specificStartupOffsets.isEmpty())
                kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);

            DataStream<ConsumerRecord<byte[], byte[]>> kafkaSource =
                    (DataStream<ConsumerRecord<byte[], byte[]>>) env.addSource(kafkaConsumer)
//                            .setParallelism(params.getInt("stream.source.parallelism"))
                            .name(kafkaTuple.f0 + "-source")
                            .rebalance();

            kafkaSource.addSink(new OffsetWriter(kafkaTuple, params))
                    .setParallelism(1)
                    .name(kafkaTuple.f0 + "-offset");

            DataStream<JsonNode> jsonNodeDataStream = kafkaSource.map(new RichMapFunction<ConsumerRecord<byte[], byte[]>, JsonNode>() {
                @Override
                public JsonNode map(ConsumerRecord<byte[], byte[]> consumerRecord) throws IOException {
                    if (null == consumerRecord) {
                        log.info("current record is null");
                        return JsonUtil.objectMapper().createObjectNode()
                                .put(PropertiesConstants.RECORD_SERVER, kafkaTuple.f0)
                                .put(PropertiesConstants.RECORD_GROUP, kafkaTuple.f1)
                                .put(PropertiesConstants.ERROR_REASON, "Null Record");
                    }
                    try {
                        JsonNode fullRecord = JsonUtil.getBean(new String(consumerRecord.value()));

                        if (fullRecord.isMissingNode()) {
                            // find matching Converter from ConverterFamily
                            String methodKey = "converter." +
                                    kafkaTuple.f0.replace(':', '.') +
                                    "." +
                                    consumerRecord.topic();
                            fullRecord = ConverterFamily.CONVERTER_MAP
                                    .getOrDefault(params.get(methodKey), ConverterFamily.emptyConverter)
                                    .process(consumerRecord);
                        }
                        //add partition offset
                        ((ObjectNode) fullRecord).put(PropertiesConstants.RECORD_SERVER, kafkaTuple.f0);
                        ((ObjectNode) fullRecord).put(PropertiesConstants.RECORD_GROUP, kafkaTuple.f1);
                        ((ObjectNode) fullRecord).put(PropertiesConstants.RECORD_TOPIC, consumerRecord.topic());
                        ((ObjectNode) fullRecord).put(PropertiesConstants.RECORD_PARTITION, consumerRecord.partition());
                        ((ObjectNode) fullRecord).put(PropertiesConstants.RECORD_OFFSET, consumerRecord.offset());
                        try {
                            String recordKey = new String(consumerRecord.key());
                            ((ObjectNode) fullRecord).put(PropertiesConstants.RECORD_KEY, recordKey);
                        } catch (Exception e) {
                            ((ObjectNode) fullRecord).put(PropertiesConstants.RECORD_KEY, "");
//                            log.debug("current record kafka key is null");
                        }
                        return fullRecord;
                    } catch (Exception e) {
                        log.error("Record with Error: ", e);
                        return JsonUtil.objectMapper().createObjectNode()
                                .put(PropertiesConstants.RECORD_SERVER, kafkaTuple.f0)
                                .put(PropertiesConstants.RECORD_GROUP, kafkaTuple.f1)
                                .put(PropertiesConstants.RECORD_TOPIC, consumerRecord.topic())
                                .put(PropertiesConstants.RECORD_PARTITION, consumerRecord.partition())
                                .put(PropertiesConstants.RECORD_OFFSET, consumerRecord.offset())
                                .put(PropertiesConstants.ERROR_REASON, "Record handle exception: " + e.getMessage());
                    }
                }
            })
//                    .setParallelism(params.getInt("stream.source.parallelism"))
                    .name(kafkaTuple.f0 + "-map");

            result = null == result ? jsonNodeDataStream : result.union(jsonNodeDataStream);
        }

        return result;
    }


    public static <T> FlinkKafkaProducer<T> toKafkaDataStream(ParameterTool params,
                                                              Class<? extends KafkaSerializationSchema<T>> clazz,
                                                              String topic,
                                                              String kafkaProducerRetries,
                                                              String kafkaProducerAck)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                params.getRequired(PropertiesConstants.PRODUCER_KAFKA_BROKERS));
        // producer auth
        String securityProtocol;
        if (params.has(PropertiesConstants.PRODUCER_SECURITY_PROTOCOL)) {
            securityProtocol = params.getRequired(PropertiesConstants.PRODUCER_SECURITY_PROTOCOL);
            properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            if (securityProtocol.contains("SASL")) {
                properties.setProperty(SaslConfigs.SASL_MECHANISM,
                        params.getRequired(PropertiesConstants.PRODUCER_SASL_MECHANISM));
                properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                        params.getRequired(PropertiesConstants.PRODUCER_SASL_JAAS_CONFIG));
            }
        }
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                params.get(PropertiesConstants.TRANSACTION_TIMEOUT));

        //producer reties from CaseConf
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,
                StringUtils.defaultIfEmpty(kafkaProducerRetries, "0"));
        //acks = 0 1 -1 from CaseConf
        properties.setProperty(ProducerConfig.ACKS_CONFIG,
                StringUtils.defaultIfEmpty(kafkaProducerAck, "1"));
        //consumer batch size
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,
                StringUtils.defaultIfEmpty(params.get(PropertiesConstants.BATCH_SIZE_CONFIG), "16384"));
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG,
                StringUtils.defaultIfEmpty(params.get(PropertiesConstants.MAX_BLOCK_MS_CONFIG), Long.MAX_VALUE + ""));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,
                StringUtils.defaultIfEmpty(params.get(PropertiesConstants.LINGER_MS_CONFIG), Long.MAX_VALUE + ""));
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,
                StringUtils.defaultIfEmpty(params.get(PropertiesConstants.BUFFER_MEMORY_CONFIG), "33554432"));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                StringUtils.defaultIfEmpty(params.get(PropertiesConstants.PRODUCER_REQUEST_TIMEOUT_MS_CONFIG), "30000"));
        //3M 请求的最大大小为字节。要小于 message.max.bytes
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "3145728");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                StringUtils.defaultIfEmpty(params.get(PropertiesConstants.COMPRESSION_TYPE_CONFIG), "none"));
        // 设置幂等性
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                params.get(PropertiesConstants.ENABLE_IDEMPOTENCE_CONFIG).equals("true") ? "true" : "false");

//        log.info("kafka producer properties: {}", properties);
        Class[] classes = new Class[]{ParameterTool.class, String.class};
        KafkaSerializationSchema<T> kafkaSerializationSchema = clazz.getConstructor(classes).newInstance(params, topic);
        // 指定连接 kafka 连接池 大小
        return new FlinkKafkaProducer<>(
                topic, //指定topic
                kafkaSerializationSchema, //指定写入Kafka的序列化Schema
                properties, //指定Kafka的相关参数
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }
}