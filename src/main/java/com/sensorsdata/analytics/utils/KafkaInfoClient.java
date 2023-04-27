package com.sensorsdata.analytics.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

@Slf4j
class KafkaInfoClient {

    private Properties properties = new Properties();
    private KafkaConsumer<String, String> sourceConsumer;
    private KafkaConsumer<String, String> sinkConsumer;
    private static final String SA_CONSUMER_SERVER_PATH = "/properties/saconsumerserver";
    private static final String SA_CONSUMER_TOPIC_PATH = "/properties/saconsumertopic";
    private static final String SA_CONSUMER_PARTITION_PATH = "/properties/saconsumerpartition";
    private static final String SA_CONSUMER_OFFSET_PATH = "/properties/saconsumeroffset";

    private String consumerSecProtocolConfig;
    private String consumerSaslMechanism;

    KafkaInfoClient(ParameterTool parameters) {
        init(parameters);
    }

    private void init(ParameterTool parameters) {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                parameters.getRequired(PropertiesConstants.PRODUCER_KAFKA_BROKERS));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                parameters.getRequired(PropertiesConstants.PRODUCER_GROUP));
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                parameters.getRequired(PropertiesConstants.PRODUCER_SECURITY_PROTOCOL));
        properties.put(SaslConfigs.SASL_MECHANISM,
                parameters.getRequired(PropertiesConstants.PRODUCER_SASL_MECHANISM));
        properties.put(SaslConfigs.SASL_JAAS_CONFIG,
                parameters.getRequired(PropertiesConstants.PRODUCER_SASL_JAAS_CONFIG));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);
        sinkConsumer = new KafkaConsumer<>(properties);

        consumerSecProtocolConfig = parameters.getRequired(PropertiesConstants.CONSUMER_SECURITY_PROTOCOL);
        consumerSaslMechanism = parameters.getRequired(PropertiesConstants.CONSUMER_SASL_MECHANISM);
    }

    void updateConsumer(Tuple3<String, String, String> kafkaInfo) {

        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaInfo.f0);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaInfo.f1);
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaInfo.f2);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, consumerSecProtocolConfig);
        properties.put(SaslConfigs.SASL_MECHANISM, consumerSaslMechanism);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        sourceConsumer = new KafkaConsumer<>(properties);
    }

    Map<Integer, Long> getLatestOffset(String topic) {
        return getLargestOffset(sourceConsumer, topic);
    }

    Map<String, Map<String, Map<Integer, Long>>> getLargestSinkOffset(String sinkTopic, long sum) {
        Map<String, Map<String, Map<Integer, Long>>> sourceProjectLargestSinkOffset = new HashMap<>();
        getSinkLargestOffset(sinkTopic, sum, sourceProjectLargestSinkOffset);
        return sourceProjectLargestSinkOffset;
    }


    /**
     * 从sink端数据内容中解析获得上游最大的partition-offset
     *
     * @param sinkTopic                 - 用于检索的目标 Topic
     * @param sum                       - source 端各个 sinkTopic 距离上一个 checkpoint 累积的总发送量
     * @param sourceProjectLatestOffset - Map<KafkaServer, Map<SourceTopic, Map<Partition, Offset>>>
     * @return Map<KafkaServer, Map < SourceTopic, Map < Partition, Offset>>>
     */
    private Map<String, Map<String, Map<Integer, Long>>>
    getSinkLargestOffset(String sinkTopic,
                         long sum,
                         Map<String, Map<String, Map<Integer, Long>>> sourceProjectLatestOffset) {

        Map<TopicPartition, Long> endOffset = getEndOffset(sinkConsumer, sinkTopic);

        for (Map.Entry<TopicPartition, Long> topicPartitionLongEntry : endOffset.entrySet()) {
            TopicPartition targetPartition = topicPartitionLongEntry.getKey();
            Long partitionOffset = topicPartitionLongEntry.getValue();
            // 指定消费分区，如果去掉该行，则不能消费到数据
            sinkConsumer.assign(Collections.singletonList(targetPartition));
            // 指定消费起始offset
            long partStartOffset = (partitionOffset - sum) > 0 ? (partitionOffset - sum) + 1 : 0;
            sinkConsumer.seek(targetPartition, partStartOffset);
            long currentPosition;
            do {
                // 真正拉取目标数据
                ConsumerRecords<String, String> records = sinkConsumer.poll(Duration.ofMillis(1000));
                System.out.println("[C:KafkaInfoClient][M:getSinkLargestOffset] - total record count [cnt = " + records.count());

                for (ConsumerRecord<String, String> record : records) {
                    // 数据处理，这里我简单用日志输出
                    JsonNode jsonNode = JsonUtil.getBean(String.valueOf(record.value()));
                    String saConsumerServer = "";
                    String saConsumerTopic = "";
                    int saConsumerPartition = -1;
                    long saConsumerOffset = -1;
                    try {
                        if (jsonNode.isArray()) {
                            for (JsonNode subNode : jsonNode) {
                                if (StringUtils.isNotBlank(subNode.at(SA_CONSUMER_PARTITION_PATH).asText())
                                        && StringUtils.isNotBlank(subNode.at(SA_CONSUMER_OFFSET_PATH).asText())
                                        && StringUtils.isNotBlank(subNode.at(SA_CONSUMER_TOPIC_PATH).asText())
                                        && StringUtils.isNotBlank(subNode.at(SA_CONSUMER_SERVER_PATH).asText())
                                ) {
                                    saConsumerServer = subNode.at(SA_CONSUMER_SERVER_PATH).asText();
                                    saConsumerTopic = subNode.at(SA_CONSUMER_TOPIC_PATH).asText();
                                    saConsumerPartition = subNode.at(SA_CONSUMER_PARTITION_PATH).asInt();
                                    saConsumerOffset = subNode.at(SA_CONSUMER_OFFSET_PATH).asLong();

                                }
                            }
                        } else {
                            saConsumerServer = jsonNode.at(SA_CONSUMER_SERVER_PATH).asText();
                            saConsumerTopic = jsonNode.at(SA_CONSUMER_TOPIC_PATH).asText();
                            saConsumerPartition = jsonNode.at(SA_CONSUMER_PARTITION_PATH).asInt();
                            saConsumerOffset = jsonNode.at(SA_CONSUMER_OFFSET_PATH).asLong();
                        }
                        Map<String, Map<Integer, Long>> sourceKafkaLatestOffset =
                                sourceProjectLatestOffset.getOrDefault(saConsumerServer, new HashMap<>());
                        Map<Integer, Long> sourceLatestOffset = sourceKafkaLatestOffset.getOrDefault(saConsumerTopic, new HashMap<>());
                        Long currentOffset = sourceLatestOffset.getOrDefault(saConsumerPartition, 0L);
                        // 判断 LatestOffset 跟新取出来的 Offset 谁更大
                        if (saConsumerOffset > currentOffset) {
                            sourceLatestOffset.put(saConsumerPartition, saConsumerOffset);
                            sourceKafkaLatestOffset.put(saConsumerTopic, sourceLatestOffset);
                            sourceProjectLatestOffset.put(saConsumerServer, sourceKafkaLatestOffset);
                        }
                    } catch (Exception e) {
                        log.error(
                                "consumer kafka data from sinkTopic {} ,and not found field saconsumerpartition or saconsumeroffset and jsonNode is {}",
                                sinkTopic, record.value());
                    }
                }
                currentPosition = sinkConsumer.position(targetPartition);
            } while (currentPosition < partitionOffset);
        }
        return sourceProjectLatestOffset;
    }

    /**
     * Get end offset
     *
     * @param kafkaConsumer - 用于执行检索的 Consumer
     * @param topic         - 需要进行检索的 Topic
     * @return 输入 topic 各个 partition 最后一条数据的 offset
     */
    private static Map<Integer, Long> getLargestOffset(KafkaConsumer<String, String> kafkaConsumer, String topic) {
        ////Get metadata about the partitions for a given topic.
        Map<Integer, Long> maps = new HashMap<>();
        for (PartitionInfo partitionInfo : (List<PartitionInfo>) kafkaConsumer.partitionsFor(topic)) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            List<TopicPartition> topicPartitions = Collections.singletonList(topicPartition);
            //Manually assign a list of partitions to this consumer
            kafkaConsumer.assign(topicPartitions);
            //Seek to the last offset for each of the given partitions
            kafkaConsumer.seekToEnd(topicPartitions);
            //Get the offset of the next record that will be fetched (if a record with that offset exists)
            long position = kafkaConsumer.position(topicPartition);
            maps.put(topicPartition.partition(), position - 1);
        }
        return maps;
    }

    /**
     * Get end offset
     *
     * @param kafkaConsumer
     * @param topic
     * @return topicPartition offset map
     */
    private static Map<TopicPartition, Long> getEndOffset(KafkaConsumer<String, String> kafkaConsumer, String topic) {
        //Get metadata about the partitions for a given topic.
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
        Map<TopicPartition, Long> maps = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfoList) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            List<TopicPartition> topicPartitions = Collections.singletonList(topicPartition);
            //Manually assign a list of partitions to this consumer
            kafkaConsumer.assign(topicPartitions);
            //Seek to the last offset for each of the given partitions
            kafkaConsumer.seekToEnd(topicPartitions);
            //Get the offset of the next record that will be fetched (if a record with that offset exists)
            long position = kafkaConsumer.position(topicPartition);
            maps.put(topicPartition, position);
        }
        return maps;
    }

    Map<Integer, Long> getPartitionOffsetByTimeStamp(String topic, long timestamp) {
        Map<Integer, Long> maps = new HashMap<>();
        HashMap<TopicPartition, Long> objectObjectHashMap = Maps.newHashMap();
        //Get metadata about the partitions for a given topic
        List<PartitionInfo> partitionInfoList = sourceConsumer.partitionsFor(topic);
        partitionInfoList.forEach(partitionInfo -> objectObjectHashMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), timestamp));
        //Look up the offsets for the given partitions by timestamp
        Map<TopicPartition, OffsetAndTimestamp> map = sourceConsumer.offsetsForTimes(objectObjectHashMap);
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> next : map.entrySet()) {
            if (null == next.getValue())
                continue;
            long timestamp1 = next.getValue().timestamp();
            long offset = next.getValue().offset();
            int partition = next.getKey().partition();
            log.info("start from spec [timestamp = {}] -- [partition = {}] -- [offset = {}] ", timestamp1, partition, offset);
            System.out.println("start from spec [timestamp = " + timestamp + "] -- [partition = " + partition + "] -- [offset = " + offset + "] ");
            maps.put(partition, offset);
        }
        return maps;
    }
}