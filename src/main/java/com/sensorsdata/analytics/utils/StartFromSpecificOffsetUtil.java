package com.sensorsdata.analytics.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

/**
 * @author igarashi233
 **/

@Slf4j
public class StartFromSpecificOffsetUtil {

    StartFromSpecificOffsetUtil() {
    }

    /**
     * 从指定的startTime 对应的partition、offset开始消费
     *
     * @param startTime - 输入的 Timestamp，用于定位所有 Partition 的 Offset
     * @return Map, Key:源topic, Value:Tuple2, f0:程序是否需要从指定的partition的offset重启, f1:指定partition offset的map
     */
    public static Map<Tuple3<String, String, String>, Map<String, Map<Integer, Long>>>
    processRestartDuplicateRemoval(ParameterTool parameters,
                                   long startTime,
                                   Map<Tuple3<String, String, String>, Map<String, List<String>>> kafkaSourceSink) {
        Map<Tuple3<String, String, String>, Map<String, Map<Integer, Long>>> result = new HashMap<>();
        KafkaInfoClient kafkaInfoClient = new KafkaInfoClient(parameters);
        kafkaSourceSink.forEach((kafkaTuple, sinkSourceMap) -> {
            kafkaInfoClient.updateConsumer(kafkaTuple);

            Set<String> sourceTopicSet = new HashSet<>();
            for (List<String> value : sinkSourceMap.values()) {
                sourceTopicSet.addAll(value);
            }

            Map<String, Map<Integer, Long>> resultPerKafka = new HashMap<>();
            for (String sourceTopic : sourceTopicSet) {
                Map<Integer, Long> partitionOffsetByTimeStamp =
                        kafkaInfoClient.getPartitionOffsetByTimeStamp(sourceTopic, startTime);
                resultPerKafka.put(sourceTopic, partitionOffsetByTimeStamp);
            }
            result.put(kafkaTuple, resultPerKafka);
        });

        return result;
    }
}
