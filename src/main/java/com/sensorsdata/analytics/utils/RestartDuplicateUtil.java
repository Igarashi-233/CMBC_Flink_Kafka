package com.sensorsdata.analytics.utils;

import com.google.common.collect.Maps;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class RestartDuplicateUtil {

    private static final String PARTITION_ID = "partition_id";
    private static final String MAX_OFFSET = "max_offset";

    RestartDuplicateUtil() {
    }

    /**
     * 程序重启后，从指定的partition、offset开始消费
     *
     * @param parameters -
     * @return Map, Key:源topic, Value:Tuple2, f0:程序是否需要从指定的partition的offset重启, f1:指定partition offset的map
     */
    public static Map<Tuple3<String, String, String>, Map<String, Map<Integer, Long>>>
    processRestartDuplicateRemoval(ParameterTool parameters,
                                   Map<Tuple3<String, String, String>, Map<String, List<String>>> kafkaSourceSink) {

        Map<Tuple3<String, String, String>, Map<String, Map<Integer, Long>>> result = new HashMap<>();
        LongAdder sinkSum = new LongAdder();
        KafkaInfoClient kafkaInfoClient = new KafkaInfoClient(parameters);

        kafkaSourceSink.forEach((kafkaTuple, projectTopicMaps) -> {
            kafkaInfoClient.updateConsumer(kafkaTuple);
            Map<String, Map<Integer, Long>> resultPerKafka = new HashMap<>();
            Set<String> sourceTopicSet = new HashSet<>();
            for (List<String> value : projectTopicMaps.values()) {
                sourceTopicSet.addAll(value);
            }
            for (String sourceTopic : sourceTopicSet) {
                log.info("[M:processRestartDuplicateRemoval] - [sourceTopic = {}]", sourceTopic);
                System.out.println("[M:processRestartDuplicateRemoval] - [sourceTopic = {}]" + sourceTopic);
                //先从mysql获取上一次checkpoint的时候的partition、offset
                List<Map<String, Object>> latestCheckpointInfo =
                        DBTools.getLatestCheckpointInfo(parameters, sourceTopic, kafkaTuple);
                //如果修改了group从头消费，是不需要从之前的checkpoint恢复的， 直接用新的group消费就行了（生产上肯定不会这样）
                if (latestCheckpointInfo.isEmpty()) {
                    resultPerKafka.put(sourceTopic, null);
                    log.info("[M:processRestartDuplicateRemoval] - [latestCheckpointInfo.isEmpty]");
                    System.out.println("[M:processRestartDuplicateRemoval] - [latestCheckpointInfo.isEmpty]");
                    // fixme: 考虑默认向前追溯 1000 条数据，规避 MySQL 读取失败可能造成的问题
                    continue;
                }
                HashMap<Integer, Long> partitionMaxOffsetMap = Maps.newHashMap();
                for (Map<String, Object> map : latestCheckpointInfo) {
                    int partitionId = -1;
                    long maxOffset = -1;
                    if (map.containsKey(PARTITION_ID)) {
                        partitionId = Integer.parseInt(map.get(PARTITION_ID).toString());
                    }
                    if (map.containsKey(MAX_OFFSET)) {
                        maxOffset = Long.parseLong(map.get(MAX_OFFSET).toString());
                    }
                    if (partitionId != -1) {
                        partitionMaxOffsetMap.put(partitionId, maxOffset);
                    }
                }
                resultPerKafka.put(sourceTopic, partitionMaxOffsetMap);
                log.info("[M:processRestartDuplicateRemoval] - [getLatestCheckpointInfo = {}]", latestCheckpointInfo);
                log.info("[M:processRestartDuplicateRemoval] - [partitionMaxOffsetMap = {}]", partitionMaxOffsetMap);
                System.out.println("[M:processRestartDuplicateRemoval] - [getLatestCheckpointInfo = {}]" + latestCheckpointInfo);
                System.out.println("[M:processRestartDuplicateRemoval] - [partitionMaxOffsetMap = {}]" + partitionMaxOffsetMap);

                //获取当前 source topic 每个 partition 最大的 offset
                Map<Integer, Long> latestOffset = kafkaInfoClient.getLatestOffset(sourceTopic);
                log.info("[M:processRestartDuplicateRemoval] - [getLatestOffset = {}]", latestOffset);
                System.out.println("[M:processRestartDuplicateRemoval] - [getLatestOffset = {}]" + latestOffset);

                //获取的每个 partition 最大的 offset 减去 checkpoint 的每个 partition 的 offset 值为 sum
                long sum = 0;
                for (Map.Entry<Integer, Long> producerPartitionOffset : partitionMaxOffsetMap.entrySet()) {
                    Long aLong = latestOffset.get(producerPartitionOffset.getKey()) - producerPartitionOffset.getValue();
                    if (aLong > 0) {
                        sum += aLong;
                    }
                }
                log.info("[M:processRestartDuplicateRemoval] - [getRecallCount = {}]", sum);
                System.out.println("[M:processRestartDuplicateRemoval] - [getRecallCount = {}]" + sum);
                //如果sum=0，表示不需要从指定的partition-offset开始重新消费。
                //如offset无法commit，需将最新的partition-offset进行记录
                if (sum == 0) {
                    resultPerKafka.put(sourceTopic, latestOffset);
                    log.info("[M:processRestartDuplicateRemoval] - [latestCheckpointInfo.isEmpty]");
                    System.out.println("[M:processRestartDuplicateRemoval] - [latestCheckpointInfo.isEmpty]");
                    continue;
                }
                sinkSum.add(sum);
                System.out.println("[M:processRestartDuplicateRemoval] - [getTotalRecallCount = {}]" + sinkSum.longValue());
            }
            result.put(kafkaTuple, resultPerKafka);
        });

        if (sinkSum.longValue() > 0L) {
            //从 sink 端获取每个分区的 sum 条最新的数据，并获取所有数据中的 source 端设置的 partition 的最大的offset， 存入map中。
            Map<String, Map<String, Map<Integer, Long>>> sourceProjectLargestSinkOffset =
                    kafkaInfoClient.getLargestSinkOffset(parameters.getRequired(PropertiesConstants.PRODUCER_TOPIC),
                            sinkSum.longValue());
            log.info("[M:processRestartDuplicateRemoval] - [largestSinkOffset = {}]", sourceProjectLargestSinkOffset);
            System.out.println("[M:processRestartDuplicateRemoval] - [largestSinkOffset = {}]" + sourceProjectLargestSinkOffset);
            for (Tuple3<String, String, String> kafkaInfo : kafkaSourceSink.keySet()) {
                Map<String, Map<Integer, Long>> relay = result.getOrDefault(kafkaInfo, new HashMap<>());
                if (sourceProjectLargestSinkOffset.containsKey(kafkaInfo.f0)) {
                    relay.putAll(sourceProjectLargestSinkOffset.get(kafkaInfo.f0));
                }
                result.put(kafkaInfo, relay);
            }
        }
        return result;
    }
}
