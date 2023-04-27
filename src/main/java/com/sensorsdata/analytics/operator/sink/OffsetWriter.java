package com.sensorsdata.analytics.operator.sink;

import com.sensorsdata.analytics.utils.DBTools;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetWriter implements SinkFunction<ConsumerRecord<byte[], byte[]>>, CheckpointedFunction {

    private final Tuple3<String, String, String> kafkaTuple;

    private transient ListState<Tuple4<String, String, TopicPartition, Long>> checkpointState;

    private final Map<Tuple3<String, String, TopicPartition>, Long> producerPartitionOffsets =
            new ConcurrentHashMap<>();

    private final ParameterTool parameters;

    public OffsetWriter(Tuple3<String, String, String> kafkaTuple,
                        ParameterTool parameters) {
        this.kafkaTuple = kafkaTuple;
        this.parameters = parameters;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        checkpointState.clear();
        int size = producerPartitionOffsets.size();
        if (size <= 0) {
            return;
        }
        Object[][] values = new Object[size][7];
        int index = 0;
        for (Map.Entry<Tuple3<String, String, TopicPartition>, Long> entry : producerPartitionOffsets.entrySet()) {
            Tuple3<String, String, TopicPartition> elementKey = entry.getKey();
            Long offset = entry.getValue();
            checkpointState.add(Tuple4.of(elementKey.f0, elementKey.f1, elementKey.f2, offset));
            //write to mysql
            String jobId = "sa-consumer";
            values[index++] = new Object[]{new Date(),
                    elementKey.f0,
                    elementKey.f2.topic(),
                    elementKey.f2.partition(),
                    offset,
                    elementKey.f1,
                    jobId};
        }

        DBTools.batchInsertCheckpointInfo(parameters, values);

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        ListStateDescriptor<Tuple4<String, String, TopicPartition, Long>> descriptor = new ListStateDescriptor<>(
                "consumer-partition-offset",
                TypeInformation.of(new TypeHint<Tuple4<String, String, TopicPartition, Long>>() {
                }));

        checkpointState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        log.info("[C:OffsetWriter - M:initializeState] - is enter.");
        //获取最近一次 checkpoint 成功的 offset
        if (functionInitializationContext.isRestored() && checkpointState != null) {
            for (Tuple4<String, String, TopicPartition, Long> element : checkpointState.get()) {
                producerPartitionOffsets.put(Tuple3.of(element.f0, element.f1, element.f2), element.f3);
            }
        }
        log.info("[C:OffsetWriter - M:initializeState] - [producerPartitionOffsets = {}]", producerPartitionOffsets);
    }

    @Override
    public void invoke(ConsumerRecord<byte[], byte[]> value, Context context) throws Exception {
        if (null == value) {
            return;
        }
        try {
            // checkpoint 记录
            String server = kafkaTuple.f0;
            String group = kafkaTuple.f1;
            TopicPartition topicPartition =
                    new TopicPartition(value.topic(),
                            value.offset());
            Tuple3<String, String, TopicPartition> combineTuple = Tuple3.of(server, group, topicPartition);
            long offset = value.offset();

            long oldOffset = producerPartitionOffsets.getOrDefault(combineTuple, 0L);
            if (oldOffset < offset) {
                producerPartitionOffsets.put(combineTuple, offset);
            }
        } catch (Exception e) {
            log.error("Offset record error: ", e);
        }
    }
}
