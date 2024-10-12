package com.sensorsdata.analytics;

import com.cmb.bdp1.utils.CaseConf;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.sensorsdata.analytics.bean.mysql.SourceSinkBean;
import com.sensorsdata.analytics.bean.serialization.CustomKafkaDeserializationSchema;
import com.sensorsdata.analytics.bean.serialization.EventDataSchema;
import com.sensorsdata.analytics.bean.serialization.InvalidEventDataSchema;
import com.sensorsdata.analytics.connectors.kafka.KafkaGenerateFactory;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import com.sensorsdata.analytics.operator.sink.RecordCounterWriter;
import com.sensorsdata.analytics.transformation.TransFactory;
import com.sensorsdata.analytics.transformation.broadcast.PropertiesMap;
import com.sensorsdata.analytics.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Kafka to SA 入口程序
 *
 * @author igarashi233
 **/

@Slf4j
public class FlinkMain {

    private String propertiesFileName = "/application.properties";

    private Long startTimeStamp = 0L;

    public static void main(String[] args) throws Exception {
        FlinkMain flinkDaemon = new FlinkMain();
        CaseConf caseConf = new CaseConf(args[0]);
        flinkDaemon.propertiesFileName = "/".concat(caseConf.get("properties_file"));
        flinkDaemon.startTimeStamp = caseConf.getLong("start_timestamp", 0L);

        log.info("start flink main with properties file. [path = {}] ", flinkDaemon.propertiesFileName);

        ParameterTool parameters;
        try {
            parameters = ParameterTool.fromPropertiesFile(FlinkMain.class.getResourceAsStream(flinkDaemon.propertiesFileName));
        } catch (IOException io) {
            throw new IOException("properties file exception {}", io);
        }

        //配置flink的重启策略
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameters);
        //配置flink的checkpoint相关参数
        boolean failOnCPErrors = Boolean.parseBoolean(caseConf.get("fail_on_cp_errors", Boolean.TRUE.toString()));
        CheckPointUtil.setCheckpointConfig(env, parameters, failOnCPErrors);

        env.getConfig().setGlobalJobParameters(parameters);

        if (!parameters.getBoolean("enable.operator.chain", true)) {
            env.disableOperatorChaining();
        }

        //job manager加载一次元数据,用于初始化
        Map<String, Object> metaData = MetaDataUtil.loadAllMetaData(parameters);

        // Map<Tuple3<kafkaServer, consumerGroup, authString>, Map<sinkProjectId, List<sourceTopic>>
        Map<Tuple3<String, String, String>, Map<String, List<String>>> kafkaSourceSink = Maps.newHashMap();
        List<SourceSinkBean> projectTopicList = (List<SourceSinkBean>) metaData.get(PropertiesConstants.TOPIC_PROJECT_MAP);
        for (SourceSinkBean sourceSinkBean : projectTopicList) {
            Tuple3<String, String, String> kafkaInfo = Tuple3.of(sourceSinkBean.getKafkaServer(),
                    sourceSinkBean.getConsumerGroup(),
                    sourceSinkBean.getAuthString());
            // Map<sinkProjectId, List<sourceTopic>>
            Map<String, List<String>> projectTopicMaps = kafkaSourceSink.getOrDefault(kafkaInfo, Maps.newHashMap());
            projectTopicMaps.put(sourceSinkBean.getSinkProject(),
                    Arrays.asList(sourceSinkBean.getSourceTopics().split(",").clone()));
            kafkaSourceSink.put(kafkaInfo, projectTopicMaps);
        }

        log.info("[C:FLinkMain] -- kafkaSourceSink is {}", kafkaSourceSink);
        System.out.println("[C:FLinkMain] -- kafkaSourceSink is " + kafkaSourceSink);

        //主程序获取重启后，从指定的partition-offset消费
        // Map<sourceTopic, Map<partition, offset>>
        // Map<Tuple3<kafkaServer, consumerGroup, authString>, Map<sourceTopic, Map<partition, offset>>>
        Map<Tuple3<String, String, String>, Map<String, Map<Integer, Long>>> serverTopicPartitionOffset =
                RestartDuplicateUtil.processRestartDuplicateRemoval(parameters, kafkaSourceSink);

        long startTimestamp = flinkDaemon.startTimeStamp;
        if (startTimestamp > 0) {
            log.info("start from spec [timestamp = {} ] ", startTimestamp);
            System.out.println("[C:FlinkMain] - start from spec timestamp " + startTimestamp);
            serverTopicPartitionOffset =
                    StartFromSpecificOffsetUtil
                            .processRestartDuplicateRemoval(parameters, startTimestamp, kafkaSourceSink);
        }

        log.info("start from spec [topic - partition/offset = {} ] ",
                serverTopicPartitionOffset);
        System.out.println("[C:FlinkMain] - start from spec [topic - partition/offset = " +
                serverTopicPartitionOffset);

        //消费kafka数据
        DataStream<JsonNode> originalKafkaDataStream =
                KafkaGenerateFactory.createKafkaDataStream(env,
                        parameters,
                        CustomKafkaDeserializationSchema.class,
                        serverTopicPartitionOffset);

        //相关数据处理规则的广播流
        DataStream<Map<String, Object>> ruleMap =
                env.addSource(new PropertiesMap(parameters))
                        .name("broadcast-rule-map")
                        .setParallelism(1)
                        .uid("broadcast-rule-map-step-2");

        //定义非法数据旁路输出
        OutputTag<JsonNode> invalidDataSideOutput = new OutputTag<JsonNode>("invalidDataSideOutput") {
        };

        OutputTag<Map<Tuple5<String, String, String, String, String>, Counter>> counterListOutput =
                new OutputTag<Map<Tuple5<String, String, String, String, String>, Counter>>("counterName") {
                };

        //处理数据的主逻辑
        SingleOutputStreamOperator<JsonNode> flatMapTransDataStream =
                TransFactory.getInstance().processDataStream(
                        originalKafkaDataStream,
                        metaData,
                        ruleMap,
                        invalidDataSideOutput,
                        counterListOutput,
                        parameters)
//                        .setParallelism(parameters.getInt(PropertiesConstants.MAIN_PROCESS_PARALLELISM))
                        .name("transformation")
                        .uid("transformation-step-4");


        String kafkaProducerRetries = caseConf.get("kafka_producer_retries", "0");
        String kafkaProducerAck = caseConf.get("kafka_producer_ack", "-1");
        flatMapTransDataStream.addSink(
                KafkaGenerateFactory.toKafkaDataStream(parameters,
                        EventDataSchema.class,
                        parameters.get(PropertiesConstants.PRODUCER_TOPIC),
                        kafkaProducerRetries,
                        kafkaProducerAck))
//                .setParallelism(parameters.getInt(PropertiesConstants.STREAM_SINK_PARALLELISM))
                .name("sink-sa-event-sensor").uid("sink-kafka-step-5-sensor");

        //处理之后的异常数据输出到sa-event-sa-invalid-data
        flatMapTransDataStream.getSideOutput(invalidDataSideOutput)
                .addSink(KafkaGenerateFactory.toKafkaDataStream(parameters,
                        InvalidEventDataSchema.class,
                        parameters.get(PropertiesConstants.INVALID_PRODUCER_TOPIC),
                        kafkaProducerRetries,
                        kafkaProducerAck))
//                .setParallelism(parameters.getInt(PropertiesConstants.STREAM_SINK_PARALLELISM))
                .name("sink-kafka-invalid-data").uid("sink-invalid-data-kafka-step-6");

        flatMapTransDataStream.getSideOutput(counterListOutput)
                .addSink(new RecordCounterWriter(parameters, metaData))
                .setParallelism(1)
                .name("sink-counter");

        String appName = caseConf.get("framework.streaming.appName", "FlinkApp");
        env.execute(appName);
    }
}
