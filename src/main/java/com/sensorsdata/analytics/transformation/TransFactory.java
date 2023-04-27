package com.sensorsdata.analytics.transformation;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sensorsdata.analytics.bean.format.SensorsEventBean;
import com.sensorsdata.analytics.bean.format.SourceBaseEntity;
import com.sensorsdata.analytics.bean.format.SourceEventBean;
import com.sensorsdata.analytics.bean.mysql.SinkProjectBean;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import com.sensorsdata.analytics.transformation.converter.SourceEventMessageConverter;
import com.sensorsdata.analytics.transformation.processor.ProcessorFamily;
import com.sensorsdata.analytics.utils.DateUtil;
import com.sensorsdata.analytics.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据处理
 *
 * @author igarashi233
 **/

@Slf4j
public class TransFactory implements Serializable {

    private TransFactory() {
    }

    private static class SingletonHelper {
        static final TransFactory INSTANCE = new TransFactory();
    }

    public static TransFactory getInstance() {
        return SingletonHelper.INSTANCE;
    }

    public static final String PROPERTIES_MAP_STATE = "properties_map";

    //状态描述
    private static final MapStateDescriptor<String, Map<String, Object>>
            PROPERTIES_MAP_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "properties_map_state_descriptor",
                    BasicTypeInfo.STRING_TYPE_INFO
                    , new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(Object.class)));

    /**
     * 对数据流先keyby，之后connect广播流，然后执行处理逻辑
     * 之所以keyby，是因为只有KeyedBroadcastProcessFunction可以用timeservice，注册定时器
     *
     * @param dataStream                    - 主数据流
     * @param metaData                      - 元数据
     * @param filterAndTransBroadCastStream - 广播流
     * @param invalidDataSideOutput         - 非法数据输出流
     * @return 单一数据输出流
     */
    public SingleOutputStreamOperator<JsonNode>
    processDataStream(DataStream<JsonNode> dataStream,
                      Map<String, Object> metaData,
                      DataStream<Map<String, Object>> filterAndTransBroadCastStream,
                      OutputTag<JsonNode> invalidDataSideOutput,
                      OutputTag<Map<Tuple5<String, String, String, String, String>, Counter>> counterListOutput,
                      ParameterTool parameters) {

        return dataStream.connect(filterAndTransBroadCastStream.broadcast(PROPERTIES_MAP_STATE_DESCRIPTOR))
                .process(new TransBroadcastProcessFunction(metaData, invalidDataSideOutput, counterListOutput, parameters));
    }

    private static class TransBroadcastProcessFunction
            extends
            BroadcastProcessFunction<JsonNode, Map<String, Object>, JsonNode> {

        private final OutputTag<JsonNode> invalidDataSideOutput;
        private final OutputTag<Map<Tuple5<String, String, String, String, String>, Counter>> counterListOutput;
        private final Map<String, Object> cache =
                new ConcurrentHashMap<>();

        private final ParameterTool parameters;

        private final Map<Tuple5<String, String, String, String, String>, Counter> counterMap =
                new ConcurrentHashMap<>();


        public TransBroadcastProcessFunction(Map<String, Object> metaData,
                                             OutputTag<JsonNode> invalidDataSideOutput,
                                             OutputTag<Map<Tuple5<String, String, String, String, String>, Counter>> counterListOutput,
                                             ParameterTool parameters) {
            this.cache.putAll(metaData);
            this.invalidDataSideOutput = invalidDataSideOutput;
            this.counterListOutput = counterListOutput;
            this.parameters = parameters;
        }

        @Override
        public void processElement(JsonNode fullRecord,
                                   ReadOnlyContext ctx,
                                   Collector<JsonNode> collector) {

            // checkpoint 记录
            String server = fullRecord.get(PropertiesConstants.RECORD_SERVER).asText();
            String group = fullRecord.get(PropertiesConstants.RECORD_GROUP).asText();

            if (fullRecord.has(PropertiesConstants.ERROR_REASON) && "Null Record".equals(fullRecord.get(PropertiesConstants.ERROR_REASON).asText())) {
                Tuple5<String, String, String, String, String> errorCounterInfo = new Tuple5<>(server,
                        group,
                        "",
                        "",
                        "0-NULL");
                incCounter(errorCounterInfo);
                return;
            }

            TopicPartition topicPartition =
                    new TopicPartition(fullRecord.get(PropertiesConstants.RECORD_TOPIC).asText(),
                            fullRecord.get(PropertiesConstants.RECORD_PARTITION).asInt());

            Tuple5<String, String, String, String, String> commonCounterInfo = new Tuple5<>(server,
                    group,
                    "",
                    topicPartition.topic(),
                    "");

            try {

                Tuple5<String, String, String, String, String> sourceCounterInfo = commonCounterInfo.copy();
                sourceCounterInfo.setField("1-SOURCE", 4);

                if (incCounter(sourceCounterInfo)) {
                    ctx.output(counterListOutput, counterMap);
                }

                // // find matching Processor from ProcessorFamily
                String methodKey = "processor." +
                        server.replace(':', '.') +
                        "." +
                        topicPartition.topic();
                List<SourceBaseEntity> beanList = ProcessorFamily.JSON_PROCESSOR_MAP
                        .getOrDefault(parameters.get(methodKey), ProcessorFamily.uniLogPlatformEvent)
                        .process(fullRecord);

                if (beanList.isEmpty()) {
//                    log.info("invalid original record {}", fullRecord);
//                    attachOutSideTips(fullRecord,
//                            "invalid-rule is: the original data does not match pattern or json format");
                    Tuple5<String, String, String, String, String> invalidCounterInfo = commonCounterInfo.copy();
                    invalidCounterInfo.setField("2-INVALID", 4);
                    if (incCounter(invalidCounterInfo)) {
                        ctx.output(counterListOutput, counterMap);
                    }
                    ctx.output(invalidDataSideOutput, fullRecord);
                }
                if (beanList.size() > 1) {
                    Tuple5<String, String, String, String, String> needSplitCounterInfo = commonCounterInfo.copy();
                    needSplitCounterInfo.setField("3-NEED_SPLIT", 4);
                    if (incCounter(needSplitCounterInfo)) {
                        ctx.output(counterListOutput, counterMap);
                    }

                    Tuple5<String, String, String, String, String> splitCounterInfo = commonCounterInfo.copy();
                    splitCounterInfo.setField("4-SPLIT", 4);
                    if (incCounter(splitCounterInfo, beanList.size())) {
                        ctx.output(counterListOutput, counterMap);
                    }
                }
                for (SourceBaseEntity sourceBaseEntity : beanList) {
                    processSingleRecord(fullRecord, sourceBaseEntity, ctx, collector, commonCounterInfo);
                }
            } catch (Exception e) {
                log.info("invalid original record {} with exception ", fullRecord, e);
                attachOutSideTips(fullRecord, "invalid-rule is: " + e);
                Tuple5<String, String, String, String, String> invalidCounterInfo = commonCounterInfo.copy();
                invalidCounterInfo.setField("2-INVALID", 4);
                if (incCounter(invalidCounterInfo)) {
                    ctx.output(counterListOutput, counterMap);
                }
                ctx.output(invalidDataSideOutput, fullRecord);
            }
        }

        private void processSingleRecord(JsonNode fullRecord,
                                         SourceBaseEntity sourceBaseEntity,
                                         ReadOnlyContext ctx,
                                         Collector<JsonNode> collector,
                                         Tuple5<String, String, String, String, String> commonCounterInfo) {
            String server = fullRecord.get(PropertiesConstants.RECORD_SERVER).asText();
            String group = fullRecord.get(PropertiesConstants.RECORD_GROUP).asText();
            TopicPartition topicPartition =
                    new TopicPartition(fullRecord.get(PropertiesConstants.RECORD_TOPIC).asText(),
                            fullRecord.get(PropertiesConstants.RECORD_PARTITION).asInt());

            List<SinkProjectBean> sinkProjectList =
                    (List<SinkProjectBean>) cache.get(PropertiesConstants.SINK_PROJECT_MAP);

            if (!StringUtils.isNotBlank(sourceBaseEntity.getProjectId()))
                sinkProjectList.forEach(sinkProjectBean -> {
                    if (sinkProjectBean.getKafkaServer().equals(server) &&
                            sinkProjectBean.getConsumerGroup().equals(group) &&
                            sinkProjectBean.getSourceTopic().equals(topicPartition.topic()))
                        sourceBaseEntity.setProjectId(sinkProjectBean.getSinkProjects());
                });

            if (sourceBaseEntity.getClass().equals(SourceEventBean.class)) {
                Tuple2<Object, String> eventResult = new SourceEventMessageConverter(cache).doConvert(sourceBaseEntity);
                String errorMsg = eventResult.f1;
                Object sensorsEventObject = eventResult.f0;

                //invalid data ,OutputTag
                if (errorMsg != null) {
                    log.info("below record with length {} and error: {}", fullRecord.toString().length(), errorMsg);
                    log.info("invalid record: {}", fullRecord.toString());
                    attachOutSideTips(fullRecord, " invalid-rule is : " + errorMsg);
                    ctx.output(invalidDataSideOutput, fullRecord);
                }

                SensorsEventBean sensorsEventBean = (SensorsEventBean) sensorsEventObject;
                //add pa_key
                sensorsEventBean.getProperties().put(PropertiesConstants.RECORD_KEY,
                        fullRecord.get(PropertiesConstants.RECORD_KEY).asText());
                try {
                    //add pa_date
                    String date = DateUtil.format(sensorsEventBean.getTime(), DateUtil.DateFormatEnum.YYYYMMDD);
                    sensorsEventBean.getProperties().put("date", date);
                } catch (Exception e) {
                    sensorsEventBean.getProperties().put("date", "invalid date");
                }

                //add partition offset
                sensorsEventBean.getProperties().put(PropertiesConstants.RECORD_SERVER,
                        server);
                sensorsEventBean.getProperties().put(PropertiesConstants.RECORD_GROUP,
                        group);
                sensorsEventBean.getProperties().put(PropertiesConstants.RECORD_TOPIC,
                        topicPartition.topic());
                sensorsEventBean.getProperties().put(PropertiesConstants.RECORD_PARTITION,
                        topicPartition.partition());
                sensorsEventBean.getProperties().put(PropertiesConstants.RECORD_OFFSET,
                        fullRecord.get(PropertiesConstants.RECORD_OFFSET).asText());

                String resultRecord = JsonUtil.toJson(sensorsEventBean);
                if (null == resultRecord) {
                    log.error("converted event is null and original dataRecord is {}", sourceBaseEntity.toString());

                    Tuple5<String, String, String, String, String> invalidCounterInfo = commonCounterInfo.copy();
                    invalidCounterInfo.setField("5-INVALID", 4);
                    if (incCounter(invalidCounterInfo)) {
                        ctx.output(counterListOutput, counterMap);
                    }
                    attachOutSideTips(fullRecord, " invalid-rule is : event bean cannot transfer to JSON");
                    ctx.output(invalidDataSideOutput, fullRecord);
                }
                JsonNode eventNode = JsonUtil.getBean(resultRecord);

                //if it's signup event, build signup event
                String methodKey = "post." +
                        server.replace(':', '.') +
                        "." +
                        topicPartition.topic();
                boolean signUpEvent = ProcessorFamily.POST_PROCESSOR_MAP
                        .getOrDefault(parameters.get(methodKey), ProcessorFamily.uniLogPlatformPost)
                        .isSignUpEvent(sensorsEventBean);

                // todo: 新建一条事件数据而不是修改原有
                if (signUpEvent) {
                    if (sensorsEventBean.getEvent().equals("$SignUp")) {
                        log.info("For test isSignUpEvent,Current event [event_name = {}]", sensorsEventBean.getEvent());
                    }
                    sensorsEventBean.setType("track_signup");
                    sensorsEventBean.setEvent("$SignUp");
                    sensorsEventBean.setOriginal_id(sensorsEventBean.getProperties().getString("$device_id"));
                    JsonNode signUpNode = JsonUtil.getBean(JsonUtil.toJson(sensorsEventBean));

                    for (String project : sourceBaseEntity.getProjectId().split(",")) {
                        Tuple5<String, String, String, String, String> signUpCounterInfo = commonCounterInfo.copy();
                        signUpCounterInfo.setField(project, 2);
                        signUpCounterInfo.setField("6-SIGNUP", 4);
                        if (incCounter(signUpCounterInfo)) {
                            ctx.output(counterListOutput, counterMap);
                        }
                        ((ObjectNode) signUpNode).put(PropertiesConstants.PROJECT, project);
                        collector.collect(signUpNode);
                    }
                }

                for (String project : sourceBaseEntity.getProjectId().split(",")) {
                    Tuple5<String, String, String, String, String> sinkCounterInfo = commonCounterInfo.copy();
                    sinkCounterInfo.setField(project, 2);
                    sinkCounterInfo.setField("7-SINK", 4);
                    if (incCounter(sinkCounterInfo)) {
                        ctx.output(counterListOutput, counterMap);
                    }\
                    ((ObjectNode) eventNode).put("project", project);
                    collector.collect(eventNode);
                }
            }
        }

        /**
         * 异常信息包装
         *
         * @param eventData
         * @param tipMsg
         */
        private void attachOutSideTips(JsonNode eventData, String tipMsg) {
            ((ObjectNode) eventData).put(PropertiesConstants.ERROR_REASON, tipMsg);
            ((ObjectNode) eventData).put(PropertiesConstants.VERSION_TAG, "InvalidTag");
        }

        /**
         * 处理广播数据，将广播数据放入propertiesMapState中，供实时数据流获取
         *
         * @param ruleMap
         * @param context
         * @param collector
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(Map<String, Object> ruleMap,
                                            Context context, Collector<JsonNode> collector) throws Exception {
            if (null == ruleMap || ruleMap.isEmpty())
                return;
            context.getBroadcastState(PROPERTIES_MAP_STATE_DESCRIPTOR).put(PROPERTIES_MAP_STATE, ruleMap);

            Map<String, Object> broadcastState =
                    context.getBroadcastState(PROPERTIES_MAP_STATE_DESCRIPTOR).get(PROPERTIES_MAP_STATE);
            //获取广播cache
            if (null != broadcastState) {
                cache.putAll(broadcastState);
            }
        }

        private boolean incCounter(Tuple5<String, String, String, String, String> counterInfo) {
            return incCounter(counterInfo, 1);
        }

        private boolean incCounter(Tuple5<String, String, String, String, String> counterInfo, long cnt) {
            if (counterMap.containsKey(counterInfo)) {
                counterMap.get(counterInfo).inc(cnt);
//                log.info("Counter {} Modified to {}", counterMap.get(counterInfo), counterMap.get(counterInfo).getCount());
                return false;
            } else {
                String counterName = String.valueOf(counterInfo.hashCode());
                Counter newCounter = getRuntimeContext().getMetricGroup().counter(counterName);
                newCounter.inc(cnt);
                counterMap.put(counterInfo, newCounter);
//                log.info("Counter {} Added to {}", counterMap.get(counterInfo), counterMap.get(counterInfo).getCount());
                return true;
            }
        }

    }

}