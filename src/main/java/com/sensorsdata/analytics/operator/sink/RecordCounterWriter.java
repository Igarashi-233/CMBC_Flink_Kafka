package com.sensorsdata.analytics.operator.sink;

import com.sensorsdata.analytics.bean.mysql.CounterBean;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import com.sensorsdata.analytics.utils.DBTools;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Log4j2
public class RecordCounterWriter extends RichSinkFunction<Map<Tuple5<String, String, String, String, String>, Counter>>
        implements CheckpointedFunction {

    private final Map<Tuple5<String, String, String, String, String>, Counter> counterMap =
            new ConcurrentHashMap<>();
    private final Map<Tuple5<String, String, String, String, String>, Counter> oldCounterMap =
            new ConcurrentHashMap<>();
    private final Map<String, Object> cache =
            new ConcurrentHashMap<>();

    private final ParameterTool parameters;

    public RecordCounterWriter(ParameterTool parameters,
                               Map<String, Object> metaData) {
        this.parameters = parameters;
        this.cache.putAll(metaData);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        if (oldCounterMap.isEmpty()) {
            String queryCountSql = parameters.get("query.counter.sql");
            List<CounterBean> counterBeanList = DBTools.queryCounterBeanList(queryCountSql, parameters);
            counterBeanList.forEach(counterBean -> {
                Tuple5<String, String, String, String, String> counterInfo = new Tuple5<>(
                        counterBean.getKafkaServer(),
                        counterBean.getConsumerGroup(),
                        counterBean.getSinkProject(),
                        counterBean.getSourceTopic(),
                        counterBean.getStage());
                String counterName = String.valueOf(counterInfo.hashCode()).concat("_ACC");
                Counter eachCounter = getRuntimeContext().getMetricGroup().counter(counterName);
                eachCounter.inc(counterBean.getCnt());
                oldCounterMap.put(counterInfo, eachCounter);
            });
        }

        Object[][] newCounterRecords = new Object[counterMap.size()][7];
        Object[][] oldCounterRecords = new Object[oldCounterMap.size()][7];
        int newIndex = 0;
        int oldIndex = 0;
        for (Map.Entry<Tuple5<String, String, String, String, String>, Counter> entry : counterMap.entrySet()) {
            Tuple5<String, String, String, String, String> counterInfo = entry.getKey();
            Counter counter = entry.getValue();
            long newCount = counter.getCount();
            if (oldCounterMap.containsKey(counterInfo)) {
                oldCounterMap.get(counterInfo).inc(newCount);
                oldCounterRecords[oldIndex++] = new Object[]{
                        oldCounterMap.get(counterInfo).getCount(),
                        new Timestamp(new Date().getTime()),
                        counterInfo.f0,
                        counterInfo.f2,
                        counterInfo.f1,
                        counterInfo.f3,
                        counterInfo.f4
                };
            } else {
                newCounterRecords[newIndex++] = new Object[]{
                        newCount,
                        new Timestamp(new Date().getTime()),
                        counterInfo.f0,
                        counterInfo.f2,
                        counterInfo.f1,
                        counterInfo.f3,
                        counterInfo.f4
                };
                String counterName = String.valueOf(counterInfo.hashCode()).concat("_ACC");
                Counter acuCounter = getRuntimeContext().getMetricGroup().counter(counterName);
                acuCounter.inc(newCount);
                oldCounterMap.put(counterInfo, acuCounter);
            }
            counter.dec(newCount);
        }
        if (newIndex > 0) {
            Object[][] newCounterRecordsFit = new Object[newIndex][7];
            System.arraycopy(newCounterRecords, 0, newCounterRecordsFit, 0, newIndex);
            DBTools.batchInsertCounterInfo(parameters, newCounterRecordsFit);
        }
        if (oldIndex > 0) {
            Object[][] oldCounterRecordsFit = new Object[oldIndex][7];
            System.arraycopy(oldCounterRecords, 0, oldCounterRecordsFit, 0, oldIndex);
            DBTools.batchUpdateCounterInfo(parameters, oldCounterRecordsFit);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        if (!cache.isEmpty()) {
            List<CounterBean> counterBeanList = (List<CounterBean>) cache.get(PropertiesConstants.COUNTER);
            counterBeanList.forEach(counterBean -> {
                Tuple5<String, String, String, String, String> counterInfo = new Tuple5<>(
                        counterBean.getKafkaServer(),
                        counterBean.getConsumerGroup(),
                        counterBean.getSinkProject(),
                        counterBean.getSourceTopic(),
                        counterBean.getStage());
                String counterName = String.valueOf(counterInfo.hashCode()).concat("_ACC");
                Counter eachCounter = getRuntimeContext().getMetricGroup().counter(counterName);
                eachCounter.inc(counterBean.getCnt());
                oldCounterMap.put(counterInfo, eachCounter);
            });
        }
    }

    @Override
    public void invoke(Map<Tuple5<String, String, String, String, String>, Counter> value, Context context) throws Exception {
        if (null == value) {
            return;
        }
        try {
            counterMap.putAll(value);
        } catch (Exception e) {
            log.error("Record count error", e);
        }
    }
}
