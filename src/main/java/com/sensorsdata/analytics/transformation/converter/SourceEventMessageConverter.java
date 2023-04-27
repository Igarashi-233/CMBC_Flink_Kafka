package com.sensorsdata.analytics.transformation.converter;

import com.alibaba.fastjson.JSONObject;
import com.sensorsdata.analytics.bean.format.SensorsEventBean;
import com.sensorsdata.analytics.bean.format.SourceEventBean;
import com.sensorsdata.analytics.constants.CommConst;
import com.sensorsdata.analytics.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

/**
 * @author igarashi233
 **/
@Slf4j
public class SourceEventMessageConverter extends AbstractCommonConverter {

    private Map<String, Object> cacheMap;

    public SourceEventMessageConverter(Map<String, Object> cacheMap) {
        this.cacheMap = cacheMap;
    }

    private SourceEventBean convertSourceMessage(Object source) {
        return (SourceEventBean) source;
    }

    @Override
    public Tuple2<Object, String> doConvert(Object source) {
        SourceEventBean sourceEventBean = convertSourceMessage(source);
        SensorsEventBean sensorsEventBean = new SensorsEventBean();
        //fixme：进入嵌套调用
        String isValidAndErrorReasonMainBody = convertMainBody(sourceEventBean, sensorsEventBean);
        //最外层优先级最高
        String isValidAndErrorReasonCommon = convertCommonProperties(sourceEventBean, sensorsEventBean);

        StringBuilder stringBuilder = new StringBuilder();
        if (isValidAndErrorReasonMainBody != null) stringBuilder.append(isValidAndErrorReasonMainBody).append(" || ");
        if (isValidAndErrorReasonCommon != null) stringBuilder.append(isValidAndErrorReasonCommon).append(" || ");

        String result = stringBuilder.length() == 0 ? null : (stringBuilder.substring(0, stringBuilder.length() - 4));
        return Tuple2.of(sensorsEventBean, result);
    }

    private String convertMainBody(SourceEventBean sourceEventBean, SensorsEventBean sensorsEventBean) {
        return doPropertiesMapping(sourceEventBean, sensorsEventBean,
                CommConst.PropertyType.MAIN_BODY.getType(), this.cacheMap);
    }

    private String convertCommonProperties(SourceEventBean sourceEventBean, SensorsEventBean sensorsEventBean) {
        StringBuilder stringBuilder = new StringBuilder();
        //设置event属性
        if (StringUtils.isNotBlank(sourceEventBean.getLoginId())) {
            sensorsEventBean.setDistinct_id(StringUtils.substring(sourceEventBean.getLoginId(), 0, 255));
        } else if (StringUtils.isNotBlank(sourceEventBean.getDeviceId())) {
            sensorsEventBean.setDistinct_id(StringUtils.substring(sourceEventBean.getDeviceId(), 0, 255));
        } else stringBuilder.append("No ID, login or device, is presented!").append(" || ");

        sensorsEventBean.setType("track");

        long time = DateUtil.format(sourceEventBean.getEventTime());
        sensorsEventBean.setTime(time);
        if (0L == time)
            stringBuilder.append("Time format is invalid!").append(" || ");
//        sensorsEventBean.setTime_free(true);

        if (StringUtils.isNotBlank(sourceEventBean.getEventName()))
            sensorsEventBean.setEvent(sourceEventBean.getEventName());
        else {
            stringBuilder.append("Event Name is not presented!").append(" || ");
            sensorsEventBean.setEvent("");
        }

        sensorsEventBean.setProject(sourceEventBean.getProjectId());
        //other event 特殊处理
        JSONObject properties = sensorsEventBean.getProperties();
        commonPropertiesSpecial(properties, sourceEventBean);

        return stringBuilder.length() == 0 ? null : (stringBuilder.substring(0, stringBuilder.length() - 4));
    }

    private void commonPropertiesSpecial(JSONObject properties, SourceEventBean hiveEventBean) {

        properties.put("$app_version", hiveEventBean.getAppVersion());
        properties.put("$device_id", hiveEventBean.getDeviceId());
        properties.put("event_name", hiveEventBean.getEventName());
        properties.put("login_id", hiveEventBean.getLoginId());
        properties.put("$is_login_id", StringUtils.isNotBlank(hiveEventBean.getLoginId()));
    }

}
