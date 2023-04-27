package com.sensorsdata.analytics.transformation.converter;

import com.sensorsdata.analytics.bean.format.SensorsUserBean;
import com.sensorsdata.analytics.bean.format.SourceUserBean;
import com.sensorsdata.analytics.constants.CommConst;
import com.sensorsdata.analytics.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

/**
 * @author igarashi233
 **/

public class SourceUserMessageConverter extends AbstractCommonConverter {

    private Map<String, Object> cacheMap;

    public SourceUserMessageConverter(Map<String, Object> cacheMap) {
        this.cacheMap = cacheMap;
    }

    private SourceUserBean convertHiveMessage(Object source) {
        return (SourceUserBean) source;
    }

    @Override
    public Tuple2<Object, String> doConvert(Object source) {
        SourceUserBean sourceUserBean = convertHiveMessage(source);
        SensorsUserBean sensorsUserBean = new SensorsUserBean();
        //根据业务需要覆盖的属性依次处理
        String errorReason = convertUserProperties(sourceUserBean, sensorsUserBean);
        //最外层优先级最高
        convertCommonProperties(sourceUserBean, sensorsUserBean);
        return Tuple2.of(sensorsUserBean, errorReason);
    }

    private String convertUserProperties(SourceUserBean sourceUserBean, SensorsUserBean sensorsUserBean) {
        return doPropertiesMapping(sourceUserBean, sensorsUserBean, CommConst.PropertyType.USERPROPERTY.getType(), cacheMap);
    }

    private void convertCommonProperties(SourceUserBean sourceUserBean, SensorsUserBean sensorsUserBean) {
        String projectId = sourceUserBean.getProjectId();
        //设置event属性
        if (StringUtils.isNotBlank(sourceUserBean.getUserId())) {
            sensorsUserBean.setDistinct_id(StringUtils.substring(sourceUserBean.getUserId(), 0, 255));
        }
        sensorsUserBean.setProject("SA_" + projectId);
        sensorsUserBean.setType("profile_set");
        long time = DateUtil.format(sourceUserBean.getEventTime());
        sensorsUserBean.setTime(time);
        sensorsUserBean.getProperties().put("pa_userid", sourceUserBean.getUserId());
        sensorsUserBean.getProperties().put("$is_login_id", true);
    }
}
