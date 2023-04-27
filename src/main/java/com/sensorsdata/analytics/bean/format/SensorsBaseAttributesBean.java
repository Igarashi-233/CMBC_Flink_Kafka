package com.sensorsdata.analytics.bean.format;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public abstract class SensorsBaseAttributesBean extends BaseEntity {
    private String distinct_id;
    private Long time;
    private String project;
    private String type;
    private JSONObject properties = new JSONObject();
}
