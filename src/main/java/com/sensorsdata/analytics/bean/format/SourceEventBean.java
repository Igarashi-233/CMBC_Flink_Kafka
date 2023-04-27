package com.sensorsdata.analytics.bean.format;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author igarashi233
 **/

@Data
@EqualsAndHashCode(callSuper = false)
public class SourceEventBean extends SourceBaseEntity {
    public static final long serialVersionUID = 1L;
    private String appVersionFieldName;
    private String appVersion;
    private String loginIdFieldName;
    private String loginId;
    private String deviceIdFieldName;
    private String deviceId;
    private String eventNameFieldName;
    private String eventName;
    private String eventTimeFieldName;
    private String eventTime;
    private JsonNode mainObj;
}
