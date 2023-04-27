package com.sensorsdata.analytics.bean.format;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class SourceUserBean extends SourceBaseEntity {
    public static final long serialVersionUID = 1L;
    private String eventTimeFieldName;
    private String eventTime;
    private String userIdFieldName;
    private String userId;
    private JsonNode userPropertyObj;
}
