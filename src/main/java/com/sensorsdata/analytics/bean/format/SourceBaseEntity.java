package com.sensorsdata.analytics.bean.format;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author igarashi233
 **/

@Data
@EqualsAndHashCode(callSuper = false)
public abstract class SourceBaseEntity extends BaseEntity {
    private String projectId;
    private String projectIdFieldName;
}
