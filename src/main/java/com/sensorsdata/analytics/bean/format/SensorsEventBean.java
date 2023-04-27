package com.sensorsdata.analytics.bean.format;

import lombok.*;

@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SensorsEventBean extends SensorsBaseAttributesBean {
    public static final long serialVersionUID = 1L;
    private String event;
    private Boolean time_free;
    private String original_id;
}
