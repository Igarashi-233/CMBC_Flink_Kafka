package com.sensorsdata.analytics.bean.mysql;

import lombok.*;

import java.io.Serializable;

/**
 * @author igarashi233
 **/

@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PropertyMappingBean implements Serializable {
    public static final long serialVersionUID = 1L;
    private String projectId;
    private String propertyName;
    private String propertyNameSa;
    private Integer propertyType;

}
