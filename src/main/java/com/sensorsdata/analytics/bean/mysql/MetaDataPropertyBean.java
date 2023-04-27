package com.sensorsdata.analytics.bean.mysql;

import lombok.*;

import java.io.Serializable;

/**
 * @author zhangshk_
 * @since 2020-07-01
 **/
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetaDataPropertyBean implements Serializable {
    public static final long serialVersionUID = 1L;
    private String projectId;
    private String propertyName;
    private String propertyChName;

}
