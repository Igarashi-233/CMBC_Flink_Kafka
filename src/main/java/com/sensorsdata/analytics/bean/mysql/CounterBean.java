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
public class CounterBean implements Serializable {
    private String kafkaServer;
    private String consumerGroup;
    private String sinkProject;
    private String sourceTopic;
    private String stage;
    private Long cnt;

}
