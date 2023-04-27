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
public class SinkProjectBean implements Serializable {
    private String kafkaServer;
    private String consumerGroup;
    private String sinkProjects;
    private String sinkTopic;
    private String sourceTopic;
}
