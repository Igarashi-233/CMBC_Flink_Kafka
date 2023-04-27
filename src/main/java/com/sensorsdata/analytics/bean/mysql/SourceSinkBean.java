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
public class SourceSinkBean implements Serializable {
    private String kafkaServer;
    private String consumerGroup;
    private String authString;
    private String sinkProject;
    private String sinkTopic;
    private String sourceTopics;
}
