package com.dereckchen.remagen.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KafkaServerConfig {
    private String host;
    private String port;
    private boolean needHttps;
    private String kafkaTopics; // can be multi topics separate by commas
}
