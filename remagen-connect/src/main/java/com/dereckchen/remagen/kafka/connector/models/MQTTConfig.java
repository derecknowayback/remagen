package com.dereckchen.remagen.kafka.connector.models;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class MQTTConfig {
    private String broker;
    private String topic;
    private String username;
    private String password;
    private String clientid;
}
