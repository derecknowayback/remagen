package com.dereckchen.remagen.mqtt.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KafkaServerConfig {
    private String host;
    private String port;
    private boolean needHttps;
}
