package com.dereckchen.remagen.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class BridgeConfig {
    private KafkaServerConfig kafkaServerConfig;
    private MQTTConfig mqttConfig;
}
