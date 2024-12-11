package com.dereck.remagen.mqtt.models;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class BridgeOption {
    private String mqttTopic;
    private String kafkaTopic;
    private String kafkaKey;
    private Map<String, String> props;
}
