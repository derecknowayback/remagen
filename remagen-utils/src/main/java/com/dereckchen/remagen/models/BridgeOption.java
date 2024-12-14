package com.dereckchen.remagen.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BridgeOption {
    private String mqttTopic;
    private String kafkaTopic;
    private Map<String, String> props;
}
