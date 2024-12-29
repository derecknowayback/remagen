package com.dereckchen.remagen.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BridgeOption {
    /**
     * The MQTT topic to which the message will be published.
     */
    private String mqttTopic;
    /**
     * The Kafka topic to which the message will be published.
     */
    private String kafkaTopic;
    /**
     * Additional properties for the bridge, such as the Kafka and MQTT server addresses, usernames, passwords, etc.
     */
    private Map<String, String> props;
}
