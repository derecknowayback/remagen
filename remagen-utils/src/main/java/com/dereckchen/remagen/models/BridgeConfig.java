package com.dereckchen.remagen.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration class for the bridge between Kafka and MQTT.
 * This class holds the necessary configurations for both Kafka and MQTT servers.
 */
@Data
@Builder
@AllArgsConstructor
public class BridgeConfig {
    // Configuration for the Kafka server
    private KafkaServerConfig kafkaServerConfig;
    // Configuration for the MQTT server
    private MQTTConfig mqttConfig;
}

