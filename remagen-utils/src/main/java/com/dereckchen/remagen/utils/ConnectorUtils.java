package com.dereckchen.remagen.utils;

import com.dereckchen.remagen.consts.ConnectorConst;
import com.dereckchen.remagen.models.BridgeConfig;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.models.MQTTConfig;

import java.util.Map;

public class ConnectorUtils {
    /**
     * Generates a connector name based on the given MQTT and Kafka topic names.
     *
     * @param mqttTopicName  The name of the MQTT topic.
     * @param kafkaTopicName The name of the Kafka topic.
     * @return A string representing the connector name.
     */
    public static String getConnectorName(String mqttTopicName, String kafkaTopicName) {
        return String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopicName, kafkaTopicName);
    }

    /**
     * Parses the configuration map and returns a BridgeConfig object.
     *
     * @param props A map containing the configuration properties.
     * @return A BridgeConfig object containing the parsed configuration.
     */
    public static BridgeConfig parseConfig(Map<String, String> props) {
        KafkaServerConfig kafkaServerConfig = KafkaUtils.parseConfig(props);
        MQTTConfig mqttConfig = MQTTUtil.parseConfig(props);
        return BridgeConfig.builder()
                .kafkaServerConfig(kafkaServerConfig)
                .mqttConfig(mqttConfig).build();
    }
}

