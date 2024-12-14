package com.dereckchen.remagen.utils;

import com.dereckchen.remagen.consts.ConnectorConst;
import com.dereckchen.remagen.models.BridgeConfig;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.models.MQTTConfig;

import java.util.Map;

public class ConnectorUtils {
    public static String getConnectorName(String mqttTopicName, String kafkaTopicName) {
        return String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopicName, kafkaTopicName);
    }

    public static BridgeConfig parseConfig(Map<String, String> props) {
        KafkaServerConfig kafkaServerConfig = KafkaUtils.parseConfig(props);
        MQTTConfig mqttConfig = MQTTUtil.parseConfig(props);
        return BridgeConfig.builder()
                .kafkaServerConfig(kafkaServerConfig)
                .mqttConfig(mqttConfig).build();
    }

}
