package com.dereckchen.remagen.mqtt.util;

import com.dereckchen.remagen.mqtt.consts.ConnectorConst;

public class ConnectorUtils {
    public static String getConnectorName(String mqttTopicName, String kafkaTopicName) {
        return String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopicName, kafkaTopicName);
    }
}
