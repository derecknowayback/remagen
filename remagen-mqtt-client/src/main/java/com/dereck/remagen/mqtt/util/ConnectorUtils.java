package com.dereck.remagen.mqtt.util;

import com.dereck.remagen.mqtt.consts.ConnectorConst;

public class ConnectorUtils {
    public static String getConnectorName(String mqttTopicName, String kafkaTopicName) {
        return String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopicName, kafkaTopicName);
    }
}
