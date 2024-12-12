package com.dereckchen.remagen.utils;

import com.dereckchen.remagen.consts.ConnectorConst;

public class ConnectorUtils {
    public static String getConnectorName(String mqttTopicName, String kafkaTopicName) {
        return String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopicName, kafkaTopicName);
    }
}
