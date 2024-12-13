package com.dereckchen.remagen.kafka.connector.utils;


import com.dereckchen.remagen.kafka.connector.models.MQTTConfig;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;


@Slf4j
public class MQTTUtil {


    public static MqttClient getMQTTClient(MQTTConfig mqttConfig) {
        MqttClient mqttClient;
        try {
            mqttClient = new MqttClient(mqttConfig.getBroker(), mqttConfig.getClientid(), new MemoryPersistence());
        } catch (MqttException exception) {
            log.error("Create client error, config: {}", mqttConfig, exception);
            throw new RuntimeException(exception);
        }

        try {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(mqttConfig.getUsername());
            options.setPassword(mqttConfig.password.toCharArray());
            options.setConnectionTimeout(0);
            options.setKeepAliveInterval(0);
            options.setAutomaticReconnect(false);
            mqttClient.connect(options);
        } catch (MqttException  exception) {
            log.error("Connect to broker error, config: {}", mqttConfig, exception);
            throw new RuntimeException(exception);
        }

        return mqttClient;
    }
}
