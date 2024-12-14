package com.dereckchen.remagen.kafka.connector.utils;


import com.dereckchen.remagen.kafka.connector.models.MQTTConfig;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
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
            options.setPassword(mqttConfig.getPassword().toCharArray());
            options.setConnectionTimeout(0);
            options.setKeepAliveInterval(0);
            options.setAutomaticReconnect(true);
            mqttClient.connect(options);
        } catch (MqttException exception) {
            log.error("Connect to broker error, config: {}", mqttConfig, exception);
            throw new RuntimeException(exception);
        }

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                for (int i = 0; i < 100; i++) {
                    log.error("Connection lost: {}", cause.getMessage(),cause);
                }
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {}

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                log.info("Successfully deliver token: {}",token);
            }
        });

        return mqttClient;
    }
}
