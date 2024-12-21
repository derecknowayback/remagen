package com.dereckchen.remagen.utils;


import com.dereckchen.remagen.consts.ConnectorConst;
import com.dereckchen.remagen.exceptions.PanicException;
import com.dereckchen.remagen.exceptions.RetryableException;
import com.dereckchen.remagen.models.MQTTConfig;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Map;
import java.util.function.BooleanSupplier;


@Slf4j
public class MQTTUtil {


    public static MQTTConfig parseConfig(Map<String, String> props) {
        return MQTTConfig.builder()
                .password(props.get(ConnectorConst.PROPS_MQTT_PASSWORD))
                .clientid(props.get(ConnectorConst.PROPS_MQTT_CLIENTID))
                .username(props.get(ConnectorConst.PROPS_MQTT_USERNAME))
                .broker(props.get(ConnectorConst.PROPS_MQTT_BROKER))
                .topic(props.get(ConnectorConst.PROPS_MQTT_TOPIC)).build();
    }


    public static MqttClient getMqttClient(MQTTConfig mqttConfig) {
        try {
            MqttClient mqttClient = new MqttClient(mqttConfig.getBroker(), mqttConfig.getClientid(), new MemoryPersistence());
            mqttClient.setManualAcks(true); // manage ack by ourselves
            return mqttClient;
        } catch (MqttException exception) {
            log.error("Create mqttClient and connect to broker error, config: {}", mqttConfig, exception);
            throw new RetryableException(exception);
        }
    }

    public static MqttConnectOptions defaultOptions(MQTTConfig mqttConfig) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(mqttConfig.getUsername());
        options.setPassword(mqttConfig.getPassword().toCharArray());
        options.setConnectionTimeout(0);
        options.setKeepAliveInterval(0);
        options.setAutomaticReconnect(true); // to avoid lost connection
        return options;
    }


    public static void tryReconnect(BooleanSupplier running, MqttClient client,
                                    MqttConnectOptions options, MQTTConfig mqttConfig) throws PanicException {
        // try reconnect
        int countTmp = 0; // local variable, concurrent safe
        while (running.getAsBoolean()) {
            countTmp++;
            try {
                log.info("Trying to connect the mqttServer for {} times ....", countTmp);
                log.info("Options: {}", options);
                log.info("Config: {}", mqttConfig);
                // instead of using client.reconnect(), we use client.connect()
                client.connect(options);
                client.subscribe(mqttConfig.getTopic(), 0);
                log.info("Re-connect the mqttServer success ....");
                break;
            } catch (Exception e) {
                log.error("Retry failed for {} times", countTmp, e);
                if (countTmp == ConnectorConst.MQTT_MAX_CONNECT_RETRY_COUNT) {
                    log.error("Retry failed for {} times, giving up...", countTmp);
                    throw new PanicException("Try to reconnect to mqtt server failed after %s times", String.valueOf(countTmp));
                }
                try {
                    Thread.sleep(ConnectorConst.MQTT_RECONNECT_PERIOD);
                } catch (InterruptedException ex) {
                    log.error("Wait time exception...", ex); // we don't care about the interrupt
                }
            }
        }
    }
}
