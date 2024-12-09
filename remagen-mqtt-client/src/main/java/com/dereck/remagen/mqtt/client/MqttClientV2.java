package com.dereck.remagen.mqtt.client;

import com.dereck.remagen.mqtt.models.BridgeOption;
import org.eclipse.paho.client.mqttv3.*;

import java.util.concurrent.ScheduledExecutorService;

public class MqttClientV2 extends MqttClient{



    public MqttClientV2(String serverURI, String clientId) throws MqttException {
        super(serverURI, clientId);
    }

    public MqttClientV2(String serverURI, String clientId, MqttClientPersistence persistence) throws MqttException {
        super(serverURI, clientId, persistence);
    }

    public MqttClientV2(String serverURI, String clientId, MqttClientPersistence persistence, ScheduledExecutorService executorService) throws MqttException {
        super(serverURI, clientId, persistence, executorService);
    }

    @Override
    public void subscribe(String[] topicFilters, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException {

    }



    // 重写publish方法
    public void publish (String topic, byte[] payload, int qos, boolean retained, BridgeOption bridgeOption) throws MqttException {
        // todo
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);
        message.setRetained(retained);
        this.publish(topic, message, bridgeOption);
    }

    public void publish (String topic, MqttMessage message ,BridgeOption bridgeOption) throws MqttException {
        // todo


    }








}
