package com.dereck.remagen.mqtt.client;

import com.dereck.remagen.mqtt.models.BridgeOption;
import com.dereck.remagen.mqtt.models.KafkaServerConfig;
import com.dereck.remagen.mqtt.util.ConnectorUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.eclipse.paho.client.mqttv3.*;

import java.util.concurrent.ScheduledExecutorService;


@Getter
@Setter
@Slf4j
public class MqttClientV2 extends MqttClient {

    private KafkaConnectManager kafkaConnectManager;


    public MqttClientV2(String serverURI, String clientId) throws MqttException {
        super(serverURI, clientId);
    }


    public MqttClientV2(String serverURI, String clientId, KafkaServerConfig option) throws MqttException {
        super(serverURI, clientId);
        this.kafkaConnectManager = new KafkaConnectManager(option.getHost(), option.getPort(), option.isNeedHttps());
    }

    public MqttClientV2(String serverURI, String clientId, MqttClientPersistence persistence) throws MqttException {
        super(serverURI, clientId, persistence);
    }

    public MqttClientV2(String serverURI, String clientId, MqttClientPersistence persistence, KafkaServerConfig option) throws MqttException {
        super(serverURI, clientId, persistence);
    }

    public MqttClientV2(String serverURI, String clientId, MqttClientPersistence persistence, ScheduledExecutorService executorService) throws MqttException {
        super(serverURI, clientId, persistence, executorService);
    }

    public MqttClientV2(String serverURI, String clientId, MqttClientPersistence persistence, ScheduledExecutorService executorService, KafkaServerConfig option) throws MqttException {
        super(serverURI, clientId, persistence, executorService);
    }

    @Override
    public void subscribe(String[] topicFilters, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException {
        super.subscribe(topicFilters, qos, messageListeners);
        // todo
    }


    // 重写publish方法
    public void publish(String topic, byte[] payload, int qos, boolean retained, BridgeOption bridgeOption) throws MqttException {
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);
        message.setRetained(retained);
        this.publish(topic, message, bridgeOption);
    }

    public void publish(String topic, MqttMessage message, BridgeOption bridgeOption) throws MqttException {
        // 前置判断: connector是否存在?
        String connectorName = ConnectorUtils.getConnectorName(bridgeOption.getMqttTopic(), bridgeOption.getKafkaTopic());
        ConnectorInfo connector = kafkaConnectManager.getConnector(connectorName);
        if (connector == null) {
            log.warn("connector:{} not exist", connectorName);
            kafkaConnectManager.createConnector(connectorName, bridgeOption.getProps());
        }
        super.publish(topic, message);
    }
}
