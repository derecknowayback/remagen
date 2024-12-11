package com.dereck.remagen.mqtt.client;

import com.dereck.remagen.mqtt.models.BridgeOption;
import com.dereck.remagen.mqtt.models.KafkaServerConfig;
import com.dereck.remagen.mqtt.util.ConnectorUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.eclipse.paho.client.mqttv3.*;

import java.util.Arrays;
import java.util.List;
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



    public void subscribe(BridgeOption[] bridgeOptions,int[] qos, IMqttMessageListener[] messageListeners) throws MqttException {
        // 建好connector
        List<String> allConnectors = kafkaConnectManager.getAllConnectors();
        for (int i = 0; i < bridgeOptions.length; i++) {
            String connectorName = ConnectorUtils.getConnectorName(bridgeOptions[i].getMqttTopic(), bridgeOptions[i].getKafkaTopic());
            if (!allConnectors.contains(connectorName)) {
                log.warn("connector:{} not exist", connectorName);
                kafkaConnectManager.createConnector(connectorName, bridgeOptions[i].getProps());
            }
        }

        // call super
        String[] topicFilters = (String[]) Arrays.stream(bridgeOptions).map(BridgeOption::getMqttTopic).toArray();
        super.subscribe(topicFilters,qos,messageListeners);
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
        if (!topic.equals(bridgeOption.getMqttTopic())) {
            throw new RuntimeException(); // todo 抛出异常
        }

        String connectorName = ConnectorUtils.getConnectorName(bridgeOption.getMqttTopic(), bridgeOption.getKafkaTopic());
        ConnectorInfo connector = kafkaConnectManager.getConnector(connectorName);
        if (connector == null) {
            log.warn("connector:{} not exist", connectorName);
            kafkaConnectManager.createConnector(connectorName, bridgeOption.getProps());
        }

        super.publish(topic, message);
        log.info("publish success");
    }

    public void unsubscribe(BridgeOption[] bridgeOptions) throws MqttException {
        // 删除connector
        for (BridgeOption bridgeOption : bridgeOptions) {
            String connectorName = ConnectorUtils.getConnectorName(bridgeOption.getMqttTopic(), bridgeOption.getKafkaTopic());
            kafkaConnectManager.deleteConnector(connectorName);
        }

        // call super
        String[] topicFilters = (String[]) Arrays.stream(bridgeOptions).map(BridgeOption::getMqttTopic).toArray();
        super.unsubscribe(topicFilters);
        log.info("unsubscribe success");
    }
}
