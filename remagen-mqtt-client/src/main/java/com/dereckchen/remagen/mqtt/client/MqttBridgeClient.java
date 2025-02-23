package com.dereckchen.remagen.mqtt.client;

import com.dereckchen.remagen.kakfa.restful.client.KafkaConnectManager;
import com.dereckchen.remagen.models.BridgeMessage;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.models.ConnectorInfoV2;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.utils.ConnectorUtils;
import com.dereckchen.remagen.utils.JsonUtils;
import com.dereckchen.remagen.utils.MetricsUtils;
import io.prometheus.client.Histogram;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.dereckchen.remagen.utils.MetricsUtils.getLocalIp;


@Getter
@Setter
@Slf4j
public class MqttBridgeClient extends MqttClient {

    private KafkaConnectManager kafkaConnectManager;

    private Histogram arriveAtMqttClientHistogram = MetricsUtils.getHistogram("arrive_at_mqtt_client", "host");


    public MqttBridgeClient(String serverURI, String clientId, KafkaServerConfig option) throws MqttException {
        super(serverURI, clientId);
        this.kafkaConnectManager = new KafkaConnectManager(option.getHost(), option.getPort(), option.isNeedHttps());
        setCallback(nopCallBack());
    }

    public MqttBridgeClient(String serverURI, String clientId, MqttClientPersistence persistence, KafkaServerConfig option) throws MqttException {
        super(serverURI, clientId, persistence);
        this.kafkaConnectManager = new KafkaConnectManager(option.getHost(), option.getPort(), option.isNeedHttps());
        setCallback(nopCallBack());
    }

    public MqttBridgeClient(String serverURI, String clientId, MqttClientPersistence persistence, ScheduledExecutorService executorService, KafkaServerConfig option) throws MqttException {
        super(serverURI, clientId, persistence, executorService);
        this.kafkaConnectManager = new KafkaConnectManager(option.getHost(), option.getPort(), option.isNeedHttps());
        setCallback(nopCallBack());
    }

    private static MqttCallback nopCallBack() {
        return new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        };
    }

    @Override
    public void setCallback(MqttCallback callback) {
        MqttCallback extended = new MqttCallback() {

            @Override
            public void connectionLost(Throwable cause) {
                callback.connectionLost(cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                MqttMessage mqttMessage = rewriteMqttMessage(message);
                callback.messageArrived(topic, mqttMessage);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        };
        super.setCallback(extended);
    }

    private MqttMessage rewriteMqttMessage(MqttMessage message) {
        BridgeMessage bridgeMessage = JsonUtils.fromJson(message.getPayload(), BridgeMessage.class);
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        bridgeMessage.setMqttEndTime(now);
        if (bridgeMessage.getPubFromSink() != null) {
            log.info("收到Sink消息,耗时:{}ms", Duration.between(bridgeMessage.getPubFromSink(), now).toMillis());
            MetricsUtils.observeRequestLatency(arriveAtMqttClientHistogram,
                    Duration.between(bridgeMessage.getPubFromSink(), now).toMillis(), getLocalIp());
        }
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setId(mqttMessage.getId());
        if (bridgeMessage.getContent() != null) {
            mqttMessage.setPayload(bridgeMessage.getContent().getBytes());
        }
        mqttMessage.setQos(bridgeMessage.getQos());
        mqttMessage.setRetained(bridgeMessage.isRetained());
        return mqttMessage;
    }


    /**
     * Subscribes to a list of MQTT topics and creates Kafka connectors for them.
     *
     * @param bridgeOptions    An array of BridgeOption objects containing the MQTT topics and Kafka connector configurations.
     * @param qos              An array of integers representing the QoS levels for each topic.
     * @param messageListeners An array of IMqttMessageListener objects that will receive messages for each topic.
     * @throws MqttException If an error occurs while subscribing to the topics or creating the connectors.
     */
    public void subscribe(BridgeOption[] bridgeOptions, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException {
        // Retrieve a list of all existing connectors from the Kafka Connect Manager.
        List<String> allConnectors = kafkaConnectManager.getAllConnectors();

        // Iterate over the array of BridgeOption objects.
        for (int i = 0; i < bridgeOptions.length; i++) {
            // Generate a unique connector name based on the MQTT topic and Kafka topic.
            String connectorName = ConnectorUtils.getConnectorName(bridgeOptions[i].getMqttTopic(), bridgeOptions[i].getKafkaTopic());

            // Check if the connector already exists in the list of all connectors.
            if (!allConnectors.contains(connectorName)) {
                // If the connector does not exist, log a warning message.
                log.warn("connector:{} not exist", connectorName);

                // Create a new Kafka connector with the generated connector name and the properties from the BridgeOption object.
                kafkaConnectManager.createConnector(connectorName, bridgeOptions[i].getProps());
            }
        }

        // Convert the array of BridgeOption objects to an array of strings containing the MQTT topics.
        String[] topicFilters = Arrays.stream(bridgeOptions).map(BridgeOption::getMqttTopic).toArray(String[]::new);

        // Call the superclass's subscribe method to subscribe to the MQTT topics with the specified QoS levels and message listeners.
        if (messageListeners != null) {
            for (int i = 0; i < messageListeners.length; i++) {
                IMqttMessageListener originListener = messageListeners[i];
                messageListeners[i] = (topic, message) -> {
                    MqttMessage mqttMessage = rewriteMqttMessage(message);
                    originListener.messageArrived(topic, mqttMessage);
                };
            }
        }
        super.subscribe(topicFilters, qos, messageListeners);
    }


    /**
     * Publishes a message to the specified MQTT topic and ensures that the corresponding Kafka connector exists.
     * If the connector does not exist, it is created with the provided BridgeOption.
     *
     * @param topic         The MQTT topic to which the message will be published.
     * @param bridgeMessage The message to be published, encapsulated as a BridgeMessage.
     * @param bridgeOption  The configuration for the Kafka connector associated with the topic.
     * @throws MqttException If there is an issue with the MQTT publish operation or if the topic does not match the expected MQTT topic in the BridgeOption.
     */
    public void publish(String topic, BridgeMessage bridgeMessage, BridgeOption bridgeOption) throws MqttException {
        // Pre-judgment: Does the connector exist?
        if (!topic.equals(bridgeOption.getMqttTopic())) {
            // If the topic does not match, throw an exception
            throw new RuntimeException("Kafka topic conflicts!");
        }

        // Convert the BridgeMessage to an MqttMessage
        bridgeMessage.setMqttPubTime(LocalDateTime.now());
        MqttMessage message = bridgeMessage.transferToMqttMessage();

        // Generate a unique connector name based on the MQTT topic and Kafka topic
        String connectorName = ConnectorUtils.getConnectorName(bridgeOption.getMqttTopic(), bridgeOption.getKafkaTopic());
        // Retrieve the connector information from the Kafka Connect Manager
        ConnectorInfoV2 connector = kafkaConnectManager.getConnector(connectorName);
        // If the connector does not exist
        if (connector.getErrorCode() != null) {
            log.warn("connector:{} not exist", connectorName);
            // Create a new Kafka connector with the generated connector name and the properties from the BridgeOption object
            connector = kafkaConnectManager.createConnector(connectorName, bridgeOption.getProps());
            if (connector.getErrorCode() != null) {
                log.error("create connector error:{}", connector.getMessage());
                throw new RuntimeException("create connector error:" + connector.getMessage());
            }
        }

        // Call the superclass's publish method to publish the MQTT message
        super.publish(topic, message);
    }


    public void unsubscribe(BridgeOption bridgeOptions) throws MqttException {
        unsubscribe(new BridgeOption[]{bridgeOptions});
    }


    /**
     * Unsubscribes from a list of MQTT topics and deletes the corresponding Kafka connectors.
     *
     * @param bridgeOptions An array of BridgeOption objects containing the MQTT topics and Kafka connector configurations.
     * @throws MqttException If an error occurs while unsubscribing from the topics or deleting the connectors.
     */
    public void unsubscribe(BridgeOption[] bridgeOptions) throws MqttException {
        // 删除connector
        for (BridgeOption bridgeOption : bridgeOptions) {
            // Generate a unique connector name based on the MQTT topic and Kafka topic
            String connectorName = ConnectorUtils.getConnectorName(bridgeOption.getMqttTopic(), bridgeOption.getKafkaTopic());
            // Delete the Kafka connector with the generated connector name
            kafkaConnectManager.deleteConnector(connectorName);
        }

        // call super
        // Convert the array of BridgeOption objects to an array of strings containing the MQTT topics
        String[] topicFilters = Arrays.stream(bridgeOptions).map(BridgeOption::getMqttTopic).toArray(String[]::new);
        // Call the superclass's unsubscribe method to unsubscribe from the MQTT topics
        super.unsubscribe(topicFilters);
        // Log a success message
        log.info("unsubscribe success");
    }

}
