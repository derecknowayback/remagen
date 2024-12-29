package com.dereckchen.remagen.kafka.interceptor;

import com.dereckchen.remagen.kakfa.restful.client.KafkaConnectManager;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.utils.ConnectorUtils;
import com.dereckchen.remagen.utils.JsonUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

import java.util.Map;

import static com.dereckchen.remagen.consts.ConnectorConst.INTERCEPTOR_PROP_HOST;
import static com.dereckchen.remagen.consts.ConnectorConst.INTERCEPTOR_PROP_PORT;
import static com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst.KAFKA_HEADER_BRIDGE_OPTION_KEY;
import static com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY;

@Data
@Slf4j
public class BridgeProducerInterceptor implements ProducerInterceptor<String, String> {


    private KafkaConnectManager kafkaConnectManager;

    @Override
    public void configure(Map<String, ?> map) {
        // do nothing
        log.info("KafkaInterceptor configure: {}", map);
        String host = (String) map.get(INTERCEPTOR_PROP_HOST);
        String port = (String) map.get(INTERCEPTOR_PROP_PORT);
        boolean needHttps = Boolean.parseBoolean((String) map.get("kafkaConnectManager.needHttps"));
        this.kafkaConnectManager = new KafkaConnectManager(host, port, needHttps);
    }


    /**
     * Intercepts and processes records before they are sent to Kafka.
     * If the record does not need to be bridged, it is returned unchanged.
     * If the record needs to be bridged, it is checked for a valid BridgeOption.
     * If the BridgeOption is valid, the connector is initialized or created if it does not exist.
     *
     * @param producerRecord The record to be sent to Kafka.
     * @return The processed record.
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // Get the headers of the record
        Headers headers = producerRecord.headers();
        // Check if the record needs to be bridged
        if (!isRecordNeedBridge(headers)) {
            // If the record does not need to be bridged, return it unchanged
            return producerRecord;
        }

        // Get the topic of the record
        String topic = producerRecord.topic();
        // Get the BridgeOption from the headers
        BridgeOption bridgeOption = getBridgeOption(headers);
        // Check if the BridgeOption is valid
        if (bridgeOption == null || !topic.equals(bridgeOption.getKafkaTopic())) {
            // If the BridgeOption is missing or the kafkaTopic conflicts, log an error and throw an exception
            log.error("BridgeOption missing or kafkaTopic conflicts: {}, record: {}", bridgeOption, producerRecord);
            throw new RuntimeException("BridgeOption missing or kafkaTopic conflicts");
        }

        // Try to initialize the connector
        String connectorName = ConnectorUtils.getConnectorName(
                bridgeOption.getMqttTopic(), topic);
        // Get the connector from the Kafka Connect manager
        ConnectorInfo connector = kafkaConnectManager.getConnector(connectorName);
        // If the connector does not exist
        if (connector == null) {
            // Log a warning
            log.warn("connector:{} not exist", connectorName);
            // Create the connector with the properties from the BridgeOption
            kafkaConnectManager.createConnector(connectorName, bridgeOption.getProps());
        }

        // Return the processed record
        return producerRecord;
    }


    /**
     * Determines whether a record needs to be bridged based on the presence and value of a specific header.
     *
     * @param headers The headers of the Kafka record.
     * @return True if the record needs to be bridged, false otherwise.
     */
    public boolean isRecordNeedBridge(Headers headers) {
        // Check if headers are null, return false if they are
        if (headers == null) {
            return false;
        }
        // Retrieve the last header with the key KAFKA_HEADER_NEED_BRIDGE_KEY
        Header header = headers.lastHeader(KAFKA_HEADER_NEED_BRIDGE_KEY);
        // If the header is null, return false
        if (header == null) {
            return false;
        }
        // Parse the header value as a boolean and return the result
        return Boolean.parseBoolean(new String(header.value()));
    }

    /**
     * Retrieves the BridgeOption from the provided Kafka message headers.
     *
     * @param headers The Kafka message headers.
     * @return The BridgeOption if found, otherwise null.
     */
    public BridgeOption getBridgeOption(Headers headers) {
        // Check if headers are null, return null if they are
        if (headers == null) {
            return null;
        }
        // Retrieve the last header with the key KAFKA_HEADER_BRIDGE_OPTION_KEY
        Header header = headers.lastHeader(KAFKA_HEADER_BRIDGE_OPTION_KEY);
        // If the header is null or its value is null, return null
        if (header == null || header.value() == null) {
            return null;
        }
        // Deserialize the header value into a BridgeOption object using JsonUtils
        return JsonUtils.fromJson(header.value(), BridgeOption.class);
    }


    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            log.error("KafkaInterceptor onAcknowledgement error. RecordMetadata: {}", recordMetadata, e);
        }
    }

    @Override
    public void close() {
        // do nothing
        log.info("KafkaInterceptor close...");
    }
}
