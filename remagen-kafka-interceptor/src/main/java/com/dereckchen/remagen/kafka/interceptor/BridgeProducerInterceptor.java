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

import static com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst.KAFKA_HEADER_BRIDGE_OPTION_KEY;
import static com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY;

@Data
@Slf4j
public class BridgeProducerInterceptor implements ProducerInterceptor<String, String> {


    private KafkaConnectManager kafkaConnectManager;

    @Override
    public void configure(Map<String, ?> map) {
        // do nothing
        log.info("KafkaInterceptor configure: {}",map);
        String host = (String)map.get("kafkaConnectManager.host");
        String port = (String)map.get("kafkaConnectManager.port");
        boolean needHttps = Boolean.parseBoolean((String) map.get("kafkaConnectManager.needHttps"));
        this.kafkaConnectManager = new KafkaConnectManager(host,port,needHttps);
    }


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        Headers headers = producerRecord.headers();
        if (!isRecordNeedBridge(headers)) {
            // do nothing if don't need bridge
            return producerRecord;
        }


        // get bridge option
        String topic = producerRecord.topic();
        BridgeOption bridgeOption = getBridgeOption(headers);
        if (bridgeOption == null || !topic.equals(bridgeOption.getKafkaTopic())) {
            log.error("BridgeOption missing or kafkaTopic conflicts: {}, record: {}", bridgeOption, producerRecord);
            throw new RuntimeException("BridgeOption missing or kafkaTopic conflicts");
        }


        // try init connector
        String connectorName = ConnectorUtils.getConnectorName(
                bridgeOption.getMqttTopic(), topic);
        ConnectorInfo connector = kafkaConnectManager.getConnector(connectorName);
        if (connector == null) {
            log.warn("connector:{} not exist", connectorName);
            kafkaConnectManager.createConnector(connectorName, bridgeOption.getProps());
        }

        return producerRecord;
    }

    public boolean isRecordNeedBridge(Headers headers) {
        if (headers == null) {
            return false;
        }
        Header header = headers.lastHeader(KAFKA_HEADER_NEED_BRIDGE_KEY);
        if (header == null) {
            return false;
        }
        return Boolean.parseBoolean(new String(header.value()));
    }

    public BridgeOption getBridgeOption(Headers headers) {
        if (headers == null) {
            return null;
        }
        Header header = headers.lastHeader(KAFKA_HEADER_BRIDGE_OPTION_KEY);
        if (header == null || header.value() == null) {
            return null;
        }
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
    }
}
