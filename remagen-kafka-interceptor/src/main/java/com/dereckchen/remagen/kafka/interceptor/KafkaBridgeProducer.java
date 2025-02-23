package com.dereckchen.remagen.kafka.interceptor;

import com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst;
import com.dereckchen.remagen.models.BridgeOption;
import com.dereckchen.remagen.utils.JsonUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaBridgeProducer<K, V> extends KafkaProducer<K, V> {
    public KafkaBridgeProducer(Map<String, Object> configs) {
        super(configs);
    }

    public KafkaBridgeProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }

    public KafkaBridgeProducer(Properties properties) {
        super(properties);
    }

    public KafkaBridgeProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(properties, keySerializer, valueSerializer);
    }


    public Future<RecordMetadata> send(ProducerRecord<K, V> record, BridgeOption bridgeOption) {
        return send(record, null, null, bridgeOption);
    }


    public Future<RecordMetadata> send(ProducerRecord<K, V> record, String msgId, Callback callback, BridgeOption bridgeOption) {
        Headers headers = record.headers();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "true".getBytes());
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_BRIDGE_OPTION_KEY, JsonUtils.toJsonBytes(bridgeOption));
        if (msgId != null) {
            headers.add(KafkaInterceptorConst.KAFKA_HEADER_MESSAGE_ID, msgId.getBytes());
        }
        return super.send(record, callback);
    }


}
