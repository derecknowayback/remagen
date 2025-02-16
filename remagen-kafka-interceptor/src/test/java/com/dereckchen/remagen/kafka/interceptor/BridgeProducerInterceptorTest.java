package com.dereckchen.remagen.kafka.interceptor;


import com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst;
import com.dereckchen.remagen.models.BridgeOption;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BridgeProducerInterceptorTest {

    private BridgeProducerInterceptor interceptor;

    @Before
    public void setUp() {
        interceptor = new BridgeProducerInterceptor();
    }

    @Test
    public void getBridgeOption_HeadersNull_ReturnsNull() {
        assertNull(interceptor.getBridgeOption(null));
    }

    @Test
    public void getBridgeOption_HeaderNotPresent_ReturnsNull() {
        Headers headers = new RecordHeaders();
        assertNull(interceptor.getBridgeOption(headers));
    }

    @Test
    public void getBridgeOption_HeaderPresentValidJson_ReturnsBridgeOption() {
        Headers headers = new RecordHeaders();
        String json = "{\"kafkaTopic\":\"testTopic\",\"mqttTopic\":\"testMqttTopic\",\"props\":{\"key\":\"value\"}}";
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_BRIDGE_OPTION_KEY, json.getBytes());
        BridgeOption bridgeOption = interceptor.getBridgeOption(headers);
        assertNotNull(bridgeOption);
        assertEquals("testTopic", bridgeOption.getKafkaTopic());
        assertEquals("testMqttTopic", bridgeOption.getMqttTopic());
        assertEquals("value", bridgeOption.getProps().get("key"));
    }

    @Test
    public void getBridgeOption_HeaderPresentInvalidJson_ReturnsNull() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_BRIDGE_OPTION_KEY, "invalidJson".getBytes());
        assertThrows(RuntimeException.class, () -> interceptor.getBridgeOption(headers));
    }
}
