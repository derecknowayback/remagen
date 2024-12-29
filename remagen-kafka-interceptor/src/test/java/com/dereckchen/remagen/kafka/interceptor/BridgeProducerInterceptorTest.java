package com.dereckchen.remagen.kafka.interceptor;


import com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst;
import com.dereckchen.remagen.models.BridgeOption;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;

public class BridgeProducerInterceptorTest {

    private BridgeProducerInterceptor interceptor;

    @Before
    public void setUp() {
        interceptor = new BridgeProducerInterceptor();
    }

    @Test
    public void isRecordNeedBridge_HeadersIsNull_ReturnsFalse() {
        assertFalse(interceptor.isRecordNeedBridge(null));
    }

    @Test
    public void isRecordNeedBridge_HeaderIsNull_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        assertFalse(interceptor.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderValueIsTrue_ReturnsTrue() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "true".getBytes());
        assertTrue(interceptor.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderValueIsFalse_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "false".getBytes());
        assertFalse(interceptor.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderValueIsNonBoolean_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "notABoolean".getBytes());
        assertFalse(interceptor.isRecordNeedBridge(headers));
    }


    @Test
    public void isRecordNeedBridge_HeadersNull_ReturnsFalse() {
        assertFalse(interceptor.isRecordNeedBridge(null));
    }

    @Test
    public void isRecordNeedBridge_HeaderNotPresent_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        assertFalse(interceptor.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderPresentTrue_ReturnsTrue() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "true".getBytes());
        assertTrue(interceptor.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderPresentFalse_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "false".getBytes());
        assertFalse(interceptor.isRecordNeedBridge(headers));
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
