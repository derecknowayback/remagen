package com.dereckchen.remagen.kafka.interceptor;

import com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HeaderUtilsTest {

    @Test
    public void isRecordNeedBridge_HeadersIsNull_ReturnsFalse() {
        assertFalse(HeaderUtils.isRecordNeedBridge(null));
    }

    @Test
    public void isRecordNeedBridge_HeaderIsNull_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        assertFalse(HeaderUtils.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderValueIsTrue_ReturnsTrue() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "true".getBytes());
        assertTrue(HeaderUtils.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderValueIsFalse_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "false".getBytes());
        assertFalse(HeaderUtils.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderValueIsNonBoolean_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "notABoolean".getBytes());
        assertFalse(HeaderUtils.isRecordNeedBridge(headers));
    }


    @Test
    public void isRecordNeedBridge_HeadersNull_ReturnsFalse() {
        assertFalse(HeaderUtils.isRecordNeedBridge(null));
    }

    @Test
    public void isRecordNeedBridge_HeaderNotPresent_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        assertFalse(HeaderUtils.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderPresentTrue_ReturnsTrue() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "true".getBytes());
        assertTrue(HeaderUtils.isRecordNeedBridge(headers));
    }

    @Test
    public void isRecordNeedBridge_HeaderPresentFalse_ReturnsFalse() {
        Headers headers = new RecordHeaders();
        headers.add(KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY, "false".getBytes());
        assertFalse(HeaderUtils.isRecordNeedBridge(headers));
    }

}