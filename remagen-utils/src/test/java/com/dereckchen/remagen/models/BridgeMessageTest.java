package com.dereckchen.remagen.models;


import com.dereckchen.remagen.utils.JsonUtils;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BridgeMessageTest {

    private BridgeMessage bridgeMessage;

    @Before
    public void setUp() {
        // Initialize a BridgeMessage with default values
        bridgeMessage = new BridgeMessage(new TestContent("testId", "testContent"), 1, false);
    }

    @Test
    public void transferToMqttMessage_DefaultValues_CorrectMqttMessage() {
        MqttMessage mqttMessage = bridgeMessage.transferToMqttMessage();

        assertEquals(1, mqttMessage.getQos());
        assertFalse(mqttMessage.isRetained());
        assertTrue(mqttMessage.getPayload().length > 0);
    }

    @Test
    public void transferToMqttMessage_RetainedTrue_CorrectMqttMessage() {
        bridgeMessage = new BridgeMessage(new TestContent("testId", "testContent"), 1, true);
        MqttMessage mqttMessage = bridgeMessage.transferToMqttMessage();

        assertEquals(1, mqttMessage.getQos());
        assertTrue(mqttMessage.isRetained());
        assertTrue(mqttMessage.getPayload().length > 0);
    }

    @Test
    public void transferToMqttMessage_QosZero_CorrectMqttMessage() {
        bridgeMessage = new BridgeMessage(new TestContent("testId", "testContent"), 0, false);
        MqttMessage mqttMessage = bridgeMessage.transferToMqttMessage();

        assertEquals(0, mqttMessage.getQos());
        assertFalse(mqttMessage.isRetained());
        assertTrue(mqttMessage.getPayload().length > 0);
    }

    @Test
    public void transferToMqttMessage_QosTwo_CorrectMqttMessage() {
        bridgeMessage = new BridgeMessage(new TestContent("testId", "testContent"), 2, true);
        MqttMessage mqttMessage = bridgeMessage.transferToMqttMessage();

        assertEquals(2, mqttMessage.getQos());
        assertTrue(mqttMessage.isRetained());
        assertTrue(mqttMessage.getPayload().length > 0);
    }

    // Test content class for serialization
    private static class TestContent implements IBridgeMessageContent {
        private String messageId;
        private String content;

        public TestContent(String messageId, String content) {
            this.messageId = messageId;
            this.content = content;
        }

        @Override
        public String getMessageId() {
            return messageId;
        }

        @Override
        public String serializeToJsonStr() {
            return JsonUtils.toJsonString(this);
        }
    }
}
