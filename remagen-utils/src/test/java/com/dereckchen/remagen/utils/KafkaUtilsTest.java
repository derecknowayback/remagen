package com.dereckchen.remagen.utils;


import com.dereckchen.remagen.models.KafkaServerConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class KafkaUtilsTest {

    private Map<String, String> properties;

    @Before
    public void setUp() {
        properties = new HashMap<>();
    }

    @Test
    public void parseConfig_AllPropertiesPresent_ShouldReturnCorrectConfig() {
        properties.put("kafka.host", "localhost");
        properties.put("kafka.port", "9092");
        properties.put("kafka.needHttps", "true");
        properties.put("kafka.topic", "testTopic");

        KafkaServerConfig config = KafkaUtils.parseConfig(properties);

        assertEquals("localhost", config.getHost());
        assertEquals("9092", config.getPort());
        assertTrue(config.isNeedHttps());
        assertEquals("testTopic", config.getKafkaTopics());
    }

    @Test
    public void parseConfig_MissingNeedHttps_ShouldDefaultToFalse() {
        properties.put("kafka.host", "localhost");
        properties.put("kafka.port", "9092");
        properties.put("kafka.topic", "testTopic");

        KafkaServerConfig config = KafkaUtils.parseConfig(properties);

        assertEquals("localhost", config.getHost());
        assertEquals("9092", config.getPort());
        assertFalse(config.isNeedHttps());
        assertEquals("testTopic", config.getKafkaTopics());
    }

    @Test
    public void parseConfig_MissingKafkaTopics_ShouldDefaultToEmptyString() {
        properties.put("kafka.host", "localhost");
        properties.put("kafka.port", "9092");
        properties.put("kafka.needHttps", "true");

        KafkaServerConfig config = KafkaUtils.parseConfig(properties);

        assertEquals("localhost", config.getHost());
        assertEquals("9092", config.getPort());
        assertTrue(config.isNeedHttps());
        assertEquals("", config.getKafkaTopics());
    }

    @Test
    public void getPartition_ValidTopics_ShouldReturnCorrectMap() {
        Map<String, String> partition = KafkaUtils.getPartition("kafkaTopic", "mqttTopic");

        assertEquals("kafkaTopic", partition.get("kafkaTopic"));
        assertEquals("mqttTopic", partition.get("mqttTopic"));
    }

    @Test
    public void getPartition_EmptyTopics_ShouldReturnEmptyValues() {
        Map<String, String> partition = KafkaUtils.getPartition("", "");

        assertEquals("", partition.get("kafkaTopic"));
        assertEquals("", partition.get("mqttTopic"));
    }

    @Test
    public void getPartition_NullTopics_ShouldReturnNullValues() {
        Map<String, String> partition = KafkaUtils.getPartition(null, null);

        assertEquals(null, partition.get("kafkaTopic"));
        assertEquals(null, partition.get("mqttTopic"));
    }
}
