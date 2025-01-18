package com.dereckchen.remagen.utils;


import com.dereckchen.remagen.consts.ConnectorConst;
import com.dereckchen.remagen.models.BridgeConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConnectorUtilsTest {

    private Map<String, String> props;

    @Before
    public void setUp() {
        props = new HashMap<>();
    }


    @Test
    public void getConnectorName_ValidInputs_CorrectFormat() {
        String mqttTopic = "testMqttTopic";
        String kafkaTopic = "testKafkaTopic";
        String expectedConnectorName = String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopic, kafkaTopic);
        assertEquals(expectedConnectorName, ConnectorUtils.getConnectorName(mqttTopic, kafkaTopic));
    }

    @Test
    public void getConnectorName_EmptyStrings_CorrectFormat() {
        String mqttTopic = "";
        String kafkaTopic = "";
        String expectedConnectorName = String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopic, kafkaTopic);
        assertEquals(expectedConnectorName, ConnectorUtils.getConnectorName(mqttTopic, kafkaTopic));
    }

    @Test
    public void getConnectorName_NullInputs_CorrectFormat() {
        String mqttTopic = null;
        String kafkaTopic = null;
        String expectedConnectorName = String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopic, kafkaTopic);
        assertEquals(expectedConnectorName, ConnectorUtils.getConnectorName(mqttTopic, kafkaTopic));
    }

    @Test
    public void getConnectorName_MixedInputs_CorrectFormat() {
        String mqttTopic = "testMqttTopic";
        String kafkaTopic = null;
        String expectedConnectorName = String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopic, kafkaTopic);
        assertEquals(expectedConnectorName, ConnectorUtils.getConnectorName(mqttTopic, kafkaTopic));

        mqttTopic = null;
        kafkaTopic = "testKafkaTopic";
        expectedConnectorName = String.format(ConnectorConst.CONNECTOR_NAME_FORMAT, mqttTopic, kafkaTopic);
        assertEquals(expectedConnectorName, ConnectorUtils.getConnectorName(mqttTopic, kafkaTopic));
    }

    @Test
    public void parseConfig_CompleteProperties_BridgeConfigCreated() {
        props.put("kafka.host", "localhost");
        props.put("kafka.port", "9092");
        props.put("kafka.needHttps", "true");
        props.put("kafka.topic", "testTopic");
        props.put("mqtt.broker", "tcp://localhost:1883");
        props.put("mqtt.topic", "testMqttTopic");
        props.put("mqtt.username", "user");
        props.put("mqtt.password", "pass");
        props.put("mqtt.clientid", "clientId");

        BridgeConfig bridgeConfig = ConnectorUtils.parseConfig(props);

        assertEquals("localhost", bridgeConfig.getKafkaServerConfig().getHost());
        assertEquals("9092", bridgeConfig.getKafkaServerConfig().getPort());
        assertEquals(true, bridgeConfig.getKafkaServerConfig().isNeedHttps());
        assertEquals("testTopic", bridgeConfig.getKafkaServerConfig().getKafkaTopics());

        assertEquals("tcp://localhost:1883", bridgeConfig.getMqttConfig().getBroker());
        assertEquals("testMqttTopic", bridgeConfig.getMqttConfig().getTopic());
        assertEquals("user", bridgeConfig.getMqttConfig().getUsername());
        assertEquals("pass", bridgeConfig.getMqttConfig().getPassword());
        assertEquals("clientId", bridgeConfig.getMqttConfig().getClientid());
    }

    @Test
    public void parseConfig_MissingKafkaProperties_MQTTConfigOnly() {
        props.put("mqtt.broker", "tcp://localhost:1883");
        props.put("mqtt.topic", "testMqttTopic");
        props.put("mqtt.username", "user");
        props.put("mqtt.password", "pass");
        props.put("mqtt.clientid", "clientId");

        BridgeConfig bridgeConfig = ConnectorUtils.parseConfig(props);

        assertNotNull(bridgeConfig.getKafkaServerConfig());

        assertEquals("tcp://localhost:1883", bridgeConfig.getMqttConfig().getBroker());
        assertEquals("testMqttTopic", bridgeConfig.getMqttConfig().getTopic());
        assertEquals("user", bridgeConfig.getMqttConfig().getUsername());
        assertEquals("pass", bridgeConfig.getMqttConfig().getPassword());
        assertEquals("clientId", bridgeConfig.getMqttConfig().getClientid());
    }

    @Test
    public void parseConfig_MissingMQTTProperties_KafkaConfigOnly() {
        props.put("kafka.host", "localhost");
        props.put("kafka.port", "9092");
        props.put("kafka.needHttps", "true");
        props.put("kafka.topic", "testTopic");

        BridgeConfig bridgeConfig = ConnectorUtils.parseConfig(props);

        assertEquals("localhost", bridgeConfig.getKafkaServerConfig().getHost());
        assertEquals("9092", bridgeConfig.getKafkaServerConfig().getPort());
        assertEquals(true, bridgeConfig.getKafkaServerConfig().isNeedHttps());
        assertEquals("testTopic", bridgeConfig.getKafkaServerConfig().getKafkaTopics());

        assertNotNull(bridgeConfig.getMqttConfig());
    }

    @Test
    public void parseConfig_EmptyMap_NullConfigs() {
        BridgeConfig bridgeConfig = ConnectorUtils.parseConfig(props);

        assertNotNull(bridgeConfig.getKafkaServerConfig());
        assertNotNull(bridgeConfig.getMqttConfig());
    }
}
