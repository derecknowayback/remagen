package com.dereckchen.remagen.utils;


import com.dereckchen.remagen.exceptions.PanicException;
import com.dereckchen.remagen.exceptions.RetryableException;
import com.dereckchen.remagen.models.MQTTConfig;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class MQTTUtilTest {

    private Map<String, String> props;
    private MQTTConfig mqttConfig;
    private MqttClient mqttClient;
    private MqttConnectOptions options;

    @Before
    public void setUp() {
        props = new HashMap<>();
        props.put("mqtt.broker", "tcp://localhost:1883");
        props.put("mqtt.clientid", "testClient");
        props.put("mqtt.username", "testUser");
        props.put("mqtt.password", "testPass");
        props.put("mqtt.topic", "testTopic");

        mqttConfig = MQTTUtil.parseConfig(props);
        mqttClient = mock(MqttClient.class);
        options = mock(MqttConnectOptions.class);
    }

    @Test
    public void parseConfig_ValidProperties_ShouldCreateMQTTConfig() {
        MQTTConfig config = MQTTUtil.parseConfig(props);
        assertNotNull(config);
        assertEquals("tcp://localhost:1883", config.getBroker());
        assertEquals("testClient", config.getClientid());
        assertEquals("testUser", config.getUsername());
        assertEquals("testPass", config.getPassword());
        assertEquals("testTopic", config.getTopic());
    }

    @Test
    public void getMqttClient_ValidConfig_ShouldCreateMqttClient() throws MqttException {
        MqttClient client = MQTTUtil.getMqttClient(mqttConfig);
        assertNotNull(client);
    }

    @Test
    public void defaultOptions_ValidConfig_ShouldCreateOptions() {
        MqttConnectOptions options = MQTTUtil.defaultOptions(mqttConfig);
        assertNotNull(options);
        assertEquals("testUser", options.getUserName());
        assertTrue(Arrays.equals("testPass".toCharArray(), options.getPassword()));
    }

    @Test
    public void tryReconnect_ValidClient_ShouldReconnectSuccessfully() throws Exception {
        BooleanSupplier running = () -> true;
        doNothing().when(mqttClient).connect(options);
        doNothing().when(mqttClient).subscribe(mqttConfig.getTopic(), 0);

        MQTTUtil.tryReconnect(running, mqttClient, options, mqttConfig);

        verify(mqttClient, times(1)).connect(options);
        verify(mqttClient, times(1)).subscribe(mqttConfig.getTopic(), 0);
    }

    @Test
    public void tryReconnect_InvalidClient_ShouldThrowPanicException() throws Exception {
        BooleanSupplier running = () -> true;
        doThrow(new MqttException(1)).when(mqttClient).connect(options);

        assertThrows(PanicException.class, () -> MQTTUtil.tryReconnect(running, mqttClient, options, mqttConfig));
    }
}
