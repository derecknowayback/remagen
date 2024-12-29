package com.dereckchen.remagen.kafka.connector.source;


import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.utils.KafkaUtils;
import com.dereckchen.remagen.utils.MQTTUtil;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaUtils.class, MQTTUtil.class, System.class, MqttSourceTask.class})
public class MqttSourceTaskTest {

    @Mock
    private OffsetStorageReader offsetStorageReader;

    @Mock
    private MqttClient mqttClient;


    private MqttSourceTask mqttSourceTask;

    private Map<String, String> properties;

    @Before
    public void setUp() throws MqttException {
        mqttSourceTask = spy(new MqttSourceTask());

        properties = new HashMap<>();
        properties.put("kafka.topics", "testTopic");
        properties.put("mqtt.broker", "tcp://localhost:1883");
        properties.put("mqtt.topic", "testMqttTopic");
        properties.put("mqtt.username", "user");
        properties.put("mqtt.password", "pass");
        properties.put("mqtt.clientid", "clientId");

        mockStatic(MQTTUtil.class);
        mockStatic(KafkaUtils.class);

        when(MQTTUtil.getMqttClient(any())).thenReturn(mqttClient);

        KafkaServerConfig kafkaServerConfig = KafkaServerConfig.builder()
                .kafkaTopics("cc").build();
        when(KafkaUtils.parseConfig(any())).thenReturn(kafkaServerConfig);


        when(mqttClient.isConnected()).thenReturn(true);
        doNothing().when(mqttClient).connect(any());

        SourceTaskContext ctx = mock(SourceTaskContext.class);
        doReturn(offsetStorageReader).when(ctx).offsetStorageReader();
        mqttSourceTask.initialize(ctx);
    }

    @Test
    public void version_WithEnvVariable_ReturnsEnvVersion() {
        mockStatic(System.class);
        when(System.getenv("REMAGEN.VERSION")).thenReturn("1.0.0");
        assertEquals("1.0.0", mqttSourceTask.version());
    }

    @Test
    public void version_WithoutEnvVariable_ReturnsDefaultVersion() {
        assertEquals("0.1-BETA", mqttSourceTask.version());
    }

    @Test
    public void testGetOffset() {
        doReturn(Collections.singletonMap("timestamp", "2023-01-01 00:00:00"))
                .when(offsetStorageReader).offset(anyMap());
        String offset = mqttSourceTask.getOffset("11", "22");
        assertEquals("2023-01-01 00:00:00", offset);
    }


    @Test
    public void getKafkaTopic_SingleTopic_ReturnsTopic() {
        String kafkaTopics = "singleTopic";
        String result = mqttSourceTask.getKafkaTopic(kafkaTopics);
        assertEquals("singleTopic", result);
    }

    @Test
    public void getKafkaTopic_MultipleTopics_ReturnsFirstTopic() {
        String kafkaTopics = "firstTopic,secondTopic,thirdTopic";
        String result = mqttSourceTask.getKafkaTopic(kafkaTopics);
        assertEquals("firstTopic", result);
    }


}
