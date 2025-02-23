package com.dereckchen.remagen.kafka.connector.sink;


import com.dereckchen.remagen.consts.ConnectorConst;
import com.dereckchen.remagen.exceptions.PanicException;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.models.MQTTConfig;
import com.dereckchen.remagen.utils.JsonUtils;
import com.dereckchen.remagen.utils.KafkaUtils;
import com.dereckchen.remagen.utils.MQTTUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.dereckchen.remagen.consts.ConnectorConst.VERSION_ENV_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest({System.class, MqttSinkTask.class, JsonUtils.class, KafkaUtils.class, MQTTUtils.class})
public class MqttSinkTaskTest {

    private MqttSinkTask mqttSinkTask;

    @Mock
    private SinkRecord sinkRecord;

    @Mock
    private MqttClient mqttClient;

    @Before
    public void setUp() {
        mqttSinkTask = spy(new MqttSinkTask());
        mockStatic(JsonUtils.class);
        mockStatic(KafkaUtils.class);
        mockStatic(MQTTUtils.class);

        when(MQTTUtils.getMqttClient(null)).thenReturn(mqttClient);
        when(MQTTUtils.getMqttClient(any())).thenReturn(mqttClient);

        when(MQTTUtils.defaultOptions(null)).thenReturn(new MqttConnectOptions());
        when(MQTTUtils.defaultOptions(any())).thenReturn(new MqttConnectOptions());

        MQTTConfig mqttConfig = MQTTConfig.builder().topic("ss").build();
        when(MQTTUtils.parseConfig(null)).thenReturn(mqttConfig);
        when(MQTTUtils.parseConfig(any())).thenReturn(mqttConfig);

        KafkaServerConfig kafkaServerConfig = KafkaServerConfig.builder().kafkaTopics("").build();
        when(KafkaUtils.parseConfig(null)).thenReturn(kafkaServerConfig);
        when(KafkaUtils.parseConfig(any())).thenReturn(kafkaServerConfig);
        mqttSinkTask.start(new HashMap<>());
    }

    @Test
    public void version_EnvironmentVariableSet_ReturnsEnvVersion() {
        mockStatic(System.class);
        when(System.getenv(VERSION_ENV_KEY)).thenReturn("1.2.3");

        // Call the method under test
        String version = mqttSinkTask.version();

        // Verify the result
        assertEquals("1.2.3", version);
    }

    @Test
    public void version_EnvironmentVariableNotSet_ReturnsDefaultVersion() {
        // Call the method under test
        String version = mqttSinkTask.version();

        // Verify the result
        assertEquals(ConnectorConst.DEFAULT_VERSION, version);
    }


    @Test
    public void testStart() {
        // Create a mock MqttSinkTask
        MqttSinkTask task = spy(new MqttSinkTask());

        // Create a map of properties
        Map<String, String> props = new HashMap<>();
        props.put("key1", "value1");
        props.put("key2", "value2");

        // Call the start method
        task.start(props);


        // Verify that the parseConfig method was called
        verify(task, Mockito.times(1)).parseConfig(props);
    }

    @Test
    public void put_TaskNotRunning_DoesNothing() throws PanicException {
        // Act
        mqttSinkTask.put(Collections.emptyList());

        // Assert
        verify(mqttSinkTask, never()).monitorRecords(any());
    }

    @Test
    public void put_EmptyRecords_DoesNothing() throws PanicException {
        mqttSinkTask.start(new HashMap<>());
        // Act
        mqttSinkTask.put(Collections.emptyList());

        // Assert
        verify(mqttSinkTask, never()).monitorRecords(any());
    }


}
