package com.dereckchen.remagen.kafka.connector.sink;


import com.dereckchen.remagen.consts.ConnectorConst;
import com.dereckchen.remagen.exceptions.PanicException;
import com.dereckchen.remagen.exceptions.RetryableException;
import com.dereckchen.remagen.models.KafkaServerConfig;
import com.dereckchen.remagen.utils.JsonUtils;
import com.dereckchen.remagen.utils.KafkaUtils;
import com.dereckchen.remagen.utils.MQTTUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import com.dereckchen.remagen.models.MQTTConfig;

import static com.dereckchen.remagen.consts.ConnectorConst.VERSION_ENV_KEY;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;


@RunWith(PowerMockRunner.class)
@PrepareForTest({System.class, MqttSinkTask.class, JsonUtils.class, KafkaUtils.class, MQTTUtil.class})
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
        mockStatic(MQTTUtil.class);

        when(MQTTUtil.getMqttClient(null)).thenReturn(mqttClient);
        when(MQTTUtil.getMqttClient(any())).thenReturn(mqttClient);

        when(MQTTUtil.defaultOptions(null)).thenReturn(new MqttConnectOptions());
        when(MQTTUtil.defaultOptions(any())).thenReturn(new MqttConnectOptions());

        MQTTConfig mqttConfig = MQTTConfig.builder().topic("ss").build();
        when(MQTTUtil.parseConfig(null)).thenReturn(mqttConfig);
        when(MQTTUtil.parseConfig(any())).thenReturn(mqttConfig);

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
        verify(mqttSinkTask, never()).sendMQTT(any());
    }

    @Test
    public void put_EmptyRecords_DoesNothing() throws PanicException {
        mqttSinkTask.start(new HashMap<>());
        // Act
        mqttSinkTask.put(Collections.emptyList());

        // Assert
        verify(mqttSinkTask, never()).monitorRecords(any());
        verify(mqttSinkTask, never()).sendMQTT(any());
    }

    @Test
    public void sendMQTT_MqttClientNotInitialized_InitializesAndPublishes() throws Exception {
        when(sinkRecord.value()).thenReturn(new Object());
        when(JsonUtils.toJsonBytes(any())).thenReturn(new byte[]{});
        when(mqttClient.isConnected()).thenReturn(true);
        doThrow(MqttException.class).when(mqttClient).publish(anyString(), any());

        assertThrows(RetryableException.class, () -> mqttSinkTask.sendMQTT(Collections.singletonList(sinkRecord)));
        verify(mqttClient, times(1)).publish(any(), any());
    }

    @Test
    public void sendMQTT_MqttClientDisconnected_TriesToReconnect() throws Exception {
        when(sinkRecord.value()).thenReturn(new Object());
        when(JsonUtils.toJsonBytes(any())).thenReturn(new byte[]{});
        when(mqttClient.isConnected()).thenReturn(false);

        PowerMockito.when(MQTTUtil.getMqttClient(any(MQTTConfig.class))).thenReturn(mqttClient);
        PowerMockito.when(MQTTUtil.defaultOptions(any(MQTTConfig.class))).thenReturn(new MqttConnectOptions());
        PowerMockito.doNothing().when(MQTTUtil.class, "tryReconnect", any(), any(), any(), any());

        doNothing().when(mqttClient).connect(any(MqttConnectOptions.class));
        doNothing().when(mqttClient).publish(anyString(), any(MqttMessage.class));

        mqttSinkTask.sendMQTT(Collections.singletonList(sinkRecord));


        verify(mqttClient).publish(anyString(), any(MqttMessage.class));
        verify(mqttClient, times(1)).connect(any(MqttConnectOptions.class));
    }

    @Test
    public void sendMQTT_MqttExceptionThrown_ThrowsRetryableException() throws Exception {
        when(sinkRecord.value()).thenReturn(new Object());
        when(JsonUtils.toJsonBytes(any())).thenReturn(new byte[]{});
        when(mqttClient.isConnected()).thenReturn(true);
        PowerMockito.when(MQTTUtil.getMqttClient(any(MQTTConfig.class))).thenReturn(mqttClient);
        PowerMockito.when(MQTTUtil.defaultOptions(any(MQTTConfig.class))).thenReturn(new MqttConnectOptions());
        doThrow(new MqttException(0)).when(mqttClient).publish(anyString(), any(MqttMessage.class));

        assertThrows(RetryableException.class, () -> mqttSinkTask.sendMQTT(Collections.singletonList(sinkRecord)));
    }


}
