package com.dereckchen.remagen.models;

import com.dereckchen.remagen.utils.JsonUtils;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Data
@NoArgsConstructor
public class BridgeMessage {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // copy from MqttMessage
    private int qos = 1;
    private boolean retained = false;

    // set by user
    private String id;
    private String content; // json format
    private String timestamp; // yyyy-MM-dd HH:mm:ss

    private LocalDateTime kafkaPubTime;
    private LocalDateTime arriveAtSink;
    private LocalDateTime pubFromSink;
    private LocalDateTime arriveAtSource;
    private LocalDateTime pubFromSource;
    private LocalDateTime mqttEndTime;
    private LocalDateTime mqttPubTime;


    /**
     * Constructs a new BridgeMessage with the specified content, QoS, and retained flag.
     *
     * @param content  The content of the message, which should be an instance of IBridgeMessageContent.
     * @param qos      The Quality of Service level for the message (0, 1, or 2).
     * @param retained If true, the message will be retained by the broker.
     */
    public BridgeMessage(IBridgeMessageContent content, int qos, boolean retained) {
        this.id = content.getMessageId();
        this.content = content.serializeToJsonStr();
        this.qos = qos;
        this.retained = retained;
        this.timestamp = LocalDateTime.now().format(DATE_TIME_FORMATTER);
    }

    /**
     * Transfers the BridgeMessage to an MQTT message.
     *
     * @return An MqttMessage object containing the serialized BridgeMessage.
     */
    public MqttMessage transferToMqttMessage() {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setRetained(retained);
        mqttMessage.setQos(qos);
        mqttMessage.setPayload(JsonUtils.toJsonBytes(this)); // serial self
        return mqttMessage;
    }
}

