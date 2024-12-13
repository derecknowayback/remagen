package com.dereckchen.remagen.models;

import com.dereckchen.remagen.utils.JsonUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Data
public class  BridgeMessage {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // copy from MqttMessage
    @JsonIgnore
    private int qos = 1;

    @JsonIgnore
    private boolean retained = false;

    // set by user
    private String id;
    private String content; // json format
    private String timestamp; // yyyy-MM-dd HH:mm:ss

    public BridgeMessage(IBridgeMessageContent content,int qos, boolean retained) {
        this.id = content.getMessageId();
        this.content = content.serializeToJsonStr();
        this.qos = qos;
        this.retained = retained;
        this.timestamp = LocalDateTime.now().format(DATE_TIME_FORMATTER);
    }


    public MqttMessage transferToMqttMessage() {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setRetained(retained);
        mqttMessage.setQos(qos);
        mqttMessage.setPayload(JsonUtils.toJsonBytes(this)); // serial self
        return mqttMessage;
    }




}
