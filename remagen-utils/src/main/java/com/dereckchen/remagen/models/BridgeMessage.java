package com.dereckchen.remagen.models;

import com.dereckchen.remagen.utils.JsonUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.eclipse.paho.client.mqttv3.MqttMessage;

@Data
public class  BridgeMessage {
    // copy from MqttMessage
    @JsonIgnore
    private int qos = 1;

    @JsonIgnore
    private boolean retained = false;

    // set by user
    private String id;
    private String content; // json format

    public BridgeMessage(IBridgeMessageContent content,int qos, boolean retained) {
        this.id = content.getMessageId();
        this.content = content.serializeToJsonStr();
        this.qos = qos;
        this.retained = retained;
    }


    public MqttMessage transferToMqttMessage() {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setRetained(retained);
        mqttMessage.setQos(qos);
        mqttMessage.setPayload(JsonUtils.toJsonBytes(this)); // serial self
        return mqttMessage;
    }




}
