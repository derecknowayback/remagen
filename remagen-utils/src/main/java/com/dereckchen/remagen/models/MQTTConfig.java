package com.dereckchen.remagen.models;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class MQTTConfig {
    /**
     * The hostname or IP address of the MQTT broker.
     */
    private String broker;
    /**
     * The MQTT topic to which the message will be published.
     */
    private String topic;
    /**
     * The username used to authenticate with the MQTT broker.
     */
    private String username;
    /**
     * The password used to authenticate with the MQTT broker.
     */
    private String password;
    /**
     * The unique identifier for the MQTT client.
     */
    private String clientid;
}

