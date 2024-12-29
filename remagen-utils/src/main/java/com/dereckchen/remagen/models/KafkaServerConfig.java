package com.dereckchen.remagen.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KafkaServerConfig {
    /**
     * The hostname or IP address of the Kafka server.
     */
    private String host;
    /**
     * The port number on which the Kafka server is listening.
     */
    private String port;
    /**
     * A flag indicating whether the Kafka server requires HTTPS for communication.
     */
    private boolean needHttps;
    /**
     * A comma-separated list of Kafka topics to which the bridge will publish messages.
     */
    private String kafkaTopics; // can be multi topics separate by commas
}

