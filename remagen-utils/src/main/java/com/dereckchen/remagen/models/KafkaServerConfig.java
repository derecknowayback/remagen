package com.dereckchen.remagen.models;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class KafkaServerConfig {
    private String host;
    private String port;
    private boolean needHttps;
    private String kafkaTopic; // option, for sink connector
}
