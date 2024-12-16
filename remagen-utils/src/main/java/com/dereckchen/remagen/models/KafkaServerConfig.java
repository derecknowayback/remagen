package com.dereckchen.remagen.models;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class KafkaServerConfig {
    private List<Host> hosts;
    private boolean needHttps;
    private String kafkaTopic; // option, for sink connector
}
