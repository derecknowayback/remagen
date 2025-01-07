package com.dereckchen.remagen.models;

import lombok.Data;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.List;
import java.util.Map;


@Data
public class ConnectorInfoV2 {

    private String name;
    private Map<String, String> config;
    private List<ConnectorTaskId> tasks;
    private ConnectorType type;
    private Integer errorCode;
    private String message;
}
