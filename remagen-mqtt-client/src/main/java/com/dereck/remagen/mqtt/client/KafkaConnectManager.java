package com.dereck.remagen.mqtt.client;

import com.dereck.remagen.mqtt.request.RestfulRequest;
import com.dereck.remagen.mqtt.request.get.GetConnectorRequest;
import com.dereck.remagen.mqtt.request.post.CreateConnectorReq;
import lombok.Data;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest.InitialState;

import java.util.Map;

@Data
public class KafkaConnectManager {

    private String host;
    private String port;
    private boolean needHttps;

    private RestManager restManager;

    public KafkaConnectManager (String host, String port) {
        this.host = host;
        this.port = port;
        // todo support https/ssl

    }




    public ConnectorInfo createConnector(String connectorName, Map<String, String> config,
                                InitialState initialState) {
        CreateConnectorReq createConnectorReq = new CreateConnectorReq(connectorName, config, initialState);
        // todo check connectorInfo
        return sendRequest(createConnectorReq);
    }


    public ConnectorInfo getConnector(String connectorName) {
        GetConnectorRequest getConnectorRequest = new GetConnectorRequest(connectorName);
        return sendRequest(getConnectorRequest);
    }




    public <T> T sendRequest(RestfulRequest<T> request) {
        switch (request.getRequestMethod()) {
            case GET:
                return restManager.sendGetRequest(request);
            case POST:
                return restManager.sendPostRequest(request);
            // todo support delete/put
            default:
                throw new RuntimeException("unsupported request method");
        }
    }









}
