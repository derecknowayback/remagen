package com.dereck.remagen.mqtt.client;

import com.dereck.remagen.mqtt.request.RestfulRequest;
import com.dereck.remagen.mqtt.request.delete.DeleteConnectorRequest;
import com.dereck.remagen.mqtt.request.get.GetAllConnectorsRequest;
import com.dereck.remagen.mqtt.request.get.GetConnectorRequest;
import com.dereck.remagen.mqtt.request.post.CreateConnectorReq;
import lombok.Data;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest.InitialState;

import java.util.List;
import java.util.Map;

@Data
public class KafkaConnectManager {

    private String host;
    private String port;
    private boolean needHttps;

    private RestManager restManager;

    public KafkaConnectManager(String host, String port, boolean needHttps) {
        this.host = host;
        this.port = port;
        // todo support https/ssl
        this.restManager = new RestManager(host, port, needHttps);
    }

    public ConnectorInfo createConnector(String connectorName, Map<String, String> config) {
        CreateConnectorReq createConnectorReq = new CreateConnectorReq(connectorName, config, InitialState.RUNNING);
        // todo check connectorInfo
        return sendRequest(createConnectorReq);
    }


    public List<String> getAllConnectors() {
        GetAllConnectorsRequest getAllConnectorsRequest = new GetAllConnectorsRequest();
        return sendRequest(getAllConnectorsRequest);
    }

    public ConnectorInfo getConnector(String connectorName) {
        GetConnectorRequest getConnectorRequest = new GetConnectorRequest(connectorName);
        return sendRequest(getConnectorRequest);
    }

    public void deleteConnector(String connectorName) {
        DeleteConnectorRequest deleteConnectorRequest = new DeleteConnectorRequest(connectorName);
        sendRequest(deleteConnectorRequest);
    }

    public <T> T sendRequest(RestfulRequest<T> request) {
        switch (request.getRequestMethod()) {
            case GET:
                return restManager.sendGetRequest(request);
            case POST:
                return restManager.sendPostRequest(request);
            case DELETE:
                return restManager.sendDeleteRequest(request);
            default:
                throw new RuntimeException("unsupported request method");
        }
    }


}
