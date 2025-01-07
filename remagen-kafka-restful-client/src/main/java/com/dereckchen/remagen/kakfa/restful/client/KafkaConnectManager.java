package com.dereckchen.remagen.kakfa.restful.client;


import com.dereckchen.remagen.kakfa.restful.request.RestfulRequest;
import com.dereckchen.remagen.kakfa.restful.request.delete.DeleteConnectorRequest;
import com.dereckchen.remagen.kakfa.restful.request.get.GetAllConnectorsRequest;
import com.dereckchen.remagen.kakfa.restful.request.get.GetConnectorRequest;
import com.dereckchen.remagen.kakfa.restful.request.post.CreateConnectorReq;
import com.dereckchen.remagen.models.ConnectorInfoV2;
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
        this.restManager = new RestManager(host, port, needHttps);
    }

    /**
     * Creates a new Kafka connector with the given name and configuration.
     *
     * @param connectorName The name of the connector to create.
     * @param config        The configuration for the connector.
     * @return The created connector info.
     */
    public ConnectorInfoV2 createConnector(String connectorName, Map<String, String> config) {
        // Create a new CreateConnectorReq object with the given connector name, config, and initial state
        CreateConnectorReq createConnectorReq = new CreateConnectorReq(connectorName, config, InitialState.RUNNING);
        // Send the create connector request and return the response
        return sendRequest(createConnectorReq);
    }


    /**
     * Retrieves a list of all Kafka connectors.
     *
     * @return A list of connector names.
     */
    public List<String> getAllConnectors() {
        // Create a new GetAllConnectorsRequest object
        GetAllConnectorsRequest getAllConnectorsRequest = new GetAllConnectorsRequest();
        // Send the request and return the response
        return sendRequest(getAllConnectorsRequest);
    }


    /**
     * Retrieves the configuration and status information of a specified Kafka connector.
     *
     * @param connectorName The name of the Kafka connector to retrieve information for.
     * @return A ConnectorInfo object containing the configuration and status of the connector.
     */
    public ConnectorInfoV2 getConnector(String connectorName) {
        // Create a new GetConnectorRequest object with the given connector name
        GetConnectorRequest getConnectorRequest = new GetConnectorRequest(connectorName);
        // Send the request and return the response
        return sendRequest(getConnectorRequest);
    }


    /**
     * Deletes a Kafka connector with the given name.
     *
     * @param connectorName The name of the connector to delete.
     */
    public void deleteConnector(String connectorName) {
        // Create a new DeleteConnectorRequest object with the given connector name
        DeleteConnectorRequest deleteConnectorRequest = new DeleteConnectorRequest(connectorName);
        // Send the delete connector request
        sendRequest(deleteConnectorRequest);
    }


    /**
     * Sends a RESTful request to the Kafka Connect API and returns the response.
     *
     * @param request The RESTful request object containing the request details.
     * @return The response from the Kafka Connect API.
     * @throws RuntimeException If the request method is not supported.
     */
    public <T> T sendRequest(RestfulRequest<T> request) {
        // Determine the HTTP method of the request
        switch (request.getRequestMethod()) {
            // If the method is GET, send a GET request and return the response
            case GET:
                return restManager.sendGetRequest(request);
            // If the method is POST, send a POST request and return the response
            case POST:
                return restManager.sendPostRequest(request);
            // If the method is DELETE, send a DELETE request and return the response
            case DELETE:
                return restManager.sendDeleteRequest(request);
            // If the method is not supported, throw an exception
            default:
                throw new RuntimeException("unsupported request method");
        }
    }


}
