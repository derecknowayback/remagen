package com.dereckchen.remagen.kakfa.restful.client;


import com.dereckchen.remagen.kakfa.restful.request.RestfulRequest;
import com.dereckchen.remagen.kakfa.restful.request.delete.DeleteConnectorRequest;
import com.dereckchen.remagen.kakfa.restful.request.get.GetAllConnectorsRequest;
import com.dereckchen.remagen.kakfa.restful.request.get.GetConnectorRequest;
import com.dereckchen.remagen.kakfa.restful.request.post.CreateConnectorReq;
import com.dereckchen.remagen.models.ConnectorInfoV2;
import com.dereckchen.remagen.utils.MetricsUtils;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import lombok.Data;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest.InitialState;

import java.util.List;
import java.util.Map;

@Data
public class KafkaConnectManager {

    private String host;
    private String port;
    private boolean needHttps;

    private RestManager restManager;
    private Histogram lantencyHistogram;
    private Counter counter;
    private Gauge gauge;

    public KafkaConnectManager(String host, String port, boolean needHttps) {
        this.host = host;
        this.port = port;
        this.restManager = new RestManager(host, port, needHttps);
        lantencyHistogram = MetricsUtils.getHistogram("kafka_connect_request_latency", "host","request_name","method");
        counter = MetricsUtils.getCounter("kafka_connect_request_counter", "host","request_name","method");
        gauge = MetricsUtils.getGauge("kafka_connect_active_requests", "host","request_name","method");
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
        T resp = null;
        long requestStart = System.currentTimeMillis();
        switch (request.getRequestMethod()) {
            // Determine the HTTP method of the request
            case GET:
                resp = restManager.sendGetRequest(request);
                break;
            case POST:
                resp = restManager.sendPostRequest(request);
                break;
            case DELETE:
                resp = restManager.sendDeleteRequest(request);
                break;
            default:
                throw new RuntimeException("unsupported request method");
        }
        MetricsUtils.observeRequestLatency(lantencyHistogram,
                System.currentTimeMillis() - requestStart,host,request.getRequestName(),request.getRequestMethod().name());
        return resp;
    }


}
