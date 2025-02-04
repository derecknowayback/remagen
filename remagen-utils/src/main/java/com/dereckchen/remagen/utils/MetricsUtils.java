package com.dereckchen.remagen.utils;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.PushGateway;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static com.dereckchen.remagen.consts.ConnectorConst.*;

/**
 * Utility class for Prometheus metrics.
 */
public class MetricsUtils {

    public static Counter getCounter(String name, String... labelNames) {
        return Counter.build()
                .name(name)
                .help(name)
                .labelNames(labelNames)
                .register();
    }


    public static Gauge getGauge(String name, String... labelNames) {
        return Gauge.build().name(name).help(name).labelNames(labelNames).register();
    }


    public static Histogram getHistogram(String name, String... labelNames) {
        return Histogram.build()
                .name(name)
                .help(name)
                .labelNames(labelNames)
                .register();
    }

    /**
     * Increment the HTTP request counter.
     */
    public static void incrementCounter(Counter counter, String... labels) {
        counter.labels(labels).inc();
    }

    public static void incrementCounter(Counter counter, double cnt, String... labels) {
        counter.labels(labels).inc(cnt);
    }

    /**
     * Increment the active requests gauge.
     */
    public static void incrementGauge(Gauge gauge, String... labels) {
        gauge.labels(labels).inc();
    }

    /**
     * Decrement the active requests gauge.
     */
    public static void decrementGauge(Gauge gauge, String... labels) {
        gauge.labels(labels).dec();
    }

    /**
     * Observe the request latency.
     */
    public static void observeRequestLatency(Histogram histogram, double latency, String... labels) {
        histogram.labels(labels).observe(latency);
    }

    @Slf4j
    @AllArgsConstructor
    public static class FlushGatewayThread implements Runnable {

        PushGateway pushGateway;

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    pushGateway.push(CollectorRegistry.defaultRegistry, SINK_TASK_METRICS);
                    Thread.sleep(PUSH_GATE_WAY_INTERVAL);
                } catch (Exception e) {
                    log.error("pushGateway Exception", e);
                }
            }
        }
    }


}
