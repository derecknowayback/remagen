package com.dereckchen.remagen.utils;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.PushGateway;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.dereckchen.remagen.consts.ConnectorConst.PUSH_GATE_WAY_INTERVAL;
import static com.dereckchen.remagen.consts.ConnectorConst.SINK_TASK_METRICS;

/**
 * Utility class for Prometheus metrics.
 */
@Slf4j
public class MetricsUtils {

    private static final ConcurrentMap<String, Counter> counterMap = new ConcurrentHashMap<>(8);
    private static final ConcurrentMap<String, Histogram> histogramMap = new ConcurrentHashMap<>(8);
    private static final ConcurrentMap<String, Gauge> gaugeMap = new ConcurrentHashMap<>(8);

    public static Counter getCounter(String name, String... labelNames) {
        if (counterMap.containsKey(name)) {
            return counterMap.get(name);
        }
        Counter counter = Counter.build()
                .name(name)
                .help(name)
                .labelNames(labelNames).register();
        counterMap.put(name, counter);
        return counter;
    }


    public static Gauge getGauge(String name, String... labelNames) {
        if (gaugeMap.containsKey(name)) {
            return gaugeMap.get(name);
        }
        Gauge gauge = Gauge.build().name(name).help(name).labelNames(labelNames).register();
        gaugeMap.put(name, gauge);
        return gauge;
    }


    public static Histogram getHistogram(String name, String... labelNames) {
        if (histogramMap.containsKey(name)) {
            return histogramMap.get(name);
        }
        Histogram histogram = Histogram.build()
                .name(name)
                .help(name)
                .labelNames(labelNames)
                .register();
        histogramMap.put(name, histogram);
        return histogram;
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

    public static String getLocalIp() {
        try {
            InetAddress address = InetAddress.getLocalHost();
            return address.getHostAddress();
        } catch (UnknownHostException e) {
            log.error("getLocalIp failed", e);
            return "127.0.0.1";
        }
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
