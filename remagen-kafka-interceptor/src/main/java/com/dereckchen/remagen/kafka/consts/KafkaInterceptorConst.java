package com.dereckchen.remagen.kafka.consts;

public class KafkaInterceptorConst {
    /**
     * The key used to identify whether a message needs to be bridged in the Kafka header
     */
    public static final String KAFKA_HEADER_NEED_BRIDGE_KEY = "needBridge";
    /**
     * The key used to store the bridge option information in the Kafka header
     */
    public static final String KAFKA_HEADER_BRIDGE_OPTION_KEY = "bridgeOption";
}
