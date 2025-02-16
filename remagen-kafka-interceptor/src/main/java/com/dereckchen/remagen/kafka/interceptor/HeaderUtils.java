package com.dereckchen.remagen.kafka.interceptor;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import static com.dereckchen.remagen.kafka.consts.KafkaInterceptorConst.KAFKA_HEADER_NEED_BRIDGE_KEY;

public class HeaderUtils {

    /**
     * Determines whether a record needs to be bridged based on the presence and value of a specific header.
     *
     * @param headers The headers of the Kafka record.
     * @return True if the record needs to be bridged, false otherwise.
     */
    public static boolean isRecordNeedBridge(Headers headers) {
        // Check if headers are null, return false if they are
        if (headers == null) {
            return false;
        }
        // Retrieve the last header with the key KAFKA_HEADER_NEED_BRIDGE_KEY
        Header header = headers.lastHeader(KAFKA_HEADER_NEED_BRIDGE_KEY);
        // If the header is null, return false
        if (header == null) {
            return false;
        }
        // Parse the header value as a boolean and return the result
        return Boolean.parseBoolean(new String(header.value()));
    }
}
