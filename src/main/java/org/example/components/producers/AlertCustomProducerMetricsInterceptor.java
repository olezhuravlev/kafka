package org.example.components.producers;

import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.example.messages.AlertCustom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AlertCustomProducerMetricsInterceptor implements ProducerInterceptor<AlertCustom, String> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertCustomProducerMetricsInterceptor.class);
    
    @Override
    public ProducerRecord<AlertCustom, String> onSend(ProducerRecord<AlertCustom, String> record) {
        Headers headers = record.headers();
        String kinactionTraceId = UUID.randomUUID().toString();
        headers.add("kinactionTraceId", kinactionTraceId.getBytes());
        LOGGER.info("+++ AlertCustomProducerMetricsInterceptor: Created kinactionTraceId: {}", kinactionTraceId);
        return record;
    }
    
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            LOGGER.info("+++ AlertCustomProducerMetricsInterceptor: topic = {} offset = {}", metadata.topic(), metadata.offset());
        } else {
            LOGGER.info("+++ AlertCustomProducerMetricsInterceptor: Exception: " + exception.getMessage());
        }
    }
    
    @Override
    public void close() {
        // Do nothing.
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing.
    }
}
