package org.example.components.consumers;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.example.messages.AlertCustom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AlertCustomConsumerMetricsInterceptor implements ConsumerInterceptor<AlertCustom, String> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertCustomConsumerMetricsInterceptor.class);
    
    public static final String CONSUME_TIME_HEADER = "consumeTime";
    
    @Override
    public ConsumerRecords<AlertCustom, String> onConsume(ConsumerRecords<AlertCustom, String> records) {
        if (records.isEmpty()) {
            return records;
        } else {
            for (ConsumerRecord<AlertCustom, String> record : records) {
                Headers headers = record.headers();
                for (Header header : headers) {
                    if ("kinactionTraceId".equals(header.key())) {
                        LOGGER.info("+++ AlertCustomConsumerMetricsInterceptor: kinactionTraceId is: " + new String(header.value()));
                    }
                }
                // Add an additional header.
                headers.add(CONSUME_TIME_HEADER, String.valueOf(System.currentTimeMillis()).getBytes());
            }
        }
        return records;
    }
    
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Do nothing.
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
