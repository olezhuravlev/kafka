package org.example.components.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.components.partitioners.AlertCustomPartitioner;
import org.example.messages.AlertCustom;
import org.example.serializers.AlertCustomKeySerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class AlertCustomProducer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertCustomProducer.class);
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    private static boolean callbackInvoked;
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AlertCustomProducer producer = new AlertCustomProducer();
        String message = "Stage 1 stopped";
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, message);
        producer.produce("kinaction_alertcustom", message, testAlert);
    }
    
    private static class AlertCustomCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            callbackInvoked = true;
            if (exception != null) {
                LOGGER.error("+++ AlertCustomProducer: Exception: ", exception);
            } else if (metadata != null) {
                LOGGER.info("+++ AlertCustomProducer: offset = {}, topic = {}, timestamp = {}", metadata.offset(), metadata.topic(),
                    metadata.timestamp());
            }
        }
    }
    
    public void produce(String topic, String message, AlertCustom alert) throws ExecutionException, InterruptedException {
        
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AlertCustomKeySerde.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AlertCustomPartitioner.class);
        // properties.put(ProducerConfig.ACKS_CONFIG, "all");
        
        try (
            Producer<AlertCustom, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<AlertCustom, String> producerRecord = new ProducerRecord<>(topic, alert, message);
            RecordMetadata result = producer.send(producerRecord, new AlertCustomCallback()).get();
            if (result != null) {
                LOGGER.info("AlertCustomProducer: offset = {}, topic = {}, timestamp = {}", result.offset(), result.topic(),
                    result.timestamp());
            }
            // producer.flush();
            // producer.close();
        }
    }
    
    public static void dropCallbackInvoked() {
        callbackInvoked = false;
    }
    
    public static boolean isCallbackInvoked() {
        return callbackInvoked;
    }
}
