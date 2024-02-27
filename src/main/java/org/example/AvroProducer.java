package org.example;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.kafkainaction.Alert;
import org.kafkainaction.AlertStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroProducer {
    
    final static Logger LOGGER = LoggerFactory.getLogger(AvroProducer.class);
    public static final String TOPIC = "kinaction_hw";
    public static final String BOOTSTRAP_SERVERS = "kafka-broker-1:9092";
    
    public static void main(String[] args) {
        
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://schema-registry:8081");
        
        // For maximum guaranties of delivery.
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        
        for (int i = 0; i < 1; ++i) {
            try (Producer<Long, Alert> producer = new KafkaProducer<>(properties)) {
                Alert alert = new Alert(12345L, Instant.now().toEpochMilli(), AlertStatus.Critical, "my_recovery_details");
                LOGGER.info("kinaction_info Alert -> {}", alert);
                ProducerRecord<Long, Alert> producerRecord = new ProducerRecord<>(TOPIC, alert.getSensorId(), alert);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (metadata != null) {
                            LOGGER.info("*** MESSAGE SENT: offset = {}, topic = {}, timestamp = {}", metadata.offset(), metadata.topic(),
                                metadata.timestamp());
                        }
                        if (exception != null) {
                            LOGGER.info("!!! EXCEPTION WHILE MESSAGE SENDING '{}'", exception.getLocalizedMessage());
                        }
                    }
                });
                producer.flush();
                //producer.close();// redundant if in `try(){}`-block!
            }
            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
