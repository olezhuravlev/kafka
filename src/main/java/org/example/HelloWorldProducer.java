package org.example;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.kafkainaction.Alert;
import org.kafkainaction.AlertStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Component
public class HelloWorldProducer {
    
    final static Logger LOGGER = LoggerFactory.getLogger(HelloWorldProducer.class);
    public static final String TOPIC = "kinaction_hw";
    public static final String BOOTSTRAP_SERVERS = "kafka-broker-1:29092, kafka-broker-2:29092, kafka-broker-3:29092";
    
    public static void main(String[] args) {
        
        Properties properties = new Properties();
        
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://schema-registry:8081");
        
        for (int i = 0; i < 1000; ++i) {
            try (Producer<Long, Alert> producer = new KafkaProducer<>(properties)) {
                Alert alert = new Alert(12345L, Instant.now().toEpochMilli(), AlertStatus.Critical);
                LOGGER.info("kinaction_info Alert -> {}", alert);
                ProducerRecord<Long, Alert> producerRecord = new ProducerRecord<>(TOPIC, alert.getSensorId(), alert);
                producer.send(producerRecord, (metadata, exception) -> {
                    if (metadata != null) {
                        LOGGER.info("MESSAGE TO TOPIC '{}'", metadata.topic());
                    }
                    if (exception != null) {
                        LOGGER.info("SUBMIT MESSAGE EXCEPTION '{}'", exception.getLocalizedMessage());
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
