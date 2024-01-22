package org.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorldProducer {
    
    final static Logger LOGGER = LoggerFactory.getLogger(HelloWorldProducer.class);
    public static final String TOPIC = "kinaction_hw";
    public static final String BOOTSTRAP_SERVERS = "kafka-broker-1:29092, kafka-broker-2:29092, kafka-broker-3:29092";
    
    public static void main(String[] args) {
        
        Properties properties = new Properties();
        
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        Producer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, null, "Hello2");
        producer.send(producerRecord, (metadata, exception) -> {
            if (metadata != null) {
                LOGGER.info("MESSAGE TO TOPIC '{}'", metadata.topic());
            }
            if (exception != null) {
                LOGGER.info("SUBMIT MESSAGE EXCEPTION '{}'", exception.getLocalizedMessage());
            }
        });
        producer.flush();
        producer.close();
    }
}
