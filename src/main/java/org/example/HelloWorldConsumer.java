package org.example;

import static org.example.HelloWorldProducer.BOOTSTRAP_SERVERS;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.kafkainaction.Alert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@Component
public class HelloWorldConsumer {
    
    final static Logger LOGGER = LoggerFactory.getLogger(HelloWorldConsumer.class);
    private static volatile boolean keepConsuming = true;
    
    public static void main(String[] args) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_helloconsumer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", "http://schema-registry:8081");
        
        HelloWorldConsumer hwConsumer = new HelloWorldConsumer();
        hwConsumer.consume(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(hwConsumer::shutdown));
    }
    
    private void consume(Properties properties) {
        try (KafkaConsumer<Long, Alert> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(HelloWorldProducer.TOPIC));
            
            while (keepConsuming) {
                ConsumerRecords<Long, Alert> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<Long, Alert> record : records) {
                    LOGGER.info("CONSUMED RECORD KEY = {}, VALUE = {}, OFFSET = {}, PARTITION = {}", record.key(), record.value(), record.offset(), record.partition());
                }
            }
        }
    }
    
    private void shutdown() {
        keepConsuming = false;
    }
}
