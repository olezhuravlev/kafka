package org.example.components;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.kafkainaction.Alert;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@Component
public class AvroConsumer {
    
    @Value("${topic}")
    private String topic;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.kafka.groupId}")
    private String groupId;
    
    @Value("${spring.kafka.schema-registry.url}")
    private String schemaRegistryUrl;
    
    private List<String> consumedMessages = new ArrayList<>();
    
    public void consume() {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", schemaRegistryUrl);
        
        //HelloWorldConsumer hwConsumer = new HelloWorldConsumer();
        consume(properties);
        //Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }
    
    private void consume(Properties properties) {
        try (KafkaConsumer<Long, Alert> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(topic));
            
            while (true) {
                ConsumerRecords<Long, Alert> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<Long, Alert> record : records) {
                    String message = String.format("CONSUMED RECORD KEY = {}, VALUE = {}, OFFSET = {}, PARTITION = {}", record.key(),
                        record.value(), record.offset(), record.partition());
                    consumedMessages.add(message);
                    return;
                }
            }
        }
    }
    
    public List<String> getConsumedMessages() {
        return new ArrayList<>(consumedMessages);
    }
}
