package org.example.components.consumers;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.messages.AlertCustom;
import org.example.serializers.AlertCustomKeySerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AlertCustomConsumer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertCustomConsumer.class);
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    private int consumingAttemptCounter;
    
    public static void main(String[] args) {
        
        AlertCustomConsumer consumer = new AlertCustomConsumer();
        consumer.consume("kinaction_alert");
        
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }
    
    private void shutdown() {
        consumingAttemptCounter = 0;
    }
    
    public Optional consume(String topic) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_webconsumer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        // Reading from the beginning:
        // kaProperties.put("group.id", UUID.randomUUID().toString());
        // kaProperties.put("auto.offset.reset", "earliest");
        
        // Reading from the end:
        // kaProperties.put("group.id", UUID.randomUUID().toString());
        // kaProperties.put("auto.offset.reset", "latest");
        
        // offsetsForTimes
        // Map<TopicPartition, OffsetAndTimestamp> kaOffsetMap = consumer.offsetsForTimes(timeStampMapper);
        // consumer.seek(partitionOne, kaOffsetMap.get(partitionOne).offset());
        
        try (
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            //consumer.subscribe(List.of(topic));
            
            TopicPartition partitionZero = new TopicPartition(topic, 0);
            consumer.assign(List.of(partitionZero));
            
            consumingAttemptCounter = 50;
            try {
                while (consumingAttemptCounter > 0) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(250));
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        LOGGER.info("kinaction_info offset={}, key={}, value={}", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                        //LOGGER.info("kinaction_info value = {}", Double.parseDouble(consumerRecord.value()) * 1.543);
                        
                        commitOffset(consumerRecord.offset(), consumerRecord.partition(), consumerRecord.topic(), consumer);
                        
                        return Optional.of(consumerRecord.value());
                    }
                    Thread.sleep(100);
                    --consumingAttemptCounter;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        
        return Optional.empty();
    }
    
    private static void commitOffset(long offset, int partition, String topic, KafkaConsumer<String, String> consumer) {
        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(++offset, "");
        Map<TopicPartition, OffsetAndMetadata> kaOffsetMap = new HashMap<>();
        kaOffsetMap.put(new TopicPartition(topic, partition), offsetMeta);
        consumer.commitAsync(kaOffsetMap, AlertCustomConsumer::onComplete);
    }
    
    private static void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if (e != null) {
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
                LOGGER.info("kinaction_error: offset {}", entry.getValue().offset());
            }
        } else {
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
                LOGGER.info("kinaction_info: offset {}", entry.getValue().offset());
            }
        }
    }
}
