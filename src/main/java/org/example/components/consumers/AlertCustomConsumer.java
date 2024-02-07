package org.example.components.consumers;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.messages.AlertCustom;
import org.example.serializers.AlertCustomKeySerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
//@Scope(scopeName = SCOPE_PROTOTYPE)
//@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AlertCustomConsumer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertCustomConsumer.class);
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    private int consumingAttemptCounter;
    
    private static boolean callbackInvoked;
    
    public static void main(String[] args) {
        
        AlertCustomConsumer consumer = new AlertCustomConsumer();
        consumer.consumeAutoCommit("kinaction_alert");
        
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }
    
    @Lookup
    public AlertCustomConsumer getConsumerLookup() {
        return null;
    }
    
    @Lookup
    public static AlertCustomConsumer getConsumerLookupStatic() {
        return null;
    }
    
    private void shutdown() {
        consumingAttemptCounter = 0;
    }
    
    public Optional<Object[]> consumeAutoCommit(String topic) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertCustomKeySerde.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_webconsumer");
        
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        try (
            KafkaConsumer<AlertCustom, String> consumer = new KafkaConsumer<>(properties)) {
            //consumer.subscribe(List.of(topic));
            
            // Assign partition to consumer bypassing the group coordinator.
            TopicPartition partitionZero = new TopicPartition(topic, 0);
            consumer.assign(List.of(partitionZero));
            
            consumingAttemptCounter = 50;
            try {
                while (consumingAttemptCounter > 0) {
                    ConsumerRecords<AlertCustom, String> consumerRecords = consumer.poll(Duration.ofMillis(250));
                    for (ConsumerRecord<AlertCustom, String> consumerRecord : consumerRecords) {
                        LOGGER.info("+++ AlertCustomConsumer: consumeAutoCommit: offset={}, key={}, value={}", consumerRecord.offset(),
                            consumerRecord.key(),
                            consumerRecord.value());
                        return Optional.of(new Object[] { consumerRecord.key(), consumerRecord.value() });
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
    
    public Optional<Object[]> consumeCommitSync(String topic) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertCustomKeySerde.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_webconsumer");
        
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // "Not less than once" semantics.
        
        try (
            KafkaConsumer<AlertCustom, String> consumer = new KafkaConsumer<>(properties)) {
            //consumer.subscribe(List.of(topic));
            
            // Assign partition to consumer bypassing the group coordinator.
            TopicPartition partitionZero = new TopicPartition(topic, 0);
            consumer.assign(List.of(partitionZero));
            
            consumingAttemptCounter = 50;
            try {
                while (consumingAttemptCounter > 0) {
                    ConsumerRecords<AlertCustom, String> consumerRecords = consumer.poll(Duration.ofMillis(250));
                    for (ConsumerRecord<AlertCustom, String> consumerRecord : consumerRecords) {
                        LOGGER.info("+++ AlertCustomConsumer: consumeCommitAsync: offset={}, key={}, value={}", consumerRecord.offset(),
                            consumerRecord.key(), consumerRecord.value());
                        commitOffsetSync(consumerRecord.offset(), consumerRecord.partition(), consumerRecord.topic(), consumer);
                        return Optional.of(new Object[] { consumerRecord.key(), consumerRecord.value() });
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
    
    private static void commitOffsetSync(long offset, int partition, String topic, KafkaConsumer<AlertCustom, String> consumer) {
        
        // Metadata defines offset to fix.
        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(++offset, "");
        
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(new TopicPartition(topic, partition), offsetMeta);
        
        consumer.commitSync(offsetMap);
    }
    
    public void consumeCommitAsync(String topic, Consumer<ConsumerRecord<AlertCustom, String>> callback) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertCustomKeySerde.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_webconsumer");
        
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // "Not less than once" semantics.
        
        try (
            KafkaConsumer<AlertCustom, String> consumer = new KafkaConsumer<>(properties)) {
            //consumer.subscribe(List.of(topic));
            
            // Assign partition to consumer bypassing the group coordinator.
            TopicPartition partitionZero = new TopicPartition(topic, 0);
            consumer.assign(List.of(partitionZero));
            
            consumingAttemptCounter = 50;
            try {
                while (consumingAttemptCounter > 0) {
                    ConsumerRecords<AlertCustom, String> consumerRecords = consumer.poll(Duration.ofMillis(250));
                    for (ConsumerRecord<AlertCustom, String> consumerRecord : consumerRecords) {
                        LOGGER.info("+++ AlertCustomConsumer: consumeCommitAsync: offset={}, key={}, value={}", consumerRecord.offset(),
                            consumerRecord.key(), consumerRecord.value());
                        commitOffsetAsync(consumerRecord, topic, consumer, callback);
                    }
                    Thread.sleep(100);
                    --consumingAttemptCounter;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private void commitOffsetAsync(ConsumerRecord<AlertCustom, String> consumerRecord, String topic,
        KafkaConsumer<AlertCustom, String> consumer, Consumer<ConsumerRecord<AlertCustom, String>> callback) {
        
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();
        
        // Metadata defines offset to fix.
        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(++offset, "");
        
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(new TopicPartition(topic, partition), offsetMeta);
        
        consumer.commitAsync(offsetMap, (map, e) -> {
            callbackInvoked = true;
            if (e == null) {
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
                    LOGGER.info("+++ AlertCustomConsumer: offset {}", entry.getValue().offset());
                }
                callback.accept(consumerRecord);
            } else {
                LOGGER.info("+++ AlertCustomConsumer: Exception {}", e.getLocalizedMessage());
                callback.accept(null);
            }
        });
    }
    
    public static void dropCallbackInvoked() {
        callbackInvoked = false;
    }
    
    public static boolean isCallbackInvoked() {
        return callbackInvoked;
    }
}
