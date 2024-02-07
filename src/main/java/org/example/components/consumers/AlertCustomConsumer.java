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
    
    public Optional<Object[]> consumeCommitAsync(String topic) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertCustomKeySerde.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_webconsumer");
        
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // "Not less than once" semantics.
        
        try (
            KafkaConsumer<AlertCustom, String> consumer = new KafkaConsumer<>(properties)) {
            //consumer.subscribe(List.of(topic));
            
            TopicPartition partition0 = new TopicPartition(topic, 0);
            //TopicPartition partition1 = new TopicPartition(topic, 1);
            //TopicPartition partition2 = new TopicPartition(topic, 2);
            List<TopicPartition> topics = List.of(partition0 /*, partition1, partition2*/);
            consumer.assign(topics);
            
            consumingAttemptCounter = 50;
            try {
                while (consumingAttemptCounter > 0) {
                    ConsumerRecords<AlertCustom, String> consumerRecords = consumer.poll(Duration.ofMillis(250));
                    for (ConsumerRecord<AlertCustom, String> consumerRecord : consumerRecords) {
                        LOGGER.info("+++ AlertCustomConsumer: consumeCommitAsync: offset={}, key={}, value={}", consumerRecord.offset(),
                            consumerRecord.key(), consumerRecord.value());
                        commitOffsetAsync(consumerRecord.offset(), consumerRecord.partition(), topics, consumer);
                        //return Optional.of(new Object[] { consumerRecord.key(), consumerRecord.value() });
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
    
    private static void commitOffsetAsync(long offset, int partition, List<TopicPartition> topics,
        KafkaConsumer<AlertCustom, String> consumer) {
        
        // Metadata defines offset to fix.
        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(++offset, "");
        
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        //offsetMap.put(new TopicPartition(topic, partition), offsetMeta);
        for (TopicPartition currentTopic : topics) {
            offsetMap.put(currentTopic, offsetMeta);
        }
        
        consumer.commitAsync(offsetMap, AlertCustomConsumer::onComplete);
    }
    
    private static void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        callbackInvoked = true;
        if (e == null) {
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
                LOGGER.info("+++ AlertCustomConsumer: offset {}", entry.getValue().offset());
            }
        } else {
            LOGGER.info("+++ AlertCustomConsumer: Exception {}", e.getLocalizedMessage());
        }
    }
    
    public static void dropCallbackInvoked() {
        callbackInvoked = false;
    }
    
    public static boolean isCallbackInvoked() {
        return callbackInvoked;
    }
}
