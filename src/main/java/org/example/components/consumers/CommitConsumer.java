package org.example.components.consumers;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.example.messages.AlertCustom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitConsumer implements Runnable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitConsumer.class);
    
    private final org.apache.kafka.clients.consumer.KafkaConsumer<AlertCustom, String> consumer;
    private final AtomicBoolean stopping = new AtomicBoolean();
    
    public CommitConsumer(KafkaConsumer<AlertCustom, String> consumer) {
        this.consumer = consumer;
    }
    
    @Override
    public void run() {
        try {
            consumer.subscribe(List.of("kinaction_promos"));
            
            while (!stopping.get()) {
                ConsumerRecords<AlertCustom, String> consumerRecords = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<AlertCustom, String> consumerRecord : consumerRecords) {
                    
                    OffsetAndMetadata offsetMeta = new OffsetAndMetadata(consumerRecord.offset() + 1, "");
                    
                    Map<TopicPartition, OffsetAndMetadata> kaOffsetMap = new HashMap<>();
                    kaOffsetMap.put(new TopicPartition("kinaction_audit", consumerRecord.partition()), offsetMeta);
                    
                    LOGGER.info("kinaction_info offset = {}, key = {}", consumerRecord.offset(), consumerRecord.key());
                    LOGGER.info("kinaction_info value = {}", Double.parseDouble(consumerRecord.value()) * 1.543);
                    
                    commitOffset(consumerRecord.offset(), consumerRecord.partition(), consumerRecord.topic(), consumer);
                }
            }
        } catch (WakeupException e) {
            if (!stopping.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }
    
    private static void commitOffset(long offset, int partition, String topic, KafkaConsumer<AlertCustom, String> consumer) {
        
        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(++offset, "");
        Map<TopicPartition, OffsetAndMetadata> kaOffsetMap = new HashMap<>();
        kaOffsetMap.put(new TopicPartition(topic, partition), offsetMeta);
        
        OffsetCommitCallback callback = new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        LOGGER.info("kinaction_error: offset {}", entry.getValue().offset());
                    }
                } else {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        LOGGER.info("kinaction_info: offset {}", entry.getValue().offset());
                    }
                }
            }
        };
        
        consumer.commitAsync(kaOffsetMap, callback);
    }
    
    public void shutdown() {
        stopping.set(true);
        consumer.wakeup();
    }
}
