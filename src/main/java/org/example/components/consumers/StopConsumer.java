package org.example.components.consumers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.example.messages.AlertCustom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopConsumer implements Runnable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(StopConsumer.class);
    
    private final KafkaConsumer<AlertCustom, String> consumer;
    private final AtomicBoolean stopping = new AtomicBoolean();
    
    public StopConsumer(KafkaConsumer<AlertCustom, String> consumer) {
        this.consumer = consumer;
    }
    
    @Override
    public void run() {
        
        //        Properties kaProperties = new Properties();
        //        kaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //        kaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertCustomKeySerde.class);
        //        kaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //
        //        kaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        //        kaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.offset);
        //        kaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //        kaProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        try {
            consumer.subscribe(List.of("kinaction_promos"));
            
            while (!stopping.get()) {
                ConsumerRecords<AlertCustom, String> consumerRecords = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<AlertCustom, String> consumerRecord : consumerRecords) {
                    LOGGER.info("kinaction_info offset = {}, key = {}", consumerRecord.offset(), consumerRecord.key());
                    LOGGER.info("kinaction_info value = {}", Double.parseDouble(consumerRecord.value()) * 1.543);
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
    
    public void shutdown() {
        stopping.set(true);
        consumer.wakeup();
    }
}
