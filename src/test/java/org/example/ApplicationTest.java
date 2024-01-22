package org.example;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.example.messages.Farewell;
import org.example.messages.Greeting;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest
public class ApplicationTest {
    
    private static final String topicName = "testTopicName";
    
    private KafkaTemplate multiTypeKafkaTemplate;
    private ConcurrentKafkaListenerContainerFactory<String, Greeting> greetingKafkaListenerContainerFactory;
    private ConsumerFactory<String, String> consumerFactory;
    
    @Test
    void stringTest() {
        multiTypeKafkaTemplate.send(topicName, "Test string message");
    }
    
    @Test
    void greetingTest() {
        
        Greeting greeting = new Greeting("Greetings!", "Hello world!");
        ProducerRecord<Greeting, String> producerRecord = new ProducerRecord<>(topicName, greeting, greeting.getMsg());
        multiTypeKafkaTemplate.send(producerRecord);
        
        Consumer consumer = consumerFactory.createConsumer();
        consumer.subscribe(List.of("topicName"));
        ConsumerRecords consumerRecords = consumer.poll(Duration.ofSeconds(1));
        Iterator iterator = consumerRecords.iterator();
        while (iterator.hasNext()) {
            
            ConsumerRecord record = (ConsumerRecord) iterator.next();
            System.out.println(record);
            
            long offset = record.offset();
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(++offset, "");
            
            Map<TopicPartition, OffsetAndMetadata> kafkaOffsetMap = new HashMap<>();
            kafkaOffsetMap.put(new TopicPartition("myPartition", record.partition()), offsetAndMetadata);
            consumer.commitSync(kafkaOffsetMap);
        }
    }
    
    @Test
    void farewellTest() {
        multiTypeKafkaTemplate.send(topicName, new Farewell("Farewell!", 1));
    }
}
