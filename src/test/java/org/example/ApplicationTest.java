package org.example;

import java.util.concurrent.CompletableFuture;

import org.example.configurations.EmbeddedKafkaHolder;
import org.example.messages.Greeting;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka
@DirtiesContext
public class ApplicationTest {
    
    @Value(value = "${topic}")
    private String topic;
    
    @Autowired
    @Qualifier("kafkaTemplate")
    private KafkaTemplate kafkaTemplate;
    
    @Autowired
    @Qualifier("greetingKafkaTemplate")
    private KafkaTemplate greetingKafkaTemplate;
    
    @BeforeAll
    public static void beforeAll() {
        EmbeddedKafkaHolder.getEmbeddedKafkaBroker().addTopics("hat", "cat");
    }
    
    @AfterAll
    public static void afterAll() {
        EmbeddedKafkaHolder.getEmbeddedKafkaBroker().destroy();
    }
    
    @Test
    void kafkaTemplateTest() {
        
        String message = "Test string message";
        //final ProducerRecord<String, String> record = createRecord(message);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
            }
        });
    }
    
    @Test
    void greetingTemplateTest() {
        
        Greeting greeting = new Greeting("Greetings!", "Hello world!");
        
        CompletableFuture<SendResult<String, Greeting>> future = greetingKafkaTemplate.send(topic, greeting);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + greeting + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + greeting + "] due to : " + ex.getMessage());
            }
        });
    }
    //
    //    @Test
    //    void greetingTemplateMultitypeTest() {
    //
    //        Greeting greeting = new Greeting("Greetings!", "Hello world!");
    //        Farewell farewell = new Farewell("Farewell!", 25);
    //        String message = "Simple string";
    //
    //        CompletableFuture<SendResult<String, Greeting>> futureGreeting = multiTypeKafkaTemplate.send(topic, greeting);
    //        futureGreeting.whenComplete((result, ex) -> {
    //            if (ex == null) {
    //                System.out.println("Sent message=[" + greeting + "] with offset=[" + result.getRecordMetadata().offset() + "]");
    //            } else {
    //                System.out.println("Unable to send message=[" + greeting + "] due to : " + ex.getMessage());
    //            }
    //        });
    //
    //        CompletableFuture<SendResult<String, Farewell>> futureFarewell = multiTypeKafkaTemplate.send(topic, farewell);
    //        futureFarewell.whenComplete((result, ex) -> {
    //            if (ex == null) {
    //                System.out.println("Sent message=[" + farewell + "] with offset=[" + result.getRecordMetadata().offset() + "]");
    //            } else {
    //                System.out.println("Unable to send message=[" + farewell + "] due to : " + ex.getMessage());
    //            }
    //        });
    //
    //        CompletableFuture<SendResult<String, String>> futureString = multiTypeKafkaTemplate.send(topic, message);
    //        futureString.whenComplete((result, ex) -> {
    //            if (ex == null) {
    //                System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
    //            } else {
    //                System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
    //            }
    //        });
    //    }
    
    //    @Test
    //    void greetingTest() {
    //
    //        Greeting greeting = new Greeting("Greetings!", "Hello world!");
    //        ProducerRecord<Greeting, String> producerRecord = new ProducerRecord<>(topicName, greeting, greeting.getMsg());
    //        multiTypeKafkaTemplate.send(producerRecord);
    //
    //        Consumer consumer = consumerFactory.createConsumer();
    //        consumer.subscribe(List.of("topicName"));
    //        ConsumerRecords consumerRecords = consumer.poll(Duration.ofSeconds(1));
    //        Iterator iterator = consumerRecords.iterator();
    //        while (iterator.hasNext()) {
    //
    //            ConsumerRecord record = (ConsumerRecord) iterator.next();
    //            System.out.println(record);
    //
    //            long offset = record.offset();
    //            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(++offset, "");
    //
    //            Map<TopicPartition, OffsetAndMetadata> kafkaOffsetMap = new HashMap<>();
    //            kafkaOffsetMap.put(new TopicPartition("myPartition", record.partition()), offsetAndMetadata);
    //            consumer.commitSync(kafkaOffsetMap);
    //        }
    //    }
    //
    //    @Test
    //    void farewellTest() {
    //        multiTypeKafkaTemplate.send(topicName, new Farewell("Farewell!", 1));
    //    }
}
