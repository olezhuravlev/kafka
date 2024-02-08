package org.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.concurrent.CompletableFuture;

import org.example.configurations.EmbeddedKafkaHolder;
import org.example.messages.Greeting;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka
@DirtiesContext
public class KafkaTemplateEmbedded {
    
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
    void kafkaTemplateStringTest() {
        
        String topic = "kafkaTemplateStringTest";
        String message = "Test string message";
        
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        future.whenComplete((result, ex) -> {
            assertNull(ex);
            assertNotNull(result);
            assertEquals(message, result.getProducerRecord().value());
        });
    }
    
    @Test
    void kafkaTemplateGreetingTest() {
        
        String topic = "kafkaTemplateGreetingTest";
        Greeting greeting = new Greeting("Greetings!", "Hello world!");
        
        CompletableFuture<SendResult<String, Greeting>> future = greetingKafkaTemplate.send(topic, greeting);
        future.whenComplete((result, ex) -> {
            assertNull(ex);
            assertNotNull(result);
            assertEquals(greeting, result.getProducerRecord().value());
        });
    }
}
