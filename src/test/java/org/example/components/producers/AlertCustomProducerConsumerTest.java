package org.example.components.producers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.example.components.consumers.AlertCustomConsumer;
import org.example.configurations.EmbeddedKafkaHolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import lombok.SneakyThrows;

@SpringBootTest
@EmbeddedKafka
@DirtiesContext
public class AlertCustomProducerConsumerTest {
    
    @Value(value = "${topic}")
    private String topic;
    
    @Autowired
    private AlertCustomProducer alertCustomProducer;
    
    @Autowired
    private AlertCustomConsumer alertCustomConsumer;
    
    @BeforeAll
    public static void beforeAll() {
        //EmbeddedKafkaHolder.getEmbeddedKafkaBroker().addTopics("hat", "cat");
    }
    
    @AfterAll
    public static void afterAll() {
        EmbeddedKafkaHolder.getEmbeddedKafkaBroker().destroy();
    }
    
    @Test
    @SneakyThrows
    void kafkaTemplateStringTest() {
        
        String testMessage = "Test message";
        new Thread(() -> {
            try {
                alertCustomProducer.produce(topic, testMessage);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        Optional payload = alertCustomConsumer.consume(topic);
        assertThat("Payload must be not empty!", payload.isPresent(), is(true));
        assertThat("Payload must contain test message!", payload.get(), equalTo(testMessage));
    }
}
