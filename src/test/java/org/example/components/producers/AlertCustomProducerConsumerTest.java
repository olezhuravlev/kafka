package org.example.components.producers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.example.components.consumers.AlertCustomConsumer;
import org.example.configurations.EmbeddedKafkaHolder;
import org.example.messages.AlertCustom;
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
    void commitAutoTest() {
        
        String testMessage = "Test message Auto Commit";
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage);
        
        new Thread(() -> {
            try {
                alertCustomProducer.produce(topic, testMessage, testAlert);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        // Payload 1.
        Optional<Object[]> payload = alertCustomConsumer.consumeAutoCommit(topic);
        assertThat("Payload must be not empty!", payload.isPresent(), is(true));
        
        Object[] result = payload.get();
        assertThat("First value of payload must be AlertCustom!", result[0], instanceOf(AlertCustom.class));
        assertThat("Second value of payload must be String!", result[1], instanceOf(String.class));
        
        assertThat("Key of Payload must be equal to test alert!", result[0], equalTo(testAlert));
        assertThat("Value of Payload must be equal to test message!", result[1], equalTo(testMessage));
    }
    
    @Test
    @SneakyThrows
    void commitAsyncTest() {
        
        String testMessage = "Test message Commit Async";
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage);
        
        new Thread(() -> {
            try {
                alertCustomProducer.produce(topic, testMessage, testAlert);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        // Payload 1.
        Optional<Object[]> payload = alertCustomConsumer.consumeCommitAsync(topic);
        assertThat("Payload must be not empty!", payload.isPresent(), is(true));
        
        Object[] result = payload.get();
        assertThat("First value of payload must be AlertCustom!", result[0], instanceOf(AlertCustom.class));
        assertThat("Second value of payload must be String!", result[1], instanceOf(String.class));
        
        assertThat("Key of Payload must be equal to test alert!", result[0], equalTo(testAlert));
        assertThat("Value of Payload must be equal to test message!", result[1], equalTo(testMessage));
    }
}
