package org.example;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.example.components.KafkaConsumer;
import org.example.components.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
@DirtiesContext
public class EmbeddedKafkaIntegrationTest {
    
    @Autowired
    private KafkaConsumer kafkaConsumer;
    
    @Autowired
    private KafkaProducer kafkaProducer;
    
    @Value("${topic}")
    private String testTopic;
    
    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithSimpleProducer_thenMessageReceived() throws Exception {
        
        String data = "Sending with our own simple KafkaProducer";
        
        kafkaProducer.send(testTopic, data);
        
        boolean messageConsumed = kafkaConsumer.getLatch().await(10, TimeUnit.SECONDS);
        assertTrue(messageConsumed);
        assertThat(kafkaConsumer.getPayload(), containsString(data));
    }
}
