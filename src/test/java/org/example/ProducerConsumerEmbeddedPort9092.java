package org.example;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

import java.util.concurrent.TimeUnit;

import org.example.components.consumers.KafkaConsumer;
import org.example.components.producers.KafkaProducer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
@DirtiesContext
@Disabled
public class ProducerConsumerEmbeddedPort9092 {
    
    @Autowired
    private KafkaConsumer kafkaConsumer;
    
    @Autowired
    private KafkaProducer kafkaProducer;
    
    @Value("${topic}")
    private String testTopic;
    
    @Test
    public void kafkaProducerConsumerTest() throws Exception {
        
        String data = "Sending with our own simple KafkaProducer";
        kafkaProducer.send(testTopic, data);
        
        kafkaConsumer.getLatch().await(10, TimeUnit.SECONDS);
        assertThat(kafkaConsumer.getPayload(), containsString(data));
    }
}
