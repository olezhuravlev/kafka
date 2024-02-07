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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import lombok.SneakyThrows;

@SpringBootTest
@EmbeddedKafka
@DirtiesContext
public class AlertCustomProducerConsumerTest implements ApplicationContextAware {
    
    private ApplicationContext applicationContext;
    
    @Value(value = "${topic}")
    private String topic;
    
    @Autowired
    private AlertCustomProducer alertCustomProducer;
    
    @Autowired
    private AlertCustomProducer alertCustomProducer2;
    
    @Autowired
    private AlertCustomProducer alertCustomProducer3;
    
    @Autowired
    private AlertCustomConsumer alertCustomConsumer;
    
    @Autowired
    private AlertCustomConsumer alertCustomConsumer2;
    
    @BeforeAll
    public static void beforeAll() {
        //EmbeddedKafkaHolder.getEmbeddedKafkaBroker().addTopics("hat", "cat");
    }
    
    @AfterAll
    public static void afterAll() {
        EmbeddedKafkaHolder.getEmbeddedKafkaBroker().destroy();
    }
    
    @BeforeEach
    public void beforeEach() {
        AlertCustomProducer.dropCallbackInvoked();
        AlertCustomConsumer.dropCallbackInvoked();
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
        
        assertThat("Producer callback must be invoked!", AlertCustomProducer.isCallbackInvoked(), equalTo(true));
    }
    
    @Test
    @SneakyThrows
    void commitSyncTest() {
        
        String testMessage = "Test message Commit Sync";
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage);
        
        new Thread(() -> {
            try {
                alertCustomProducer2.produce(topic, testMessage, testAlert);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        // Payload 1.
        Optional<Object[]> payload = alertCustomConsumer.consumeCommitSync(topic);
        assertThat("Payload must be not empty!", payload.isPresent(), is(true));
        
        Object[] result = payload.get();
        assertThat("First value of payload must be AlertCustom!", result[0], instanceOf(AlertCustom.class));
        assertThat("Second value of payload must be String!", result[1], instanceOf(String.class));
        
        assertThat("Key of Payload must be equal to test alert!", result[0], equalTo(testAlert));
        assertThat("Value of Payload must be equal to test message!", result[1], equalTo(testMessage));
        
        assertThat("Producer callback must be invoked!", AlertCustomProducer.isCallbackInvoked(), equalTo(true));
    }
    
    @Test
    @SneakyThrows
    void commitAsyncTest() {
        
        String testMessage = "Test message Commit Async";
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage);
        
        new Thread(() -> {
            try {
                alertCustomProducer2.produce(topic, testMessage, testAlert);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        AlertCustomConsumer alertCustomConsumer1 = getConsumer();
        AlertCustomConsumer alertCustomConsumer2 = getConsumer();
        
        AlertCustomConsumer alertCustomConsumer1_1 = alertCustomConsumer1.getConsumerLookup();
        AlertCustomConsumer alertCustomConsumer1_2 = getConsumer().getConsumerLookup();
        
        // Doesn't work.
        AlertCustomConsumer alertCustomConsumer3 = AlertCustomConsumer.getConsumerLookupStatic();
        AlertCustomConsumer alertCustomConsumer4 = AlertCustomConsumer.getConsumerLookupStatic();
        AlertCustomConsumer alertCustomConsumer5 = getConsumerLookup();
        AlertCustomConsumer alertCustomConsumer6 = getConsumerLookup();
        
        // Payload 1.
        alertCustomConsumer.consumeCommitAsync(topic, consumerRecord -> {
            
            Optional<Object[]> payload = Optional.of(new Object[] { consumerRecord.key(), consumerRecord.value() });
            assertThat("Payload must be not empty!", payload.isPresent(), is(true));
            
            Object[] result = payload.get();
            assertThat("First value of payload must be AlertCustom!", result[0], instanceOf(AlertCustom.class));
            assertThat("Second value of payload must be String!", result[1], instanceOf(String.class));
            
            assertThat("Key of Payload must be equal to test alert!", result[0], equalTo(testAlert));
            assertThat("Value of Payload must be equal to test message!", result[1], equalTo(testMessage));
            assertThat("Consumer callback must be invoked!", AlertCustomConsumer.isCallbackInvoked(), equalTo(true));
            assertThat("Producer callback must be invoked!", AlertCustomProducer.isCallbackInvoked(), equalTo(true));
        });
    }
    
    @Test
    @Disabled
    void ackTest() {
        
        // Strict sequential dispatch.
        // RecordMetadata result = producer.send(producerRecord).get(); // Wait for the responce!
        // producer.close();
        
        // Properties properties = new Properties();
        // properties.put("acks", "all");
        // properties.put("retries", "3");
        // properties.put("max.in.flight.requests.per.connection", "1");
    }
    
    @Test
    @Disabled
    void offsetEarliestTest() {
        // Not fixed offsets lead to repeated messages returned by broker.
        // Properties properties = new Properties();
        // properties.put("group.id", UUID.randomUUID().toString());
        // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
    
    @Test
    @Disabled
    void offsetLatestTest() {
        // Properties properties = new Properties();
        // properties.put("group.id", UUID.randomUUID().toString());
        // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }
    
    @Test
    @Disabled
    void offsetForTimesTest() {
        
        // Find first offset.
        // Map<TopicPartition, OffsetAndTimestamp> kaOffsetMap = consumer.offsetsForTimes(timeStampMapper);
        
        // Use obtained map of offsets.
        // consumer.seek(partitionOne, kaOffsetMap.get(partitionOne).offset());
    }
    
    @Test
    @Disabled
    void messageCoordinatesTest() {
        // groupId => Topic:Partition (Leading Replica):Offset
    }
    
    @Test
    @Disabled
    void moreConsumersThanPartitionsTest() {
        // One consumer must not have messages received.
    }
    
    @Test
    @Disabled
    void morePartitionsThenConsumersTest() {
        // One consumer must receive more messages than others.
    }
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    private AlertCustomConsumer getConsumer() {
        return this.applicationContext.getBean(AlertCustomConsumer.class);
    }
    
    @Lookup
    private AlertCustomConsumer getConsumerLookup() {
        return null;
    }
}
