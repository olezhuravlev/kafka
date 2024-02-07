package org.example.components.producers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
    private AlertCustomConsumer alertCustomConsumer;
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
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
    @DirtiesContext
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
    @DirtiesContext
    void commitSyncTest() {
        
        String testMessage = "Test message Commit Sync";
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
    @DirtiesContext
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
    @DirtiesContext
    void offsetEarliestTest() {
        
        Properties properties = new Properties();
        String groupId1 = UUID.randomUUID().toString();
        properties.put("group.id", groupId1);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        String testTopic = "offsetEarliestTest";
        List<ProducerRecord<AlertCustom, String>> producerRecords = getProducerRecords(testTopic);
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer1 = getProducer();
                alertCustomProducer1.produce(producerRecords);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        AlertCustomConsumer alertCustomConsumer1 = getConsumer();
        List<Object[]> results1 = alertCustomConsumer1.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Results must have the save amount of records as the source!", results1.size(), is(producerRecords.size()));
        
        for (int i = 0; i < results1.size(); ++i) {
            
            AlertCustom alertCustom = (AlertCustom) results1.get(i)[0];
            String message = (String) results1.get(i)[1];
            
            ProducerRecord<AlertCustom, String> producerRecord = producerRecords.get(i);
            AlertCustom producerRecordAlertCustom = producerRecord.key();
            String producerRecordTestMessage = producerRecord.value();
            
            assertThat("Key of Payload must be equal to test AlertCustom!", producerRecordAlertCustom, equalTo(alertCustom));
            assertThat("Value of Payload must be equal to test message!", producerRecordTestMessage, equalTo(message));
        }
        
        assertThat("Producer callback must be invoked!", AlertCustomProducer.isCallbackInvoked(), equalTo(true));
        
        // Get messages using other groupId.
        AlertCustomConsumer alertCustomConsumer2 = getConsumer();
        properties.put("group.id", UUID.randomUUID().toString());
        List<Object[]> results2 = alertCustomConsumer2.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Results must have the save amount of records as the source!", results2.size(), is(producerRecords.size()));
        
        for (int i = 0; i < results2.size(); ++i) {
            
            AlertCustom alertCustom = (AlertCustom) results2.get(i)[0];
            String message = (String) results2.get(i)[1];
            
            ProducerRecord<AlertCustom, String> producerRecord = producerRecords.get(i);
            AlertCustom producerRecordAlertCustom = producerRecord.key();
            String producerRecordTestMessage = producerRecord.value();
            
            assertThat("Key of Payload must be equal to test AlertCustom!", producerRecordAlertCustom, equalTo(alertCustom));
            assertThat("Value of Payload must be equal to test message!", producerRecordTestMessage, equalTo(message));
        }
        
        // Get messages with initial groupId.
        AlertCustomConsumer alertCustomConsumer3 = getConsumer();
        properties.put("group.id", groupId1);
        List<Object[]> results3 = alertCustomConsumer3.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Repeated message request must return 0 values!", results3.isEmpty(), is(true));
    }
    
    @Test
    @DirtiesContext
    void offsetLatestTest() {
        
        Properties properties = new Properties();
        String groupId1 = UUID.randomUUID().toString();
        properties.put("group.id", groupId1);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        String testTopic = "offsetLatestTest";
        List<ProducerRecord<AlertCustom, String>> producerRecords = getProducerRecords(testTopic);
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer1 = getProducer();
                alertCustomProducer1.produce(producerRecords);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        AlertCustomConsumer alertCustomConsumer1 = getConsumer();
        List<Object[]> results1 = alertCustomConsumer1.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Results must have the save amount of records as the source!", results1.size(), is(producerRecords.size()));
        
        for (int i = 0; i < results1.size(); ++i) {
            
            AlertCustom alertCustom = (AlertCustom) results1.get(i)[0];
            String message = (String) results1.get(i)[1];
            
            ProducerRecord<AlertCustom, String> producerRecord = producerRecords.get(i);
            AlertCustom producerRecordAlertCustom = producerRecord.key();
            String producerRecordTestMessage = producerRecord.value();
            
            assertThat("Key of Payload must be equal to test AlertCustom!", producerRecordAlertCustom, equalTo(alertCustom));
            assertThat("Value of Payload must be equal to test message!", producerRecordTestMessage, equalTo(message));
        }
        
        assertThat("Producer callback must be invoked!", AlertCustomProducer.isCallbackInvoked(), equalTo(true));
        
        // Get messages using other groupId.
        AlertCustomConsumer alertCustomConsumer2 = getConsumer();
        properties.put("group.id", UUID.randomUUID().toString());
        List<Object[]> results2 = alertCustomConsumer2.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Repeated message request must return 0 values!", results2.isEmpty(), is(true));
        
        // Get messages with initial groupId.
        AlertCustomConsumer alertCustomConsumer3 = getConsumer();
        properties.put("group.id", groupId1);
        List<Object[]> results3 = alertCustomConsumer3.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Repeated message request must return 0 values!", results3.isEmpty(), is(true));
    }
    
    @Test
    @DirtiesContext
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
    @DirtiesContext
    @Disabled
    void notFixedOffsetTest() {
        // Not fixed offsets lead to repeated messages returned by broker.
    }
    
    @Test
    @DirtiesContext
    @Disabled
    void offsetForTimesTest() {
        
        // Find first offset.
        // Map<TopicPartition, OffsetAndTimestamp> kaOffsetMap = consumer.offsetsForTimes(timeStampMapper);
        
        // Use obtained map of offsets.
        // consumer.seek(partitionOne, kaOffsetMap.get(partitionOne).offset());
    }
    
    @Test
    @DirtiesContext
    @Disabled
    void messageCoordinatesTest() {
        // groupId => Topic:Partition (Leading Replica):Offset
    }
    
    @Test
    @DirtiesContext
    @Disabled
    void moreConsumersThanPartitionsTest() {
        // One consumer must not have messages received.
    }
    
    @Test
    @DirtiesContext
    @Disabled
    void morePartitionsThenConsumersTest() {
        // One consumer must receive more messages than others.
    }
    
    //////////////////////////////////////////////////////
    // SERVICE FUNCTIONS
    private AlertCustomProducer getProducer() {
        return this.applicationContext.getBean(AlertCustomProducer.class);
    }
    
    private AlertCustomConsumer getConsumer() {
        return this.applicationContext.getBean(AlertCustomConsumer.class);
    }
    
    private List<ProducerRecord<AlertCustom, String>> getProducerRecords(String recordsTopic) {
        
        List<ProducerRecord<AlertCustom, String>> producerRecords = new ArrayList<>();
        
        long timestamp = System.currentTimeMillis() / 1000;
        String testMessage1 = "Test message 1 [" + timestamp + "]";
        AlertCustom testAlert1 = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage1);
        producerRecords.add(new ProducerRecord<>(recordsTopic, testAlert1, testMessage1));
        
        String testMessage2 = "Test message 2 [" + timestamp + "]";
        AlertCustom testAlert2 = new AlertCustom(2, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage2);
        producerRecords.add(new ProducerRecord<>(recordsTopic, testAlert2, testMessage2));
        
        String testMessage3 = "Test message 3 [" + timestamp + "]";
        AlertCustom testAlert3 = new AlertCustom(3, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage3);
        producerRecords.add(new ProducerRecord<>(recordsTopic, testAlert3, testMessage3));
        
        return producerRecords;
    }
}
