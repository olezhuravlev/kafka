package org.example.components.producers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.example.components.consumers.AlertCustomConsumer;
import org.example.configurations.EmbeddedKafkaHolder;
import org.example.messages.AlertCustom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
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
        
        String topic = "commitAutoTest";
        String testMessage = "Test message Auto Commit";
        
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage);
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer = getProducer();
                alertCustomProducer.produce(topic, testMessage, testAlert);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        AlertCustomConsumer alertCustomConsumer = getConsumer();
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
        
        String topic = "commitSyncTest";
        String testMessage = "Test message Commit Sync";
        
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage);
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer = getProducer();
                alertCustomProducer.produce(topic, testMessage, testAlert);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        AlertCustomConsumer alertCustomConsumer = getConsumer();
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
        
        String topic = "commitAsyncTest";
        String testMessage = "Test message Commit Async";
        
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage);
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer = getProducer();
                alertCustomProducer.produce(topic, testMessage, testAlert);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        AlertCustomConsumer alertCustomConsumer = getConsumer();
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
        
        String testTopic = "offsetEarliestTest";
        
        List<ProducerRecord<AlertCustom, String>> producerRecords = getProducerRecordsWithTimestampInMessage(testTopic);
        
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
        
        Properties properties = new Properties();
        String groupId1 = UUID.randomUUID().toString();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
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
        
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
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
        
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1);
        List<Object[]> results3 = alertCustomConsumer3.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Repeated message request must return 0 values!", results3.isEmpty(), is(true));
    }
    
    @Test
    @DirtiesContext
    void offsetLatestTest() {
        
        String testTopic = "offsetLatestTest";
        
        List<ProducerRecord<AlertCustom, String>> producerRecords = getProducerRecordsWithTimestampInMessage(testTopic);
        
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
        
        Properties properties = new Properties();
        String groupId1 = UUID.randomUUID().toString();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
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
        
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        List<Object[]> results2 = alertCustomConsumer2.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Repeated message request must return 0 values!", results2.isEmpty(), is(true));
        
        // Get messages with initial groupId.
        AlertCustomConsumer alertCustomConsumer3 = getConsumer();
        
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1);
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
        // properties.put("max.in.flight.requests.per.connection", "1");
        // properties.put("retries", "3");
    }
    
    @Test
    @DirtiesContext
    void notFixedOffsetTest() {
        
        // Not fixed offsets lead to repeated messages returned by broker.
        
        String testTopic = "notFixedOffsetTest";
        
        List<ProducerRecord<AlertCustom, String>> producerRecords = getProducerRecordsWithTimestampInMessage(testTopic);
        
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
        
        Properties properties = new Properties();
        String groupId1 = UUID.randomUUID().toString();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
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
        
        //////////////////////////////////////////////////////////////////////
        // Attempt2: receiving the same message set with the same Consumer.
        List<Object[]> results12 = alertCustomConsumer1.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Results must have the save amount of records as the source!", results12.size(), is(producerRecords.size()));
        
        for (int i = 0; i < results12.size(); ++i) {
            
            AlertCustom alertCustom = (AlertCustom) results12.get(i)[0];
            String message = (String) results12.get(i)[1];
            
            ProducerRecord<AlertCustom, String> producerRecord = producerRecords.get(i);
            AlertCustom producerRecordAlertCustom = producerRecord.key();
            String producerRecordTestMessage = producerRecord.value();
            
            assertThat("Key of Payload must be equal to test AlertCustom!", producerRecordAlertCustom, equalTo(alertCustom));
            assertThat("Value of Payload must be equal to test message!", producerRecordTestMessage, equalTo(message));
        }
        
        //////////////////////////////////////////////////////////////////////
        // Attempt3: receiving the same message set with other Consumer.
        AlertCustomConsumer alertCustomConsumer2 = getConsumer();
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
    }
    
    @Test
    @DirtiesContext
    void offsetForTimesTest() throws InterruptedException {
        
        //ReentrantLock reentrantLock = new ReentrantLock();
        String testTopic = "offsetForTimesTest";
        
        ///////////////////////////////////////////////////////////////////////////////////
        // PRODUCING
        List<ProducerRecord<AlertCustom, String>> producerRecords1 = getProducerRecordsWithTimestampInMessage(testTopic);
        List<ProducerRecord<AlertCustom, String>> producerRecords2 = getProducerRecordsWithTimestampInMessage(testTopic);
        
        final long[] timestamps = new long[3];
        Thread producerThread = new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer = getProducer();
                
                timestamps[0] = System.currentTimeMillis();
                alertCustomProducer.produce(producerRecords1);
                timestamps[1] = System.currentTimeMillis();
                alertCustomProducer.produce(producerRecords2);
                timestamps[2] = System.currentTimeMillis();
                
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        producerThread.start();
        
        producerThread.join();
        
        ///////////////////////////////////////////////////////////////////////////////////
        // CONSUMING
        AlertCustomConsumer alertCustomConsumer = getConsumer();
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        List<Object[]> results = alertCustomConsumer.consumeRecordsAutoCommit(testTopic, properties);
        assertThat("Results must have the save amount of records as the source!", results.size(),
            is(producerRecords1.size() + producerRecords2.size()));
        
        ///////////////////////////////////////////////////////////////////////////////////
        // GETTING OFFSETS BY TIMESTAMPS
        TopicPartition topicPartition = new TopicPartition(testTopic, 0);
        
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(new TopicPartition(testTopic, 0), timestamps[0]);
        Map<TopicPartition, OffsetAndTimestamp> topicOffsetAndTimestamp_0 = alertCustomConsumer.consumeRecordsOffsetsForTimes(
            timestampsToSearch, properties);
        assertThat("One partition must be presented!", topicOffsetAndTimestamp_0.size(), equalTo(1));
        
        OffsetAndTimestamp offsetAndTimestamp_0 = topicOffsetAndTimestamp_0.get(topicPartition);
        assertThat("Offset must be presented!", offsetAndTimestamp_0, is(notNullValue()));
        
        timestampsToSearch.clear();
        timestampsToSearch.put(new TopicPartition(testTopic, 0), timestamps[1]);
        Map<TopicPartition, OffsetAndTimestamp> topicOffsetAndTimestamp_1 = alertCustomConsumer.consumeRecordsOffsetsForTimes(
            timestampsToSearch, properties);
        assertThat("One partition must be presented!", topicOffsetAndTimestamp_1.size(), equalTo(1));
        
        OffsetAndTimestamp offsetAndTimestamp_1 = topicOffsetAndTimestamp_1.get(topicPartition);
        assertThat("Offset must be presented!", offsetAndTimestamp_1, is(notNullValue()));
        
        assertThat("Offsets must be different!", offsetAndTimestamp_0, not(equalTo(offsetAndTimestamp_1)));
        
        timestampsToSearch.clear();
        timestampsToSearch.put(new TopicPartition(testTopic, 0), timestamps[2]);
        Map<TopicPartition, OffsetAndTimestamp> topicOffsetAndTimestamp_2 = alertCustomConsumer.consumeRecordsOffsetsForTimes(
            timestampsToSearch, properties);
        assertThat("One partition must be presented!", topicOffsetAndTimestamp_2.size(), equalTo(1));
        
        OffsetAndTimestamp offsetAndTimestamp_2 = topicOffsetAndTimestamp_2.get(topicPartition);
        assertThat("Offset must be null!", offsetAndTimestamp_2, is(nullValue()));
        
        ///////////////////////////////////////////////////////////////////////////////////
        // RETRIEVING MESSAGES BY OFFSETS
        List<Object[]> resultsSeek_0 = alertCustomConsumer.seekAndConsumeRecordsAutoCommit(topicPartition, offsetAndTimestamp_0.offset(),
            properties);
        assertThat("Messages of all producing must be retrieved!", resultsSeek_0.size(),
            equalTo(producerRecords1.size() + producerRecords2.size()));
        
        List<Object[]> resultsSeek_1 = alertCustomConsumer.seekAndConsumeRecordsAutoCommit(topicPartition, offsetAndTimestamp_1.offset(),
            properties);
        assertThat("Messages of the last producing must be retrieved!", resultsSeek_1.size(), equalTo(producerRecords2.size()));
    }
    
    //////////////////////////////////////////////////////
    // SERVICE FUNCTIONS
    private AlertCustomProducer getProducer() {
        return this.applicationContext.getBean(AlertCustomProducer.class);
    }
    
    private AlertCustomConsumer getConsumer() {
        return this.applicationContext.getBean(AlertCustomConsumer.class);
    }
    
    private List<ProducerRecord<AlertCustom, String>> getProducerRecordsWithTimestampInMessage(String recordsTopic) {
        
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
