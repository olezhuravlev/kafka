package org.example.components.producers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.components.consumers.AlertCustomConsumer;
import org.example.components.partitioners.AlertCustomTopicNumberPartitioner;
import org.example.configurations.EmbeddedKafkaHolder;
import org.example.messages.AlertCustom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka(topics = "testPartitionsTopic", partitions = 3)
@DirtiesContext
public class AlertCustomProducerConsumerPartitionsTest implements ApplicationContextAware {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertCustomProducerConsumerPartitionsTest.class);
    
    private ApplicationContext applicationContext;
    
    private final static String TEST_TOPIC = "testPartitionsTopic"; // Topic declared for @EmbeddedKafka!
    
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
    @DirtiesContext
    void moreConsumersThanPartitionsTest() throws InterruptedException {
        
        // Some consumers can't receive messages.
        
        CountDownLatch consumersCountDownLatch = new CountDownLatch(1);
        CountDownLatch resultCountDownLatch = new CountDownLatch(6);
        
        ////////////////////////////////////////////////////////////////////////////////
        // PRODUCING
        
        List<ProducerRecord<AlertCustom, String>> producerRecords1 = getProducerRecordsNumberedMessages(TEST_TOPIC);
        List<ProducerRecord<AlertCustom, String>> producerRecords2 = getProducerRecordsNumberedMessages(TEST_TOPIC);
        
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AlertCustomTopicNumberPartitioner.class);
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer1 = getProducer();
                alertCustomProducer1.produce(producerRecords1, producerProperties);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer2 = getProducer();
                alertCustomProducer2.produce(producerRecords2, producerProperties);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        Thread.sleep(1000);
        
        ////////////////////////////////////////////////////////////////////////////////
        // CONSUMING
        
        Properties consumerProperties = new Properties();
        String groupId1 = UUID.randomUUID().toString();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        //consumerProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        
        List<Object[]> results_1 = new ArrayList<>();
        new Thread(() -> {
            try {
                AlertCustomConsumer alertCustomConsumer1 = getConsumer();
                consumersCountDownLatch.await();
                List<Object[]> results = alertCustomConsumer1.consumeAutoCommitDynamicPartitions(TEST_TOPIC, consumerProperties);
                results_1.addAll(results);
                resultCountDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        List<Object[]> results_2 = new ArrayList<>();
        new Thread(() -> {
            try {
                AlertCustomConsumer alertCustomConsumer1 = getConsumer();
                consumersCountDownLatch.await();
                List<Object[]> results = alertCustomConsumer1.consumeAutoCommitDynamicPartitions(TEST_TOPIC, consumerProperties);
                results_2.addAll(results);
                resultCountDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        List<Object[]> results_3 = new ArrayList<>();
        new Thread(() -> {
            try {
                AlertCustomConsumer alertCustomConsumer2 = getConsumer();
                consumersCountDownLatch.await();
                List<Object[]> results = alertCustomConsumer2.consumeAutoCommitDynamicPartitions(TEST_TOPIC, consumerProperties);
                results_3.addAll(results);
                resultCountDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        List<Object[]> results_4 = new ArrayList<>();
        new Thread(() -> {
            try {
                AlertCustomConsumer alertCustomConsumer2 = getConsumer();
                consumersCountDownLatch.await();
                List<Object[]> results = alertCustomConsumer2.consumeAutoCommitDynamicPartitions(TEST_TOPIC, consumerProperties);
                results_4.addAll(results);
                resultCountDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        List<Object[]> results_5 = new ArrayList<>();
        new Thread(() -> {
            try {
                AlertCustomConsumer alertCustomConsumer3 = getConsumer();
                consumersCountDownLatch.await();
                List<Object[]> results = alertCustomConsumer3.consumeAutoCommitDynamicPartitions(TEST_TOPIC, consumerProperties);
                results_5.addAll(results);
                resultCountDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        List<Object[]> results_6 = new ArrayList<>();
        new Thread(() -> {
            try {
                AlertCustomConsumer alertCustomConsumer3 = getConsumer();
                consumersCountDownLatch.await();
                List<Object[]> results = alertCustomConsumer3.consumeAutoCommitDynamicPartitions(TEST_TOPIC, consumerProperties);
                results_6.addAll(results);
                resultCountDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        Thread.sleep(1000);
        
        consumersCountDownLatch.countDown();
        resultCountDownLatch.await();
        assertThat("Results must have the save amount of records as the sources!",
            results_1.size() + results_2.size() + results_3.size() + results_4.size() + results_5.size() + results_6.size(),
            is(producerRecords1.size() + producerRecords2.size()));
        
        int emptyCollectionsCounter = 0;
        if (results_1.isEmpty()) {
            ++emptyCollectionsCounter;
        }
        if (results_2.isEmpty()) {
            ++emptyCollectionsCounter;
        }
        if (results_3.isEmpty()) {
            ++emptyCollectionsCounter;
        }
        if (results_4.isEmpty()) {
            ++emptyCollectionsCounter;
        }
        if (results_5.isEmpty()) {
            ++emptyCollectionsCounter;
        }
        if (results_6.isEmpty()) {
            ++emptyCollectionsCounter;
        }
        assertThat("At least half of collections must be empty!", emptyCollectionsCounter, greaterThanOrEqualTo(3));
    }
    
    @Test
    @DirtiesContext
    void morePartitionsThenConsumersTest() throws InterruptedException {
        
        // Some consumers receive more messages than others.
        
        CountDownLatch consumersCountDownLatch = new CountDownLatch(1);
        CountDownLatch resultCountDownLatch = new CountDownLatch(2);
        
        ////////////////////////////////////////////////////////////////////////////////
        // PRODUCING
        
        List<ProducerRecord<AlertCustom, String>> producerRecords1 = getProducerRecordsNumberedMessages(TEST_TOPIC);
        List<ProducerRecord<AlertCustom, String>> producerRecords2 = getProducerRecordsNumberedMessages(TEST_TOPIC);
        
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AlertCustomTopicNumberPartitioner.class);
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer1 = getProducer();
                alertCustomProducer1.produce(producerRecords1, producerProperties);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer2 = getProducer();
                alertCustomProducer2.produce(producerRecords2, producerProperties);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        Thread.sleep(1000);
        
        ////////////////////////////////////////////////////////////////////////////////
        // CONSUMING
        
        Properties consumerProperties = new Properties();
        String groupId1 = UUID.randomUUID().toString();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        //consumerProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        
        List<Object[]> results_1 = new ArrayList<>();
        new Thread(() -> {
            try {
                AlertCustomConsumer alertCustomConsumer1 = getConsumer();
                consumersCountDownLatch.await();
                List<Object[]> results = alertCustomConsumer1.consumeAutoCommitDynamicPartitions(TEST_TOPIC, consumerProperties);
                results_1.addAll(results);
                resultCountDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        List<Object[]> results_2 = new ArrayList<>();
        new Thread(() -> {
            try {
                AlertCustomConsumer alertCustomConsumer1 = getConsumer();
                consumersCountDownLatch.await();
                List<Object[]> results = alertCustomConsumer1.consumeAutoCommitDynamicPartitions(TEST_TOPIC, consumerProperties);
                results_2.addAll(results);
                resultCountDownLatch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        Thread.sleep(1000);
        
        consumersCountDownLatch.countDown();
        resultCountDownLatch.await();
        assertThat("Results must have the save amount of records as the sources!",
            results_1.size() + results_2.size(),
            is(producerRecords1.size() + producerRecords2.size()));
        
        assertThat("Consumers must have different amount of received messages!",
            results_1.size() == results_2.size(),
            is(false));
    }
    
    //////////////////////////////////////////////////////
    // SERVICE FUNCTIONS
    private AlertCustomProducer getProducer() {
        return this.applicationContext.getBean(AlertCustomProducer.class);
    }
    
    private AlertCustomConsumer getConsumer() {
        return this.applicationContext.getBean(AlertCustomConsumer.class);
    }
    
    private List<ProducerRecord<AlertCustom, String>> getProducerRecordsNumberedMessages(String recordsTopic) {
        
        List<ProducerRecord<AlertCustom, String>> producerRecords = new ArrayList<>();
        
        String testMessage1 = "0";
        AlertCustom testAlert1 = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage1);
        producerRecords.add(new ProducerRecord<>(recordsTopic, testAlert1, testMessage1));
        
        String testMessage2 = "1";
        AlertCustom testAlert2 = new AlertCustom(2, "Stage 3", AlertCustom.AlertLevel.CRITICAL, testMessage2);
        producerRecords.add(new ProducerRecord<>(recordsTopic, testAlert2, testMessage2));
        
        String testMessage3 = "2";
        AlertCustom testAlert3 = new AlertCustom(3, "Stage 4", AlertCustom.AlertLevel.CRITICAL, testMessage3);
        producerRecords.add(new ProducerRecord<>(recordsTopic, testAlert3, testMessage3));
        
        return producerRecords;
    }
}
