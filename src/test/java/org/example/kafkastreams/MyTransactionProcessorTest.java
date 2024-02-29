package org.example.kafkastreams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.nio.file.Files.createTempDirectory;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.kafkainaction.TransactionType.DEPOSIT;
import static org.kafkainaction.TransactionType.WITHDRAW;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Before;
import org.junit.Test;
import org.kafkainaction.ErrorType;
import org.kafkainaction.MyFunds;
import org.kafkainaction.MyTransaction;
import org.kafkainaction.MyTransactionResult;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class MyTransactionProcessorTest {
    
    private Serde<String> stringSerde;
    private MyTransaction deposit100;
    private MyTransaction withdraw100;
    private MyTransaction withdraw200;
    
    private Properties properties;
    private Topology topology;
    private SpecificAvroSerde<MyTransactionResult> transactionResultSerde;
    private SpecificAvroSerde<MyTransaction> transactionSerde;
    private SpecificAvroSerde<MyFunds> fundsSerde;
    
    private static String transactionsInputTopicName = "transaction-request";
    private static String transactionSuccessTopicName = "transaction-success";
    private static String transactionFailedTopicName = "transaction-failed";
    private static String fundsStoreName = "funds-store";
    
    private static String testAccountNumber = "1";
    
    public MyTransactionProcessorTest() throws IOException {
    }
    
    @Before
    public void setUp() throws IOException {
        
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        properties = new Properties();
        properties.putAll(Map.of(SCHEMA_REGISTRY_URL_CONFIG, "mock://schema-registry.kafkainaction.org:8080", STATE_DIR_CONFIG, createTempDirectory("kafka-streams").toAbsolutePath().toString()) // workaround https://stackoverflow.com/a/50933452/27563
        );
        
        // serdes
        stringSerde = Serdes.String();
        transactionResultSerde = MySchemaSerdes.getSpecificAvroSerde(properties);
        transactionSerde = MySchemaSerdes.getSpecificAvroSerde(properties);
        fundsSerde = MySchemaSerdes.getSpecificAvroSerde(properties);
        
        MyTransactionProcessor myTransactionProcessor = new MyTransactionProcessor(transactionsInputTopicName, transactionSuccessTopicName, transactionFailedTopicName, fundsStoreName);
        topology = myTransactionProcessor.createTopology(streamsBuilder, transactionSerde, transactionResultSerde, fundsSerde);
        
        deposit100 = new MyTransaction(UUID.randomUUID().toString(), testAccountNumber, new BigDecimal(100), DEPOSIT, "USD", "USA");
        withdraw100 = new MyTransaction(UUID.randomUUID().toString(), testAccountNumber, new BigDecimal(100), WITHDRAW, "USD", "USA");
        withdraw200 = new MyTransaction(UUID.randomUUID().toString(), testAccountNumber, new BigDecimal(200), WITHDRAW, "USD", "USA");
    }
    
    @Test
    public void testDriverShouldNotBeNull() {
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, properties)) {
            assertThat(testDriver).isNotNull();
        }
    }
    
    @Test
    public void shouldCreateSuccessfulTransaction() {
        
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, properties)) {
            
            final TestInputTopic<String, MyTransaction> inputTopic = testDriver.createInputTopic(transactionsInputTopicName, stringSerde.serializer(), transactionSerde.serializer());
            
            inputTopic.pipeInput(deposit100.getAccount(), deposit100);
            inputTopic.pipeInput(withdraw100.getAccount(), withdraw100);
            
            final TestOutputTopic<String, MyTransactionResult> outputTopic = testDriver.createOutputTopic(transactionSuccessTopicName, stringSerde.deserializer(), transactionResultSerde.deserializer());
            
            final List<MyTransactionResult> successfulTransactions = outputTopic.readValuesToList();
            // balance should be 0
            final MyTransactionResult transactionResult = successfulTransactions.get(1);
            
            assertThat(transactionResult.getFunds().getBalance()).isEqualByComparingTo(new BigDecimal(0));
        }
    }
    
    @Test
    public void shouldBeInsufficientFunds() {
        
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, properties)) {
            
            final TestInputTopic<String, MyTransaction> inputTopic = testDriver.createInputTopic(transactionsInputTopicName, stringSerde.serializer(), transactionSerde.serializer());
            
            inputTopic.pipeInput(deposit100.getAccount(), deposit100);
            inputTopic.pipeInput(withdraw200.getAccount(), withdraw200);
            
            final TestOutputTopic<String, MyTransactionResult> failedResultOutputTopic = testDriver.createOutputTopic(transactionFailedTopicName, stringSerde.deserializer(), transactionResultSerde.deserializer());
            
            final TestOutputTopic<String, MyTransactionResult> successResultOutputTopic = testDriver.createOutputTopic(transactionSuccessTopicName, stringSerde.deserializer(), transactionResultSerde.deserializer());
            
            final MyTransactionResult successfulDeposit100Result = successResultOutputTopic.readValuesToList().get(0);
            
            assertThat(successfulDeposit100Result.getFunds().getBalance()).isEqualByComparingTo(new BigDecimal(100));
            
            final List<MyTransactionResult> failedTransactions = failedResultOutputTopic.readValuesToList();
            // balance should be 0
            final MyTransactionResult transactionResult = failedTransactions.get(0);
            assertThat(transactionResult.getErrorType()).isEqualTo(ErrorType.INSUFFICIENT_FUNDS);
        }
    }
    
    @Test
    public void balanceShouldBe300() {
        
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, properties)) {
            
            final TestInputTopic<String, MyTransaction> inputTopic = testDriver.createInputTopic(transactionsInputTopicName, stringSerde.serializer(), transactionSerde.serializer());
            
            inputTopic.pipeInput(deposit100.getAccount(), deposit100);
            inputTopic.pipeInput(deposit100.getAccount(), deposit100);
            inputTopic.pipeInput(deposit100.getAccount(), deposit100);
            
            final KeyValueStore<String, MyFunds> store = testDriver.getKeyValueStore(fundsStoreName);
            
            MyFunds funds = store.get(testAccountNumber);
            BigDecimal balance = funds.getBalance();
            assertThat(balance).isEqualByComparingTo(new BigDecimal(300));
        }
    }
}
