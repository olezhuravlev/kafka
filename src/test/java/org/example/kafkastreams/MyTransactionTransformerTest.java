package org.example.kafkastreams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.kafkainaction.ErrorType.INSUFFICIENT_FUNDS;
import static org.kafkainaction.TransactionType.DEPOSIT;
import static org.kafkainaction.TransactionType.WITHDRAW;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.Before;
import org.junit.Test;
import org.kafkainaction.MyFunds;
import org.kafkainaction.MyTransaction;
import org.kafkainaction.MyTransactionResult;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class MyTransactionTransformerTest {
    
    private KeyValueStore<String, MyFunds> fundsStore;
    private MockProcessorContext mockContext;
    private ValueTransformer<MyTransaction, MyTransactionResult> transactionTransformer;
    final static Map<String, String> testConfig = Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:8080", APPLICATION_ID_CONFIG, "mytest", SCHEMA_REGISTRY_URL_CONFIG, "mock://schema-registry.kafkainaction.org:8080");
    
    @Before
    @SuppressWarnings("deprecation")
    public void setup() {
        final Properties properties = new Properties();
        properties.putAll(testConfig);
        mockContext = new MockProcessorContext(properties);
        
        final SpecificAvroSerde<MyFunds> fundsSpecificAvroSerde = MySchemaSerdes.getSpecificAvroSerde(properties);
        
        final Serde<String> stringSerde = Serdes.String();
        final String fundsStoreName = "fundsStore";
        this.fundsStore = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(fundsStoreName), stringSerde, fundsSpecificAvroSerde).withLoggingDisabled()    // Changelog is not supported by MockProcessorContext.
            .build();
        
        // might do this a different way
        fundsStore.init(mockContext, fundsStore);
        mockContext.register(fundsStore, null);
        
        transactionTransformer = new MyTransactionTransformer(fundsStoreName);
        transactionTransformer.init(mockContext);
    }
    
    @Test
    public void shouldStoreTransaction() {
        final MyTransaction transaction = new MyTransaction(UUID.randomUUID().toString(), "1", new BigDecimal(100), DEPOSIT, "USD", "USA");
        final MyTransactionResult transactionResult = transactionTransformer.transform(transaction);
        
        assertThat(transactionResult.getSuccess()).isTrue();
        
    }
    
    @Test
    public void shouldHaveInsufficientFunds() {
        final MyTransaction transaction = new MyTransaction(UUID.randomUUID().toString(), "1", new BigDecimal("100"), WITHDRAW, "RUR", "Russia");
        final MyTransactionResult result = transactionTransformer.transform(transaction);
        
        assertThat(result.getSuccess()).isFalse();
        assertThat(result.getErrorType()).isEqualTo(INSUFFICIENT_FUNDS);
    }
    
    @Test
    public void shouldHaveEnoughFunds() {
        final MyTransaction transaction1 = new MyTransaction(UUID.randomUUID().toString(), "1", new BigDecimal("300"), DEPOSIT, "RUR", "Russia");
        
        final MyTransaction transaction2 = new MyTransaction(UUID.randomUUID().toString(), "1", new BigDecimal("200"), WITHDRAW, "RUR", "Russia");
        transactionTransformer.transform(transaction1);
        final MyTransactionResult result = transactionTransformer.transform(transaction2);
        
        assertThat(result.getSuccess()).isTrue();
        assertThat(result.getErrorType()).isNull();
    }
}
