package org.example.kafkastreams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.common.metrics.Sensor.RecordingLevel.TRACE;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.kafkainaction.MyFunds;
import org.kafkainaction.MyTransaction;
import org.kafkainaction.MyTransactionResult;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class MyTransactionProcessor {
    
    private final String transactionsInputTopicName;
    private final String transactionSuccessTopicName;
    private final String transactionFailedTopicName;
    private final String fundsStoreName;
    
    public MyTransactionProcessor(final String transactionsInputTopicName, final String transactionSuccessTopicName, final String transactionFailedTopicName, final String fundsStoreName) {
        
        this.transactionsInputTopicName = transactionsInputTopicName;
        this.transactionSuccessTopicName = transactionSuccessTopicName;
        this.transactionFailedTopicName = transactionFailedTopicName;
        this.fundsStoreName = fundsStoreName;
    }
    
    private static boolean success(String account, MyTransactionResult result) {
        return result.getSuccess();
    }
    
    public static void main(String[] args) {
        
        String transactionsInputTopicName = "transaction-request";
        String transactionSuccessTopicName = "transaction-success";
        String transactionFailedTopicName = "transaction-failed";
        String fundsStoreName = "funds-store";
        
        final MyTransactionProcessor myTransactionProcessor = new MyTransactionProcessor(transactionsInputTopicName, transactionSuccessTopicName, transactionFailedTopicName, fundsStoreName);
        
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "transaction-processor");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(METRICS_RECORDING_LEVEL_CONFIG, TRACE.name);
        
        myTransactionProcessor.createTopics(props, transactionsInputTopicName, transactionSuccessTopicName, transactionFailedTopicName);
        
        // Starting point for topology building.
        StreamsBuilder builder = new StreamsBuilder();
        
        // could use default serde config instead
        final SpecificAvroSerde<MyTransaction> transactionRequestAvroSerde = MySchemaSerdes.getSpecificAvroSerde(props);
        final SpecificAvroSerde<MyTransactionResult> transactionResultAvroSerde = MySchemaSerdes.getSpecificAvroSerde(props);
        final SpecificAvroSerde<MyFunds> fundsSerde = MySchemaSerdes.getSpecificAvroSerde(props);
        
        // Create topology consisting of printing, storing into table, transforming and two receiving topics.
        final Topology topology = myTransactionProcessor.createTopology(builder, transactionRequestAvroSerde, transactionResultAvroSerde, fundsSerde);
        
        System.out.println("topology = " + topology.describe().toString());
        
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
        
        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });
        
        try {
            // Clean up local state storages.
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
    
    private void createTopics(Properties p, String... names) {
        
        try (AdminClient client = AdminClient.create(p)) {
            final List<NewTopic> topicList = Arrays.stream(names).map(name -> new NewTopic(name, 6, (short) 1)).collect(Collectors.toList());
            client.createTopics(topicList);
        }
    }
    
    public Topology createTopology(final StreamsBuilder builder, final SpecificAvroSerde<MyTransaction> transactionRequestAvroSerde, final SpecificAvroSerde<MyTransactionResult> transactionResultAvroSerde, final SpecificAvroSerde<MyFunds> fundsSerde) {
        
        final Serde<String> stringSerde = Serdes.String();
        storesBuilder(this.fundsStoreName, stringSerde, fundsSerde);
        
        // 12.1
        // Create KStream object to start processing from the topic.
        final KStream<String, MyTransaction> transactionStream = builder.stream(this.transactionsInputTopicName, Consumed.with(stringSerde, transactionRequestAvroSerde));
        
        // 12.5
        // final GlobalKTable<String, MyTransaction> turnover = builder.globalTable("turnover");
        
        // 12.3
        // Extend topology with printing data (end node).
        transactionStream.print(Printed.<String, MyTransaction>toSysOut().withLabel("transactions logger"));
        
        // 12.4
        // Extend topology with writing data to table (end node).
        transactionStream.toTable(Materialized.<String, MyTransaction, KeyValueStore<Bytes, byte[]>>as("latest-transactions").withKeySerde(stringSerde).withValueSerde(transactionRequestAvroSerde));
        
        // 12.2
        // Extend topology with transforming processor.
        final KStream<String, MyTransactionResult> resultStream = transactionStream.transformValues(new ValueTransformerSupplier<>() {
            @Override
            public ValueTransformer<MyTransaction, MyTransactionResult> get() {
                return new MyTransactionTransformer(fundsStoreName);
            }
            
            @Override
            public Set<StoreBuilder<?>> stores() {
                return Set.of(MyTransactionProcessor.storesBuilder(fundsStoreName, stringSerde, fundsSerde));
            }
        });
        
        /* final KStream<String, TransactionResult> resultStream =  transactionStream.transformValues(() -> new MyTransactionTransformer()); */
        
        // 12.2
        // Redirect successful/failed transactions to different topics.
        resultStream.filter(MyTransactionProcessor::success).to(this.transactionSuccessTopicName, Produced.with(Serdes.String(), transactionResultAvroSerde));
        resultStream.filterNot(MyTransactionProcessor::success).to(this.transactionFailedTopicName, Produced.with(Serdes.String(), transactionResultAvroSerde));
        
        return builder.build();
    }
    
    protected static StoreBuilder<KeyValueStore<String, MyFunds>> storesBuilder(final String storeName, final Serde<String> keySerde, final SpecificAvroSerde<MyFunds> valueSerde) {
        return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), keySerde, valueSerde);
    }
}
