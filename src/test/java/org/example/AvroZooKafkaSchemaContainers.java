package org.example;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.example.components.AvroConsumer;
import org.example.components.AvroProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@DirtiesContext
public class AvroZooKafkaSchemaContainers {
    
    @Autowired
    AvroProducer avroProducer;
    
    @Autowired
    AvroConsumer avroConsumer;
    
    private static final Network network = Network.newNetwork();
    
    protected static GenericContainer ZOOKEEPER_CONTAINER = new GenericContainer<>(
        DockerImageName.parse("confluentinc/cp-zookeeper:7.5.3"))
        .withNetwork(network)
        .withNetworkAliases("zookeeper-1")
        .withExposedPorts(2181)
        .withEnv(Map.of(
            "ZOOKEEPER_CLIENT_PORT", "2181",
            "ZOOKEEPER_TICK_TIME", "2000"
        ));
    
    public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.3"))
        .dependsOn(ZOOKEEPER_CONTAINER)
        .withNetwork(network)
        .withNetworkAliases("kafka-broker-1")
        .withExposedPorts(9092, 9093)
        .withExternalZookeeper("zookeeper-1:2181");
    
    protected static final GenericContainer SCHEMA_REGISTRY_CONTAINER = new GenericContainer<>(
        DockerImageName.parse("confluentinc/cp-schema-registry:7.5.3"))
        .dependsOn(ZOOKEEPER_CONTAINER, KAFKA_CONTAINER)
        .withNetwork(network)
        .withNetworkAliases("schema-registry-1")
        .withExposedPorts(8081)
        .withEnv(Map.of(
            "SCHEMA_REGISTRY_HOST_NAME", "schema-registry-1",
            "SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081",
            "SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "PLAINTEXT"
            )
        )
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
    
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
    
        // Integer firstMappedPortZookeeper = ZOOKEEPER_CONTAINER.getFirstMappedPort(); //32946
        // List<String> portBindingsZookeeper = ZOOKEEPER_CONTAINER.getPortBindings(); // 0
        // List<Integer> exposedPortsZookeeper = ZOOKEEPER_CONTAINER.getExposedPorts(); // 2181
        // List<Integer> boundPortNumbersZookeeper = ZOOKEEPER_CONTAINER.getBoundPortNumbers();// 0
        // Set<Integer> livenessCheckPortNumbersZookeeper = ZOOKEEPER_CONTAINER.getLivenessCheckPortNumbers();// 32925...32935...32946

        // Integer firstMappedPortKafka = KAFKA_CONTAINER.getFirstMappedPort(); // 32942...32947
        // List<String> portBindingsKafka = KAFKA_CONTAINER.getPortBindings(); // 0
        // List<Integer> exposedPortsKafka = KAFKA_CONTAINER.getExposedPorts(); // 9093
        // List<Integer> boundPortNumbersKafka = KAFKA_CONTAINER.getBoundPortNumbers(); // 0
        // Set<Integer> livenessCheckPortNumbersKafka = KAFKA_CONTAINER.getLivenessCheckPortNumbers(); // 32942...32943...32947
        // String bootstrapServersKafka = KAFKA_CONTAINER.getBootstrapServers(); // PLAINTEXT://localhost:32942
        
        // Integer firstMappedPortRegistry = SCHEMA_REGISTRY_CONTAINER.getFirstMappedPort(); //
        // List<String> portBindingsRegistry = SCHEMA_REGISTRY_CONTAINER.getPortBindings(); //
        // List<Integer> exposedPortsRegistry = SCHEMA_REGISTRY_CONTAINER.getExposedPorts(); //
        // List<Integer> boundPortNumbersRegistry = SCHEMA_REGISTRY_CONTAINER.getBoundPortNumbers(); //
        // Set<Integer> livenessCheckPortNumbersRegistry = SCHEMA_REGISTRY_CONTAINER.getLivenessCheckPortNumbers(); //
        
        registry.add("spring.kafka.bootstrap-servers", () -> KAFKA_CONTAINER.getBootstrapServers());
        registry.add("spring.kafka.schema-registry.url", () -> "http://" + SCHEMA_REGISTRY_CONTAINER.getHost() + ":" + SCHEMA_REGISTRY_CONTAINER.getFirstMappedPort());
    }
    
    @BeforeAll
    public static final void beforeAll() {
        Stream.of(ZOOKEEPER_CONTAINER, KAFKA_CONTAINER).parallel().forEach(GenericContainer::start);
        SCHEMA_REGISTRY_CONTAINER.addEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",  "PLAINTEXT://" + KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092");
        SCHEMA_REGISTRY_CONTAINER.start();
    }
    
    @Test
    void avroProducerConsumerTest() {
        
        new Thread(() -> avroProducer.produce(1)).start();
        avroConsumer.consume();
        
        List<String> consumedMessages = avroConsumer.getConsumedMessages();
        Assertions.assertFalse(consumedMessages.isEmpty(), "Consumed message must be presented!");
    }
}
