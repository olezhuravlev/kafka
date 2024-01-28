package org.example;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import net.bytebuddy.utility.dispatcher.JavaDispatcher;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
@DirtiesContext
class HelloWorldProducerTest {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.embedded.kafka.brokers}")
    private String bootstrapServers2;
    
    @Autowired
    org.example.components.HelloWorldProducer helloWorldProducer;
    
    @Autowired
    org.example.components.HelloWorldConsumer helloWorldConsumer;
    
    private static final Network network = Network.newNetwork();
    
    protected static GenericContainer ZOOKEEPER = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper:7.5.3"))
        .withNetwork(network)
        .withNetworkAliases("zookeeper-1")
        .withExposedPorts(22181, 22888, 23888)
        .withEnv(Map.of(
            "ZOOKEEPER_SERVER_ID", "1",
            "ZOOKEEPER_CLIENT_PORT", "2181",
            "ZOOKEEPER_TICK_TIME", "2000",
            "ZOOKEEPER_SERVERS", "0.0.0.0:22888:23888"));
    
    public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"))
        .dependsOn(ZOOKEEPER)
        .withNetwork(network)
        .withNetworkAliases("kafka-broker-1")
        .withExposedPorts(9072, 9977)
        .withExternalZookeeper("zookeeper-1:2181")
        .withEnv((Map.of(
            "KAFKA_BROKER_ID", "1",
            "KAFKA_ZOOKEEPER_CONNECT", "zookeeper-1:2181",
            "KAFKA_JMX_PORT", "9997",
            "KAFKA_LISTENERS", "PLAINTEXT://kafka-broker-1:29092",
            "KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka-broker-1:29092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT"
        )));
    
    protected static final GenericContainer SCHEMA_REGISTRY = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.3"))
        .dependsOn(ZOOKEEPER)
        .withNetwork(network)
        .withNetworkAliases("schema-registry")
        .withExposedPorts(8081)
        .withEnv(Map.of(
            "SCHEMA_REGISTRY_HOST_NAME", "schema-registry",
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092",
            "SCHEMA_REGISTRY_LISTENERS", "http://schema-registry:8081"
        ))
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));;
    
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> KAFKA_CONTAINER.getHost() + ":" + KAFKA_CONTAINER.getFirstMappedPort());
        registry.add("spring.kafka.schema-registry.url", () -> "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getFirstMappedPort());
    }
    
    @BeforeAll
    public static final void beforeAll() {
        ZOOKEEPER.start();
        KAFKA_CONTAINER.start();
        SCHEMA_REGISTRY.start();
    }
    
    @Test
    void kafkaTemplateTest() throws Exception {
        
        helloWorldProducer.produce(1);
        helloWorldConsumer.consume();
        
        Thread.sleep(3000);
        
        List<String> consumedMessages = helloWorldConsumer.getConsumedMessages();
        Assertions.assertFalse(consumedMessages.isEmpty(), "Consumed message must be presented!");
    }
}
