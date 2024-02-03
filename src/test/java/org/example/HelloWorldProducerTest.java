package org.example;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
import org.testcontainers.utility.DockerImageName;

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
    
    protected static GenericContainer ZOOKEEPER_CONTAINER = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper:7.5.3"))
        .withNetwork(network)
        .withNetworkAliases("zookeeper-1")
        .withExposedPorts(2181)
        .withEnv(Map.of(
            "ZOOKEEPER_SERVER_ID", "1",
            "ZOOKEEPER_CLIENT_PORT", "2181",
            "ZOOKEEPER_TICK_TIME", "2000",
            "ZOOKEEPER_SERVERS", "0.0.0.0:22888:23888"));
    
    public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.3"))
        .dependsOn(ZOOKEEPER_CONTAINER)
        .withNetwork(network)
        .withNetworkAliases("kafka-broker-1")
        .withExternalZookeeper("zookeeper-1:2181");
    
    protected static final GenericContainer SCHEMA_REGISTRY = new GenericContainer<>(
        DockerImageName.parse("confluentinc/cp-schema-registry:7.5.3"))
        .dependsOn(ZOOKEEPER_CONTAINER)
        .withNetwork(network)
        .withNetworkAliases("schema-registry-1");
    //.withExposedPorts(8081)
    //        .withEnv(Map.of(
    //            "SCHEMA_REGISTRY_HOST_NAME", "schema-registry",
    //            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092",
    //            "SCHEMA_REGISTRY_LISTENERS", "http://schema-registry:8081"
    //        ))
    //        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));;
    
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        
        Integer firstMappedPortZookeeper = ZOOKEEPER_CONTAINER.getFirstMappedPort(); //32946
        List<String> portBindingsZookeeper = ZOOKEEPER_CONTAINER.getPortBindings(); // 0
        List<Integer> exposedPortsZookeeper = ZOOKEEPER_CONTAINER.getExposedPorts(); // 2181
        List<Integer> boundPortNumbersZookeeper = ZOOKEEPER_CONTAINER.getBoundPortNumbers();// 0
        Set<Integer> livenessCheckPortNumbersZookeeper = ZOOKEEPER_CONTAINER.getLivenessCheckPortNumbers();// 32925...32935...32946
        
        Integer firstMappedPortKafka = KAFKA_CONTAINER.getFirstMappedPort(); // 32942...32947
        List<String> portBindingsKafka = KAFKA_CONTAINER.getPortBindings(); // 0
        List<Integer> exposedPortsKafka = KAFKA_CONTAINER.getExposedPorts(); // 9093
        List<Integer> boundPortNumbersKafka = KAFKA_CONTAINER.getBoundPortNumbers(); // 0
        Set<Integer> livenessCheckPortNumbersKafka = KAFKA_CONTAINER.getLivenessCheckPortNumbers(); // 32942...32943...32947
        String bootstrapServersKafka = KAFKA_CONTAINER.getBootstrapServers(); // PLAINTEXT://localhost:32942
        
        //registry.add("spring.kafka.bootstrap-servers", () -> KAFKA_CONTAINER.getHost() + ":" + KAFKA_CONTAINER.getFirstMappedPort());
        //registry.add("spring.kafka.schema-registry.url", () -> "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getFirstMappedPort());
    }
    
    @BeforeAll
    public static final void beforeAll() {
        Stream.of(ZOOKEEPER_CONTAINER, KAFKA_CONTAINER).parallel().forEach(GenericContainer::start);
        //Stream.of(ZOOKEEPER_CONTAINER, KAFKA_CONTAINER, SCHEMA_REGISTRY).parallel().forEach(GenericContainer::start);
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
