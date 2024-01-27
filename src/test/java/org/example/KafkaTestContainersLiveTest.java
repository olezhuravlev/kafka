package org.example;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.components.KafkaConsumer;
import org.example.components.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Import(KafkaTestContainersLiveTest.KafkaTestContainersConfiguration.class)
@TestPropertySource(locations = "classpath:application.properties")
@DirtiesContext
public class KafkaTestContainersLiveTest {
    
    public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));
    
    @Autowired
    private KafkaConsumer kafkaConsumer;
    
    @Autowired
    private KafkaProducer kafkaProducer;
    
    @Value("${topic}")
    private String testTopic;
    
    @BeforeAll
    public static final void beforeAll() {
        KAFKA_CONTAINER.start();
    }
    
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", new Supplier<Object>() {
            @Override
            public Object get() {
                return KAFKA_CONTAINER.getHost() + ":" + KAFKA_CONTAINER.getFirstMappedPort();
            }
        });
    }
    
    @Test
    public void testUsage() throws Exception {
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        assertThat(bootstrapServers, not(emptyString()));
    }
    
    @TestConfiguration
    static class KafkaTestContainersConfiguration {
        
        @Bean
        ConcurrentKafkaListenerContainerFactory<Integer, String> concurrentKafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(testConsumerFactory());
            return factory;
        }
        
        @Bean
        public ConsumerFactory<Integer, String> testConsumerFactory() {
            return new DefaultKafkaConsumerFactory<>(testConsumerConfigs());
        }
        
        @Bean
        public Map<String, Object> testConsumerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "baeldung");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return props;
        }
        
        @Bean
        public ProducerFactory<String, String> testProducerFactory() {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(configProps);
        }
        
        @Bean
        public KafkaTemplate<String, String> testKafkaTemplate() {
            return new KafkaTemplate<>(testProducerFactory());
        }
    }
    
    @Test
    public void givenKafkaDockerContainer_whenSendingWithSimpleProducer_thenMessageReceived() throws Exception {
        
        String data = "Sending with our own simple KafkaProducer";
        kafkaProducer.send(testTopic, data);
        
        boolean messageConsumed = kafkaConsumer.getLatch().await(10, TimeUnit.SECONDS);
        assertTrue(messageConsumed);
        assertThat(kafkaConsumer.getPayload(), containsString(data));
    }
}
