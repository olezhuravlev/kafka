package org.example;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasKey;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasPartition;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
@DirtiesContext
public class KafkaTemplateEmbeddedPort9092 {
    
    @Test
    public void test(EmbeddedKafkaBroker broker) {
        String brokerList = broker.getBrokersAsString(); // localhost:42789
        assertThat(brokerList, not(emptyString()));
    }
    
    @Test
    public void testTemplate(EmbeddedKafkaBroker broker) throws Exception {
        
        String topic = "testTemplate";
        
        //String brokerList = broker.getBrokersAsString(); // localhost:42789
        //broker.addTopics(TEST_TOPIC);
        //broker.afterPropertiesSet();
        //EmbeddedKafkaBroker broker = embeddedKafka.getEmbeddedKafka();
        String brokerList = broker.getBrokersAsString(); // localhost:42789
        
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", broker);
        DefaultKafkaConsumerFactory<Integer, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        ContainerProperties containerProperties = new ContainerProperties(topic);
        KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(consumerFactory,
            containerProperties);
        final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();
        container.setupMessageListener(new MessageListener<Integer, String>() {
            @Override
            public void onMessage(ConsumerRecord<Integer, String> record) {
                System.out.println(record);
                records.add(record);
            }
        });
        
        container.setBeanName("templateTests");
        container.start();
        
        ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        
        ProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(topic);
        template.sendDefault("foo");
        assertThat(records.poll(10, TimeUnit.SECONDS), hasValue("foo"));
        
        template.sendDefault(0, 2, "bar");
        ConsumerRecord<Integer, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasPartition(0));
        assertThat(received, hasKey(2));
        assertThat(received, hasValue("bar"));
        
        template.send(topic, 0, 2, "baz");
        received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasPartition(0));
        assertThat(received, hasKey(2));
        assertThat(received, hasValue("baz"));
    }
}
