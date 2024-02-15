package org.example.components.producers;

import static org.example.components.consumers.AlertCustomConsumerMetricsInterceptor.CONSUME_TIME_HEADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.example.components.consumers.AlertCustomConsumer;
import org.example.configurations.EmbeddedKafkaHolder;
import org.example.messages.AlertCustom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.BeansException;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import lombok.SneakyThrows;

@SpringBootTest
@EmbeddedKafka(controlledShutdown = true)
@DirtiesContext
@RunWith(SpringRunner.class)
@ExtendWith(MockitoExtension.class)
public class AlertCustomProducerInterceptorTest implements ApplicationContextAware {
    
    private ApplicationContext applicationContext;
    
    @SpyBean
    private AlertCustomProducerMetricsInterceptor alertCustomProducerMetricsInterceptor;
    
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
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    @SneakyThrows
    @DirtiesContext
    void producerInterceptorTest() {
        
        String topic = "producerInterceptorTest";
        String testMessage = "Test message Producer Interceptor Test";
        
        AlertCustom testAlert = new AlertCustom(1, "Stage 1", AlertCustom.AlertLevel.CRITICAL, testMessage);
        
        new Thread(() -> {
            try {
                AlertCustomProducer alertCustomProducer = getProducer();
                alertCustomProducer.produceWithInterceptor(topic, testMessage, testAlert);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        
        AlertCustomConsumer alertCustomConsumer = getConsumer();
        Optional<Object[]> payload = alertCustomConsumer.consumeAutoCommit(topic);
        assertThat("Payload must be not empty!", payload.isPresent(), is(true));
        
        verify(alertCustomProducerMetricsInterceptor, atLeastOnce()).onSend(any(ProducerRecord.class));
        
        // Custom consumer interceptor modifies received records by adding a new header - CONSUME_TIME_HEADER.
        // We just check presence of the header to check for the interceptor was invoked.
        Object[] content = payload.get();
        RecordHeaders recordHeaders = (RecordHeaders) content[2];
        Iterable<Header> headerIterable = recordHeaders.headers(CONSUME_TIME_HEADER);
        assertThat("Header must be presented", headerIterable.iterator().hasNext(), is(true));
    }
    
    //////////////////////////////////////////////////////
    // SERVICE FUNCTIONS
    private AlertCustomProducer getProducer() {
        return this.applicationContext.getBean(AlertCustomProducer.class);
    }
    
    private AlertCustomConsumer getConsumer() {
        return this.applicationContext.getBean(AlertCustomConsumer.class);
    }
}
