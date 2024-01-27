package org.example.configurations;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

public class EmbeddedKafkaHolder {
    private static EmbeddedKafkaBroker embeddedKafkaBroker = new EmbeddedKafkaZKBroker(1, false);
    private static boolean started;
    
    private EmbeddedKafkaHolder() {
        super();
    }
    
    public static EmbeddedKafkaBroker getEmbeddedKafkaBroker() {
        
        if (started) {
            return embeddedKafkaBroker;
        }
        
        try {
            embeddedKafkaBroker.afterPropertiesSet();
        } catch (Exception e) {
            throw new KafkaException("Embedded Kafka broker failed to start", e);
        }
        
        started = true;
        return embeddedKafkaBroker;
    }
}
