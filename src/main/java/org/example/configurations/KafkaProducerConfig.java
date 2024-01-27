package org.example.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaProducerConfig {
    
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;
    
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }
    
    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
    
    @Bean("kafkaTemplate")
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    @Bean
    public KafkaTemplate<String, String> stringTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
    
    //    @Bean
    //    public KafkaTemplate<String, byte[]> bytesTemplate(ProducerFactory<String, byte[]> pf) {
    //        return new KafkaTemplate<>(pf,
    //            Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    //    }
    
    //    @Bean
    //    public ProducerFactory<String, String> producerFactory() {
    //        Map<String, Object> configProps = new HashMap<>();
    //        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    //        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    //        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    //        return new DefaultKafkaProducerFactory<>(configProps);
    //    }
    
    @Bean
    public ProducerFactory<String, String> greetingProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
    
    //    @Bean
    //    public ProducerFactory<String, Object> multiTypeProducerFactory() {
    //        Map<String, Object> configProps = new HashMap<>();
    //        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    //        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    //        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    //        configProps.put(JsonSerializer.TYPE_MAPPINGS, "greeting:org.example.messages.Greeting, farewell:org.example.messages.Farewell");
    //        return new DefaultKafkaProducerFactory<>(configProps);
    //    }
    
    @Bean("greetingKafkaTemplate")
    public KafkaTemplate<String, String> greetingkafkaTemplate() {
        return new KafkaTemplate<>(greetingProducerFactory());
    }
    
    //    @Bean("multiTypeKafkaTemplate")
    //    public KafkaTemplate<String, Object> multiTypeKafkaTemplate() {
    //        return new KafkaTemplate<>(multiTypeProducerFactory());
    //    }
}
