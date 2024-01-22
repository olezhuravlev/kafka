package org.example.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.messages.Farewell;
import org.example.messages.Greeting;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

//@EnableKafka
//@Configuration
public class KafkaConsumerConfig {

//    @Value(value = "${spring.kafka.bootstrap-servers}")
//    private String bootstrapAddress;
//
//    @Value(value = "${spring.kafka.groupId}")
//    private String groupId;
//
//    @Bean
//    public ConsumerFactory<String, String> consumerFactory() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        return new DefaultKafkaConsumerFactory<>(props);
//    }
//
//    @Bean
//    public ConsumerFactory<String, Greeting> greetingConsumerFactory() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>());
//    }
//
//    @Bean
//    public ConsumerFactory<String, Object> multiTypeConsumerFactory() {
//        HashMap<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//        return new DefaultKafkaConsumerFactory<>(props);
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
//
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        return factory;
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> filterKafkaListenerContainerFactory() {
//
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.setRecordFilterStrategy(record -> record.value().contains("World"));
//        return factory;
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, Greeting> greetingKafkaListenerContainerFactory() {
//
//        ConcurrentKafkaListenerContainerFactory<String, Greeting> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(greetingConsumerFactory());
//        return factory;
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, Object> multiTypeKafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(multiTypeConsumerFactory());
//        factory.setRecordMessageConverter(multiTypeConverter());
//        return factory;
//    }
//
//    @Bean
//    public RecordMessageConverter multiTypeConverter() {
//
//        StringJsonMessageConverter converter = new StringJsonMessageConverter();
//        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
//        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
//        typeMapper.addTrustedPackages("org.example");
//
//        Map<String, Class<?>> mappings = new HashMap<>();
//        mappings.put("greeting", Greeting.class);
//        mappings.put("farewell", Farewell.class);
//        typeMapper.setIdClassMapping(mappings);
//        converter.setTypeMapper(typeMapper);
//
//        return converter;
//    }
//
//    @KafkaListener(topics = "topicName", groupId = "myGroupId")
//    public void listenGroupFoo(String message) {
//        System.out.println("Received Message in group 'myGroupId': " + message);
//    }
//
//    @KafkaListener(topics = "topicName")
//    public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
//        System.out.println("Received Message: " + message + "from partition: " + partition);
//    }
//
//    @KafkaListener(topicPartitions = @TopicPartition(topic = "topicName", partitionOffsets = {
//        @PartitionOffset(partition = "0", initialOffset = "0"), @PartitionOffset(partition = "3", initialOffset = "0")
//    }), containerFactory = "partitionsKafkaListenerContainerFactory")
//    public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
//        System.out.println("Received Message: " + message + "from partition: " + partition);
//    }
//
//    @KafkaListener(topics = "topicName", containerFactory = "filterKafkaListenerContainerFactory")
//    public void listenWithFilter(String message) {
//        System.out.println("Received Message in filtered listener: " + message);
//    }
//
//    @KafkaListener(topics = "topicName", containerFactory = "greetingKafkaListenerContainerFactory")
//    public void greetingListener(Greeting greeting) {
//        System.out.println(greeting);
//    }
}
