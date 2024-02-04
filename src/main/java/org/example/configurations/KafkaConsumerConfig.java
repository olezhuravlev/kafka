package org.example.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

/**
 * https://docs.spring.io/spring-kafka/reference/kafka.html
 */

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;
    
    //////////////////////////////////////////////////////////////
    // Batch listener.
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }
    
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroupId");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
        return props;
    }
    
//    @KafkaListener(topics = "kafkaTopic,testKafkaTopic")
//    public void listen1(String data) {
//        System.out.println("LISTENER_1: " + data);
//    }

//    @KafkaListener(id = "myListener", topics = "kafkaTopic,testKafkaTopic", autoStartup = "${listen.auto.start:true}", concurrency = "${listen.concurrency:3}")
//    public void listen2(String data) {
//        System.out.println("LISTENER_2: " + data);
//    }
    
    @KafkaListener(id = "thing3", topicPartitions = {
        @TopicPartition(topic = "topic1", partitions = { "0", "1" }),
        @TopicPartition(topic = "topic2", partitions = "0", partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "100"))
    })
    public void listen3(ConsumerRecord<?, ?> record) {
        System.out.println("LISTENER_3: " + record);
    }
    
    @KafkaListener(id = "thing4", topicPartitions = {
        @TopicPartition(topic = "topic1", partitions = { "0", "1" }, partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0"))
    })
    public void listen4(ConsumerRecord<?, ?> record) {
        System.out.println("LISTENER_4: " + record);
    }
    
    @KafkaListener(id = "pp", autoStartup = "false", topicPartitions = @TopicPartition(topic = "topic1", partitions = "0-5, 7, 10-15"))
    public void listen5(String in) {
        System.out.println("LISTENER_5: " + in);
    }
    
    @KafkaListener(id = "thing6", topicPartitions = {
        @TopicPartition(topic = "topic1", partitionOffsets = @PartitionOffset(partition = "0-5", initialOffset = "0"))
    })
    public void listen6(ConsumerRecord<?, ?> record) {
        System.out.println("LISTENER_6: " + record);
    }
    
    //    @KafkaListener(id = "cat", topics = "myTopic", containerFactory = "kafkaManualAckListenerContainerFactory")
    //    public void listen7(String data, Acknowledgment ack) {
    //        System.out.println("LISTENER_6: " + data + " ACK: " + ack);
    //        ack.acknowledge();
    //    }
    
    @KafkaListener(id = "qux", topicPattern = "myTopic1")
    public void listen8(@Payload String foo, @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) Integer key, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        
        System.out.println("LISTENER_8: " + foo + " RECEIVED_KEY: " + key + " RECEIVED_PARTITION: " + partition + " RECEIVED_TOPIC: " + topic + " RECEIVED_TIMESTAMP:" + ts);
    }
    
//    @KafkaListener(topics = "kafkaTopic,testKafkaTopic")
//    public void listen9(String str, ConsumerRecordMetadata meta) {
//        System.out.println("LISTENER_9: " + str + " ConsumerRecordMetadata: " + meta);
//    }

//    @KafkaListener(topics = "kafkaTopic,testKafkaTopic", groupId = "group", properties = { "max.poll.interval.ms:60000", ConsumerConfig.MAX_POLL_RECORDS_CONFIG + "=100" })
//    public void listen10(ConsumerRecords<?, ?> records) {
//        System.out.println("LISTENER_10: " + records);
//    }
    
    //////////////////////////////////////////////////////////////
    // Obtain Consumer group.id.
    @KafkaListener(id = "id", topicPattern = "someTopic")
    public void listen11(@Payload String payload, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        System.out.println("LISTENER_11: " + payload + " GROUP.ID: " + groupId);
    }
    
    //////////////////////////////////////////////////////////////
    // Batch listener.
    @Bean
    public KafkaListenerContainerFactory<?> batchFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        return factory;
    }
    
    // Receiving list of payloads.
    //    @KafkaListener(id = "list", topics = "kafkaTopic,testKafkaTopic", containerFactory = "batchFactory")
    //    public void listenBatch1(List<String> list) {
    //        System.out.println("LIST OF PAYLOADS (listenBatch1): " + list);
    //    }
    
    //    @KafkaListener(id = "list", topics = "kafkaTopic,testKafkaTopic", containerFactory = "batchFactory")
    //    public void listenBatch2(List<String> list,
    //        @Header(KafkaHeaders.RECEIVED_KEY) List<Integer> keys,
    //        @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
    //        @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
    //        @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
    //        System.out.println("LIST OF PAYLOADS (listenBatch1): " + list + " RECEIVED_KEY: " + keys + " RECEIVED_PARTITION: " + partitions + " RECEIVED_TOPIC: " + topics
    //            + " OFFSET: " + offsets);
    //    }
    
    //    @KafkaListener(id = "listMsg", topics = "kafkaTopic,testKafkaTopic", containerFactory = "batchFactory")
    //    public void listenBatch3(List<Message<?>> list) {
    //        System.out.println("LIST OF PAYLOADS (listenBatch3): " + list);
    //    }
    
    //    @KafkaListener(id = "listMsgAck", topics = "kafkaTopic,testKafkaTopic", containerFactory = "batchFactory")
    //    public void listenBatch4(List<Message<?>> list, Acknowledgment ack) {
    //        System.out.println("LIST OF PAYLOADS (listenBatch4): " + list + " ACK: " + ack);
    //    }
    
    // @KafkaListener(id = "listMsgAckConsumer", topics = "kafkaTopic,testKafkaTopic", containerFactory = "batchFactory")
    // public void listenBatch5(List<Message<?>> list, Acknowledgment ack, Consumer<?, ?> consumer) {
    //     System.out.println("LIST OF PAYLOADS (listenBatch5): " + list + " ACK: " + ack + " Consumer: " + consumer);
    // }
    
    //    @KafkaListener(id = "listCRs", topics = "kafkaTopic,testKafkaTopic", containerFactory = "batchFactory")
    //    public void listenBatch6(List<ConsumerRecord<Integer, String>> list) {
    //        System.out.println("LIST OF PAYLOADS (listenBatch6): " + list);
    //    }
    
    //    @KafkaListener(id = "listCRsAck", topics = "kafkaTopic,testKafkaTopic", containerFactory = "batchFactory")
    //    public void listenBatch7(List<ConsumerRecord<Integer, String>> list, Acknowledgment ack) {
    //        System.out.println("LIST OF PAYLOADS (listenBatch7): " + list + " ACK: " + ack);
    //    }
    
    //    @KafkaListener(id = "pollResults", topics = "kafkaTopic,testKafkaTopic", containerFactory = "batchFactory")
    //    public void listenBatch8(ConsumerRecords<?, ?> records) {
    //        System.out.println("LIST OF PAYLOADS (listenBatch8): " + records);
    //    }
    
    //////////////////////////////////////////////////////////////
    // Custom class listener.
    @Bean
    public Listener listener1() {
        return new Listener("kafkaTopic");
    }
    
    @Bean
    public Listener listener2() {
        return new Listener("testKafkaTopic");
    }
    
    //////////////////////////////////////////////////////////////
    // Custom listener interface.
//    @MyThreeConsumersListener(id = "my.group", topics = "kafkaTopic,testKafkaTopic")
//    public void customInterfaceListener(String data) {
//        System.out.println("customInterfaceListener: " + data);
//    }
    
    //////////////////////////////////////////////////////////////
    @KafkaHandler(isDefault = true)
    public void kafkaHandlerListener1(Object data, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println("kafkaHandlerListener1: " + data + " RECEIVED_TOPIC: " + topic);
    }
    
    @KafkaHandler(isDefault = true)
    void kafkaHandlerListener2(Object data, @Header(KafkaHeaders.RECORD_METADATA) ConsumerRecordMetadata meta) {
        String topic = meta.topic();
        System.out.println("kafkaHandlerListener2: " + data + " RECORD_METADATA: " + meta);
    }
    //////////////////////////////////////////////////////////////
    
    //    public ConsumerFactory<String, String> consumerFactory(String groupId) {
    //        Map<String, Object> props = new HashMap<>();
    //        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    //        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    //        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    //        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    //        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
    //        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
    //        return new DefaultKafkaConsumerFactory<>(props);
    //    }
    //
    //    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(String groupId) {
    //        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    //        factory.setConsumerFactory(consumerFactory(groupId));
    //        return factory;
    //    }
    
    ///////////////////////////////////////////////////////
    // Greeting
    
    //    public ConsumerFactory<String, Greeting> greetingConsumerFactory() {
    //        Map<String, Object> props = new HashMap<>();
    //        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    //        props.put(ConsumerConfig.GROUP_ID_CONFIG, "greeting");
    //        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    //        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    //        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(Greeting.class));
    //    }
    //
    //    @Bean
    //    public ConcurrentKafkaListenerContainerFactory<String, Greeting> greetingKafkaListenerContainerFactory() {
    //        ConcurrentKafkaListenerContainerFactory<String, Greeting> factory = new ConcurrentKafkaListenerContainerFactory<>();
    //        factory.setConsumerFactory(greetingConsumerFactory());
    //        return factory;
    //    }
    
    ///////////////////////////////////////////////////////
    // Multitype
    
    //    @Bean("consumerFactory")
    //    public ConsumerFactory<String, Object> multiTypeConsumerFactory() {
    //        HashMap<String, Object> props = new HashMap<>();
    //        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    //        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    //        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    //        return new DefaultKafkaConsumerFactory<>(props);
    //    }
    //
    //    @Bean
    //    public ConcurrentKafkaListenerContainerFactory<String, Object> multiTypeKafkaListenerContainerFactory() {
    //
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
    
    ///////////////////////////////////////////////////////
    // Filter
    
    //    @Bean
    //    public ConcurrentKafkaListenerContainerFactory<String, String> filterKafkaListenerContainerFactory() {
    //
    //        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    //        //factory.setConsumerFactory(consumerFactory());
    //        factory.setRecordFilterStrategy(record -> record.value().contains("World"));
    //        return factory;
    //    }
    //
    //    @KafkaListener(topics = "kafkaTopic, testKafkaTopic", containerFactory = "filterKafkaListenerContainerFactory")
    //    public void listenWithFilter(String message) {
    //        System.out.println("Received Message in filtered listener: " + message);
    //    }
    
    ///////////////////////////////////////////////////////
    // Partitions
    //    @Bean
    //    public ConcurrentKafkaListenerContainerFactory<String, String> fooKafkaListenerContainerFactory() {
    //        return kafkaListenerContainerFactory("foo");
    //    }
    //
    //    @Bean
    //    public ConcurrentKafkaListenerContainerFactory<String, String> barKafkaListenerContainerFactory() {
    //        return kafkaListenerContainerFactory("bar");
    //    }
    //
    //    @Bean
    //    public ConcurrentKafkaListenerContainerFactory<String, String> headersKafkaListenerContainerFactory() {
    //        return kafkaListenerContainerFactory("headers");
    //    }
    //
    //    @Bean
    //    public ConcurrentKafkaListenerContainerFactory<String, String> partitionsKafkaListenerContainerFactory() {
    //        return kafkaListenerContainerFactory("partitions");
    //    }
    //
    //    @Bean
    //    public ConcurrentKafkaListenerContainerFactory<String, String> longMessageKafkaListenerContainerFactory() {
    //        return kafkaListenerContainerFactory("longMessage");
    //    }
    //        @Bean
    //        public ConcurrentKafkaListenerContainerFactory<String, String> partitionsKafkaListenerContainerFactory() {
    //            return kafkaListenerContainerFactory("partitions");
    //        }
    
    //    @KafkaListener(topicPartitions = @TopicPartition(topic = "kafkaTopic, testKafkaTopic", partitionOffsets = {
    //        @PartitionOffset(partition = "0", initialOffset = "0"), @PartitionOffset(partition = "3", initialOffset = "0")
    //    }), containerFactory = "partitionsKafkaListenerContainerFactory")
    //    public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
    //        System.out.println("Received Message: " + message + "from partition: " + partition);
    //    }
    
    ///////////////////////////////////////////////////////
    // Other listeners
    //
    //    @KafkaListener(topics = "kafkaTopic, testKafkaTopic", groupId = "myGroupId")
    //    public void listenGroupFoo(String message) {
    //        System.out.println("Received Message in group 'myGroupId': " + message);
    //    }
    //
    //    @KafkaListener(topics = "kafkaTopic, testKafkaTopic")
    //    public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
    //        System.out.println("Received Message: " + message + "from partition: " + partition);
    //    }
    
    //    @KafkaListener(topics = "kafkaTopic, testKafkaTopic", containerFactory = "greetingKafkaListenerContainerFactory")
    //    public void greetingListener(Greeting greeting) {
    //        System.out.println(greeting);
    //    }
}
