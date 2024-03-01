package org.example.kafkastreams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafkainaction.MyAccount;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AccountProducerTest {
    
    public static final String ACCOUNT_TOPIC_NAME = "account";
    
    public static void main(String[] args) {
        
        var p = new Properties();
        p.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        p.put(CLIENT_ID_CONFIG, "account-producer");
        p.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        
        try (var client = AdminClient.create(p)) {
            var txRequest = new NewTopic(ACCOUNT_TOPIC_NAME, 6, (short) 1);
            client.createTopics(singletonList(txRequest));
        }
        
        try (var producer = new KafkaProducer<String, MyAccount>(p)) {
            IntStream.range(1, 10).forEach(index -> {
                final var account = new MyAccount(index, "firstName_" + UUID.randomUUID(), "lastName_" + UUID.randomUUID(), "streetName_" + UUID.randomUUID(), "buildingNumber_" + UUID.randomUUID(), "city_" + UUID.randomUUID(), "country_" + UUID.randomUUID(),
                    LocalDateTime.now().toEpochSecond(ZoneOffset.UTC), LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
                producer.send(new ProducerRecord<>(ACCOUNT_TOPIC_NAME, valueOf(account.getNumber()), account));
            });
        }
    }
}
