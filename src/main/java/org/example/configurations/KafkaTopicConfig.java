package org.example.configurations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {
    
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;
    
    @Value(value = "${topic}")
    private String topic;
    
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }
    
    @Bean
    public NewTopic topic1() {
        return TopicBuilder
            .name("newTopicFromKafkaTopicConfig")
            .partitions(10)
            .replicas(3)
            .compact()
            .build();
    }
    
    @Bean
    public NewTopic topic2() {
        return TopicBuilder
            .name("thing2")
            .partitions(10)
            .replicas(3)
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
            .build();
    }
    
    @Bean
    public NewTopic topic3() {
        return TopicBuilder
            .name("thing3")
            .assignReplicas(0, List.of(0, 1))
            .assignReplicas(1, List.of(1, 2))
            .assignReplicas(2, List.of(2, 0))
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
            .build();
    }
    
    @Bean
    public NewTopic topic4() {
        return TopicBuilder.name("defaultBoth")
            .build();
    }
    
    @Bean
    public NewTopic topic5() {
        return TopicBuilder.name("defaultPart")
            .replicas(1)
            .build();
    }
    
    @Bean
    public NewTopic topic6() {
        return TopicBuilder.name("defaultRepl")
            .partitions(3)
            .build();
    }
    
    @Bean
    public KafkaAdmin.NewTopics topics456() {
        return new KafkaAdmin.NewTopics(
            TopicBuilder.name("defaultBoth")
                .build(),
            TopicBuilder.name("defaultPart")
                .replicas(1)
                .build(),
            TopicBuilder.name("defaultRepl")
                .partitions(3)
                .build());
    }
}
