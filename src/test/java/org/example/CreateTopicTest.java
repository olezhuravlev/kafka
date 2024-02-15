package org.example;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.example.configurations.EmbeddedKafkaHolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka(partitions = 2, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
@DirtiesContext
public class CreateTopicTest {
    
    // create/delete/list topics;
    // configuration changing;
    // create/delete/list ACLs;
    // create partitions;
    // getting descriptions/lists for consumers groups;
    // getting cluster descriptions;
    
    private static EmbeddedKafkaBroker embeddedKafkaBroker;
    
    @BeforeAll
    public static void beforeAll() {
        embeddedKafkaBroker = EmbeddedKafkaHolder.getEmbeddedKafkaBroker();
    }
    
    @AfterAll
    static void afterAll() {
        EmbeddedKafkaHolder.getEmbeddedKafkaBroker().destroy();
    }
    
    @Test
    void createTopicTest() throws ExecutionException, InterruptedException {
        
        String newTopicName = "kinaction_selfserviceTopic";
        int numPartitions = 2;
        short replicationFactor = 1;
        
        String brokers = embeddedKafkaBroker.getBrokersAsString(); // 127.0.0.1:44191
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        
        AdminClient adminClient = AdminClient.create(properties);
        
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
        Set<String> topicsBefore = listTopicsResult.names().get();
        
        NewTopic requestedTopic = new NewTopic(newTopicName, numPartitions, replicationFactor);
        CreateTopicsResult topicResult = adminClient.createTopics(List.of(requestedTopic));
        adminClient.close();
        
        // GET RESULTS
        AdminClient adminClient2 = AdminClient.create(properties);
        listTopicsResult = adminClient2.listTopics(listTopicsOptions);
        Set<String> topicsAfter = listTopicsResult.names().get();
        
        assertThat("One topic must be added", topicsAfter.size() - topicsBefore.size(), is(1));
        assertThat("", topicsAfter.contains(newTopicName), is(true));
    }
    
    @Test
    void createTopicAsyncTest() throws ExecutionException, InterruptedException {
        
        String newTopicName = "kinaction_selfserviceTopic";
        String brokers = embeddedKafkaBroker.getBrokersAsString();
        
        Thread createTopicThread = new Thread(() -> {
            
            int numPartitions = 2;
            short replicationFactor = 1;
            
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            
            AdminClient adminClient = KafkaAdminClient.create(properties);
            
            NewTopic requestedTopic = new NewTopic(newTopicName, numPartitions, replicationFactor);
            CreateTopicsResult topicResult = adminClient.createTopics(List.of(requestedTopic));
            adminClient.close();
        });
        createTopicThread.start();
        
        createTopicThread.join();
        
        Set<String> topics = embeddedKafkaBroker.getTopics();
        System.out.println(topics);
        
        // GET RESULTS
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        
        AdminClient adminClient = KafkaAdminClient.create(properties);
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
        Set<String> topicsAfter = listTopicsResult.names().get();
        assertThat("", topicsAfter.contains(newTopicName), is(true));
    }
}
