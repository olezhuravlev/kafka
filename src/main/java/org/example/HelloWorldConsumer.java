package org.example;

import static org.example.HelloWorldProducer.BOOTSTRAP_SERVERS;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorldConsumer {
    
    final static Logger LOGGER = LoggerFactory.getLogger(HelloWorldConsumer.class);
    private static volatile boolean keepConsuming = true;
    
    public static void main(String[] args) {
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_helloconsumer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        HelloWorldConsumer hwConsumer = new HelloWorldConsumer();
        hwConsumer.consume(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(hwConsumer::shutdown));
        
        //        // get a reference to the current thread
        //        final Thread mainThread = Thread.currentThread();
        //
        //        // adding the shutdown hook
        //        Runtime.getRuntime().addShutdownHook(new Thread() {
        //            public void run() {
        //                LOGGER.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
        //                consumer.wakeup();
        //
        //                // join the main thread to allow the execution of the code in the main thread
        //                try {
        //                    mainThread.join();
        //                } catch (InterruptedException e) {
        //                    e.printStackTrace();
        //                }
        //            }
        //        });
        //
        //        try {
        //            // subscribe consumer to our topic(s)
        //            consumer.subscribe(Arrays.asList(HelloWorldProducer.TOPIC));
        //            // poll for new data
        //            while (true) {
        //                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        //                for (ConsumerRecord<String, String> record : records) {
        //                    LOGGER.info("CONSUMED RECORD KEY = {}, VALUE = {}, OFFSET = {}, PARTITION = {}", record.key(), record.value(), record.offset(), record.partition());
        //                }
        //            }
        //        } catch (WakeupException e) {
        //            LOGGER.info("Wake up exception!");
        //            // we ignore this as this is an expected exception when closing a consumer
        //        } catch (Exception e) {
        //            LOGGER.error("Unexpected exception", e);
        //        } finally {
        //            consumer.close(); // this will also commit the offsets if need be.
        //            LOGGER.info("The consumer is now gracefully closed.");
        //        }
    }
    
    private void consume(Properties properties) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(HelloWorldProducer.TOPIC));
            
            while (keepConsuming) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("CONSUMED RECORD KEY = {}, VALUE = {}, OFFSET = {}, PARTITION = {}", record.key(), record.value(), record.offset(), record.partition());
                }
            }
        }
    }
    
    private void shutdown() {
        keepConsuming = false;
    }
}
