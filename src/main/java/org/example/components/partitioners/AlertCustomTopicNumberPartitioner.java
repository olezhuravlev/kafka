package org.example.components.partitioners;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class AlertCustomTopicNumberPartitioner implements Partitioner {
    
    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("+++ CONFIGURE PARTITIONER! +++");
    }
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // Number of designated partition set in record value.
        return Integer.valueOf((String) value);
    }
    
    @Override
    public void close() {
        System.out.println("+++ CLOSE PARTITIONER! +++");
    }
}
