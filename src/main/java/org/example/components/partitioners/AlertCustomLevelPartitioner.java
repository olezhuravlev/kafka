package org.example.components.partitioners;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class AlertCustomLevelPartitioner implements Partitioner {
    
    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("+++ CONFIGURE PARTITIONER! +++");
    }
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }
    
    @Override
    public void close() {
        System.out.println("+++ CLOSE PARTITIONER! +++");
    }
}
