package org.example.configurations;

import org.springframework.kafka.annotation.KafkaListener;

public class Listener {
    
    private final String topic;
    
    public Listener(String topic) {
        this.topic = topic;
    }
    
    @KafkaListener(topics = "#{__listener.topic}", groupId = "#{__listener.topic}.group")
    public void listen(String data) {
        System.out.println("Listener:listen(String): " + data);
    }
    
    public String getTopic() {
        return this.topic;
    }
}
