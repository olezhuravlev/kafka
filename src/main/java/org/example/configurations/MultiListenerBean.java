package org.example.configurations;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;

@KafkaListener(id = "multi", topics = "kafkaTopic,testKafkaTopic")
public class MultiListenerBean {
    
    @KafkaHandler
    public void listen(String data) {
        System.out.println("MultiListenerBean:listen(String): " + data);
    }
    
    @KafkaHandler
    public void listen(Integer data) {
        System.out.println("MultiListenerBean:listen(Integer): " + data);
    }
    
    @KafkaHandler(isDefault = true)
    public void listenDefault(Object data) {
        System.out.println("MultiListenerBean:listen(Object): " + data);
    }
}
