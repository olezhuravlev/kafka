package org.example.serializers;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.example.messages.AlertCustom;

public class AlertCustomKeySerde implements Serializer<AlertCustom>, Deserializer<AlertCustom> {
    
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }
    
    @Override
    public byte[] serialize(String topic, AlertCustom key) {
        
        if (key == null) {
            return null;
        }
        
        return key.getStageId().getBytes(StandardCharsets.UTF_8);
    }
    
    @Override
    public AlertCustom deserialize(String topic, byte[] data) {
        return null;
    }
    
    @Override
    public void close() {
        Serializer.super.close();
    }
}
