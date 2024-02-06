package org.example.serializers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
        
        // We serialize only alertId (int - always 4 bytes) and message (the rest of the result byte array).
        
        int alertId = key.getAlertId();
        byte[] alertIdByte = ByteBuffer.allocate(4).putInt(alertId).array();
        
        String alertMessage = key.getAlertMessage();
        byte[] alertMessageByte = alertMessage.getBytes(StandardCharsets.UTF_8);
        
        byte[] data = Arrays.copyOf(alertIdByte, alertIdByte.length + alertMessageByte.length);
        System.arraycopy(alertMessageByte, 0, data, alertIdByte.length, alertMessageByte.length);
        
        return data;
    }
    
    @Override
    public AlertCustom deserialize(String topic, byte[] data) {
        
        byte[] alertIdByte = Arrays.copyOf(data, 4);
        int alertId = ByteBuffer.wrap(alertIdByte).getInt();
        
        byte[] alertMessageByte = Arrays.copyOfRange(data, 4, data.length);
        String alertMessage = new String(alertMessageByte, StandardCharsets.UTF_8);
        
        AlertCustom alertCustom = new AlertCustom(alertId, "Stage 1", AlertCustom.AlertLevel.CRITICAL, alertMessage);
        return alertCustom;
    }
    
    @Override
    public void close() {
        Serializer.super.close();
    }
}
