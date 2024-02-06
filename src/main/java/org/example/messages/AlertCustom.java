package org.example.messages;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AlertCustom implements Serializable {
    
    public enum AlertLevel {
        CRITICAL, MAJOR, MINOR, WARNING
    }
    
    private final int alertId;
    private String stageId;
    private final AlertLevel alertLevel;
    private final String alertMessage;
}
