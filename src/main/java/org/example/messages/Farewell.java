package org.example.messages;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class Farewell {
    private String message;
    private Integer remainingMinutes;
}
