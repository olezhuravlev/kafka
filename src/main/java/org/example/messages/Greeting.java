package org.example.messages;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class Greeting {
    private String msg;
    private String name;
}
