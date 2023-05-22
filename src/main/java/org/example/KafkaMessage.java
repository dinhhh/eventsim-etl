package org.example;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class KafkaMessage {

    private MessageSchema schema;
    private String payload;

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class MessageSchema {
        private String type;
        private boolean optional;
    }
}
