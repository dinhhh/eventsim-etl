package org.example;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class Event {
    private String ts;
    private String userId;
    private long sessionId;
    private String page;
    private String auth;
    private String method;
    private long status;
    private String level;
    private long itemInSession;
    private String location;
    private String userAgent;
    private String lastName;
    private String firstName;
    private String registration;
    private String gender;

    private String song;
    private String artist;
    private double length;
}
