package com.ywq.preparser;

import lombok.Data;

@Data
public class PreParsedLog {
    private String serverTime;
    private String serverIp;
    private String method;
    private String uriStem;
    private String queryString;
    private int serverPort;
    private String clientIp;
    private String userAgent;
    private int profileId;
    private String command;
    //用于我们的hive表的分区
    private int year;
    private int month;
    private int day;
}
