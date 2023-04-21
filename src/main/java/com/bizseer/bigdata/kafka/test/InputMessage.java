package com.bizseer.bigdata.kafka.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import org.joda.time.LocalDate;
import java.util.HashMap;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class InputMessage {
    private Header header;
    private Measurement measurement;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Header {
        private Integer bizseer_metric_version;
        private String owner;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Measurement {
        private String name;
        private Long timestamp;
        private HashMap<String, Integer> metrics;
        private HashMap<String, String> dimensions;
    }

    public static void main(String[] args) {
        long timestamp=1671524921000L;
        timestamp = new LocalDate(timestamp).toDateTimeAtStartOfDay().getMillis();
        //influxdb 写入数据
        
    }
}
