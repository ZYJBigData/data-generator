package com.bizseer.bigdata.kafka.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class InputMessageData implements Serializable {
    private static final long serialVersionUID = 1249814927528867871L;

    private Map<String, String> header;
    private Measurement measurement;
    

    @Data
    public static class Measurement {
        private static final long serialVersionUID = 9173466837633522488L;

        private String name;
        private Long timestamp;
        private Map<String, Double> metrics;
        private Map<String, String> dimensions;
        //新增指标模型需要的相关字段
        @JsonProperty(value = "metric_name_cn")
        private Map<String, String> metricNameCn = new HashMap<>();
        private Map<String, String> unit = new HashMap<>();
        @JsonProperty(value = "important_level")
        private Map<String, String> importantLevel = new HashMap<>();
        private Map<String, String> granularity = new HashMap<>();
        @JsonProperty(value = "dimension_name_cn")
        private Map<String, String> dimensionNameCn = new HashMap<>();
    }
}
