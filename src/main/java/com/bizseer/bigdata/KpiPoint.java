package com.bizseer.bigdata;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * @author zhangyingjie
 */
public class KpiPoint {
    @JsonProperty(value = "db")
    private String db;
    @JsonProperty(value = "time")
    private long time;
    @JsonProperty(value = "metrics")
    Map<String, Map<String, Float>> metrics;
    @JsonProperty(value = "tags")
    private Map<String, String> tags;
    @JsonProperty(value = "pre-agg")
    private boolean preAgg = false;


    public KpiPoint setDb(String db) {
        this.db = db;
        return this;
    }

    public KpiPoint setTime(long time) {
        this.time = time;
        return this;
    }

    public KpiPoint setMetrics(Map<String, Map<String, Float>> metrics) {
        this.metrics = metrics;
        return this;
    }

    public KpiPoint setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public KpiPoint setPreAgg(boolean preAgg) {
        this.preAgg = preAgg;
        return this;
    }
}
