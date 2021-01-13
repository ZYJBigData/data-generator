package com.bizseer.bigdata;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * @author only
 */
public class CpuPoint {
    @JsonProperty(value = "db")
    private String db;
    @JsonProperty(value = "time")
    private long time;
    @JsonProperty(value = "metrics")
    private Map<String, Float> metrics;
    @JsonProperty(value = "tags")
    private Map<String, String> tags;
    @JsonProperty(value = "pre-agg")
    private boolean preAgg = true;


    public CpuPoint setDb(String db) {
        this.db = db;
        return this;
    }

    public CpuPoint setTime(long time) {
        this.time = time;
        return this;
    }

    public CpuPoint setMetrics(Map<String, Float> metrics) {
        this.metrics = metrics;
        return this;
    }

    public CpuPoint setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public CpuPoint setPreAgg(boolean preAgg) {
        this.preAgg = preAgg;
        return this;
    }
}
