package com.bizseer.bigdata.clickhouse;

import lombok.Data;

import java.time.Duration;
import java.util.List;

@Data
public class ExecutionInfo {

    private Duration duration;

    private String queryDql;

    private List<Object> queryArgs;

}