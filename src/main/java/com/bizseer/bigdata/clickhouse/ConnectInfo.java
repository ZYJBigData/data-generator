package com.bizseer.bigdata.clickhouse;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public abstract class ConnectInfo {

    private String host;

    private Integer port;

    public abstract SourceType getSourceType();

    public abstract String toDescription();

    public ExecutionInfoSubscriber executionInfoSubscriber;

}