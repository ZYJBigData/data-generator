package com.bizseer.bigdata.clickhouse;

public interface ExecutionInfoSubscriber {

    public void beforeReturn(ExecutionInfo executionInfo);

}