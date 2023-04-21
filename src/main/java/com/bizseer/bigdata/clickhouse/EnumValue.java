package com.bizseer.bigdata.clickhouse;

import java.io.Serializable;

public interface EnumValue<T> extends Serializable {
    T getCode();
}