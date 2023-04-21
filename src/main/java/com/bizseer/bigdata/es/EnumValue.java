package com.bizseer.bigdata.es;

import java.io.Serializable;

public interface EnumValue<T> extends Serializable {
    T getCode();
}
