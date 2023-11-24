package com.bizseer.bigdata.es.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class Pair<L, R> implements Serializable {

    private static final long serialVersionUID = 957139950474121878L;

    private L left;
    private R right;
}
