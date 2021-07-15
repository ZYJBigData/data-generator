package com.bizseer.bigdata.influxdb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author zhangyingjie
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Metric {
    public String measurement;
    public Map<String, String> tags;
}
