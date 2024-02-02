package com.bizseer.bigdata.kafka.test;

import java.util.*;

import com.bizseer.bigdata.kafka.bean.InputMessageData;

public class KafkaDataPlat1 {
    static List<String> dim001 = Arrays.asList("10.8.80.161", "10.8.80.162", "10.8.80.163", "10.8.80.164", "10.8.80.165", "10.8.80.166");
    static List<String> dim002 = Arrays.asList("gateway", "upm");
    static Random random = new Random();

    public static List<InputMessageData> getMetric(Integer count) {
        Map<String, String> header = new HashMap<>();
        header.put("model_name", "model_1124_001");
        header.put("is_auto_model", "true");
        header.put("model_level", "测试层级1");
        header.put("biz_category_id", "1");
        header.put("business_system", "default_system");
        header.put("reserve", "7");
        header.put("source", "zabbix");

        InputMessageData.Measurement measurement = new InputMessageData.Measurement();
        measurement.setName("CPU");
        measurement.setTimestamp(60000 + System.currentTimeMillis());
        Map<String, Double> metrics = new HashMap<>();
        //TODO 变化
        metrics.put("idle", 45d + random.nextDouble());
        metrics.put("usage", 55d + random.nextDouble());
        measurement.setMetrics(metrics);
        Map<String, String> granularity = new HashMap<>();
        granularity.put("idle", "60");
        granularity.put("usage", "60");
        measurement.setGranularity(granularity);
        Map<String, String> metricNameCN = new HashMap<>();
        metricNameCN.put("idle", "CPU的空闲率");
        metricNameCN.put("usage", "CPU的使用率");
        measurement.setMetricNameCn(metricNameCN);
        Map<String, String> unit = new HashMap<>();
        unit.put("idle", "%");
        unit.put("usage", "s");
        measurement.setUnit(unit);
        Map<String, String> importantLevel = new HashMap<>();
        importantLevel.put("idle", "黄金交易指标");
        importantLevel.put("usage", "黄金指标");
        measurement.setImportantLevel(importantLevel);
        Map<String, String> dimensionNameCn = new HashMap<>();
        dimensionNameCn.put("host", "主机");
        dimensionNameCn.put("ip", "IP");
        measurement.setDimensionNameCn(dimensionNameCn);
        Map<String, String> dimensions = new HashMap<>();
        //TODO 变化
        dimensions.put("ip", dim001.get(random.nextInt(4)));
        dimensions.put("host", "host_0"+count);
        measurement.setDimensions(dimensions);

        InputMessageData inputMessageData = new InputMessageData();
        inputMessageData.setHeader(header);
        inputMessageData.setMeasurement(measurement);

        List<InputMessageData> list = new ArrayList<>();
        list.add(inputMessageData);
        return list;
    }
}
