package com.bizseer.bigdata.influxdb;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangyingjie
 */
public class InfluxDbTask implements Runnable {
    private final String url;
    private final List<Metric> metrics;
    private final int interval;
    private final int batchSize;
    private final String database;
    private final Random random = new Random();

    public InfluxDbTask(String url, String database, List<Metric> metrics, int interval, int batchSize) {
        this.url = url;
        this.metrics = metrics;
        this.interval = interval;
        this.batchSize = batchSize;
        this.database = database;
    }

    @Override
    public void run() {
        //初始化influxDB客户端
        InfluxDB influxDb = InfluxDBFactory.connect(url)
                .enableBatch(batchSize, 2000, TimeUnit.MILLISECONDS)
                .setDatabase(database)
                .setRetentionPolicy("autogen");
        Point point;
        long lastPushTime = (System.currentTimeMillis() / 1000 / interval) * interval * 1000 - 24 * 60 * 60 * 1000;
        while (true) {
            long now = System.currentTimeMillis() / 1000;
            if (now - lastPushTime < interval) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            } else if (now - lastPushTime > interval * 2) {
                System.out.println("lag more than " + (now - lastPushTime) + " seconds.");
            }
            lastPushTime += interval;
            Map<String, Object> fields = new HashMap<>();
            for (Metric metric : metrics) {
                if (metric.measurement.startsWith("cpu")) {
                    fields.put("value", random.nextFloat() * 100);
                } else {
                    for (int i = 1; i <= 10; i++) {
                        fields.put("value_" + i, random.nextFloat() * 100);
                    }
                }
                point = Point.measurement(metric.measurement)
                        .time(lastPushTime, TimeUnit.SECONDS)
                        .tag(metric.tags)
                        .fields(fields)
                        .build();
                influxDb.write(point);
                fields.clear();
            }
        }
    }

}
