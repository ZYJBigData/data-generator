package com.bizseer.bigdata;

import com.bizseer.bigdata.influxdb.Metric;
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
    /**
     * influxdb 的url
     */
    private final String url;
    /**
     * 这批要写入的数据
     */
    private final List<Metric> metrics;
    /**
     * 写入间隔,比如十秒写入一次
     */
    private final int interval;
    /**
     * 批次写入
     */
    private final int batchSize;

    public InfluxDbTask(String url, List<Metric> metrics, int interval, int batchSize) {
        this.url = url;
        this.metrics = metrics;
        this.interval = interval;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        final String database = "bizseer_metric";
        //初始化influxDB客户端
        InfluxDB influxDB = InfluxDBFactory.connect(url)
                .enableBatch(batchSize, 2000, TimeUnit.MILLISECONDS)
                .setDatabase(database)
                .setRetentionPolicy("autogen");

        Point point;
        long lastPushTime = (System.currentTimeMillis() / 1000 / interval) * interval;
        //no inspection Infinite Loop Statement
        while (true) {
            long now = System.currentTimeMillis() / 1000;
            if (now - lastPushTime < interval) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            } else if (now - lastPushTime > interval * 2) {
                // 如果时间比2个interval，那就是证明写入的时间太长。就出现了阻塞
                System.out.println("lag more than " + (now - lastPushTime) + " seconds.");
            }
            lastPushTime += interval;
            Map<String, Object> fields = new HashMap<>();
            Random random = new Random();
            for (Metric metric : metrics) {
                if (metric.measurement.startsWith("cpu")) {
                    fields.put("value", random.nextFloat() * 100);
                } else {
                    for (int i = 1; i <= 1; i++) {
                        fields.put("value_" + i, random.nextFloat() * 100);
                    }
                }
                point = Point.measurement(metric.measurement)
                        .time(lastPushTime, TimeUnit.SECONDS)
                        .tag(metric.tags)
                        .fields(fields)
                        .build();
                influxDB.write(point);
                fields.clear();
            }
        }
    }
}
