package com.bizseer.bigdata.influxdb;

import com.beust.jcommander.Parameter;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author zhangyingjie
 */
public class InfluxDbGenerator {
    final static int APPLICATION_NUMBER = 20;
    final static int SERVICE_NUMBER = 10;
    final static int PROCESS_NUMBER = 50;
    final static int HOST_NUMBER = 10;

    @Parameter(names = "--interval", description = "采样间隔")
    int interval = 10;
    @Parameter(names = "--cpu", description = "cpu数量,也就是measurement的个数")
    int cpuNum = 20;
    @Parameter(names = "--cpu-db", description = "influxdb数据库名字")
    String cpuDb = "metric";
    @Parameter(names = "--server", description = "influxdb服务地址")
    String server = "http://10.0.90.74:8086";
    @Parameter(names = "--executor", description = "任务执行的线程数")
    Integer executor = 3;
    @Parameter(names = "--count", description = "往influxdb没批次写入数据量")
    Integer count = 10000;

    public static void main(String[] args) {
        InfluxDbGenerator cli = new InfluxDbGenerator();

        ThreadPoolExecutor executor = new ThreadPoolExecutor(cli.executor, 1000, 1000,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100000));
        //数据初始化
        print("Data init...");
        List<Metric> metrics = initData(cli);

        //数据分片
        List<List<Metric>> segments = averageAssign(metrics, cli.executor);
        print("metrics.size:" + metrics.size());

        print("Start sending...");
        for (List<Metric> segment : segments) {
            executor.execute(new InfluxDbTask(cli.server, cli.cpuDb, segment, cli.interval, cli.count));
        }

        try {
            executor.awaitTermination(cli.interval * 2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdown();
        }
    }

    /**
     * 将一组数据平均分成n组
     *
     * @param source 要分组的数据源
     * @param n      平均分成n组
     * @param <T>    template
     * @return 切分好的数组
     */
    public static <T> List<List<T>> averageAssign(List<T> source, int n) {
        List<List<T>> result = new ArrayList<>();
        int remainder = source.size() % n;  //(先计算出余数)
        int number = source.size() / n;  //然后是商
        int offset = 0;//偏移量
        for (int i = 0; i < n; i++) {
            List<T> value;
            if (remainder > 0) {
                value = source.subList(i * number + offset, (i + 1) * number + offset + 1);
                remainder--;
                offset++;
            } else {
                value = source.subList(i * number + offset, (i + 1) * number + offset);
            }
            result.add(value);
        }
        return result;
    }

    private static List<Metric> initData(InfluxDbGenerator cli) {
        List<Metric> result = new ArrayList<>();
        for (int app = 1; app <= APPLICATION_NUMBER; app++) {
            for (int service = 1; service <= SERVICE_NUMBER; service++) {
                for (int clazz = 1; clazz <= HOST_NUMBER; clazz++) {
                    for (int instance = 1; instance <= PROCESS_NUMBER; instance++) {
                        Map<String, String> tags = new HashMap<>();
                        tags.put("application", "application_" + app);
                        tags.put("host", "host_" + service);
                        tags.put("process", "process_" + clazz);
                        tags.put("service", "service_" + instance);
                        for (int i = 0; i < cli.cpuNum; i++) {
                            Metric metric = new Metric();
                            metric.tags = tags;
                            metric.measurement = "cpu_" + i;
                            result.add(metric);
                        }
                    }
                }
            }
        }
        return result;
    }

    private static void print(String message) {
        System.out.println(message);
    }
}
