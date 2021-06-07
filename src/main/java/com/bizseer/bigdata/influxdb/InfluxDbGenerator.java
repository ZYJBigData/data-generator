package com.bizseer.bigdata.influxdb;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.bizseer.bigdata.InfluxDbTask;

import java.util.*;
import java.util.concurrent.*;

public class InfluxDbGenerator {

    final static int INSTANCE_NUMBER = 20;
    final static int MODEL_NUMBER = 5;

    @Parameter(names = "--url", description = "influxdb 地址")
    String url = "http://10.0.90.74:8086";

    @Parameter(names = "--thread", description = "thread 数量")
    int threadNumber = 5;

    @Parameter(names = "--batch", description = "batch size")
    int batchSize = 2000;

    @Parameter(names = "--interval", description = "时间间隔,单位秒")
    int interval = 60;

    public static void main(String[] args) {
        InfluxDbGenerator cli = new InfluxDbGenerator();
        JCommander.newBuilder().addObject(cli).build().parse(args);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(cli.threadNumber, cli.threadNumber, 1000,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100000));
        //数据初始化
        print("Data init，生成需要的所有数据。");
        List<Metric> metrics = initData();
        print("metrics.size:" + metrics.size());
        //根据线程数将数据分片，即就是将数据平均的分给五个线程
        List<List<Metric>> segments = averageAssign(metrics, cli.threadNumber);
        print("Start sending...");
        //获取没批的数据放入线程池中执行
        for (List<Metric> segment : segments) {
            executor.execute(new InfluxDbTask(cli.url, segment, cli.interval, cli.batchSize));
        }
        try {
            executor.awaitTermination(cli.interval * 2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdown();
        }
    }

    private static List<Metric> initData() {
        List<Metric> result = new ArrayList<>();
        for (int instance = 1; instance <= INSTANCE_NUMBER; instance++) {
            for (int model = 1; model <= MODEL_NUMBER; model++) {
                Map<String, String> tags = new HashMap<>();
                tags.put("instance", "instance_" + instance);
                tags.put("model", "model_" + model);
                for (int i = 0; i < 10; i++) {
                    Metric metric = new Metric();
                    metric.tags = tags;
                    metric.measurement = "profile_data.test" + i;
                    result.add(metric);
                }
            }
        }
        return result;
    }

    /**
     * 将数据源根据线程数分组
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

    private static void print(String message) {
        System.out.println(message);
    }
}
