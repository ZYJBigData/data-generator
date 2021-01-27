package com.bizseer.bigdata;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author zhangyingjie
 */
public class KafkaGenerator {
    final static int APP_NUMBER = 20;
    final static int SERVICE_NUMBER = 10;
    final static int CLASS_NUMBER = 5;
    final static int INSTANCE_NUMBER = 10;
    final static Random RANDOM = new Random();

    @Parameter(names = "--bootstrap-servers")
    String bootStrapServers = "10.0.90.74:9000,10.0.90.75:9000,10.0.90.76:9000";

    @Parameter(names = "--topic")
    String topic = "release-2.x-influxdbsink";

    @Parameter(names = "--interval", description = "采样间隔")
    int interval = 10;

    @Parameter(names = "--kpi", description = "kpi数量")
    int kpiNum = 50;

    @Parameter(names = "--cpu", description = "cpu数量")
    int cpuNum = 250;

    @Parameter(names = "--cpu-db")
    String cpuDb = "metric";

    public static void main(String[] args) {
        KafkaGenerator cli = new KafkaGenerator();
        JCommander.newBuilder().addObject(cli).build().parse(args);
        ObjectMapper mapper = new ObjectMapper();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", cli.bootStrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        CpuPoint cpuPoint = new CpuPoint();
        Map<String, Float> metrics = new HashMap<>(10);
        List<Map<String, String>> tags = initTag();
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println(sdf.format(System.currentTimeMillis()) + ": start!");
                long time = System.currentTimeMillis() / 1000 / cli.interval * cli.interval * 1000 - 4 * 24 * 60 * 60 * 1000;
                try {
                    for (Map<String, String> tag : tags) {
                        for (int i = 1; i <= cli.cpuNum; i++) {
                            metrics.put("cpu_" + i, RANDOM.nextFloat() * 100);
                            cpuPoint.setDb(cli.cpuDb).setTime(time).setTags(tag).setMetrics(metrics);
                            String metricString = mapper.writeValueAsString(cpuPoint);
                            kafkaProducer.send(new ProducerRecord<>(cli.topic, metricString));
                            metrics.clear();
                        }
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    metrics.clear();
                }
                System.out.println(sdf.format(System.currentTimeMillis()) + ": done!");
            }
        }, 0, cli.interval * 1000L);
    }

    public static List<Map<String, String>> initTag() {
        List<Map<String, String>> result = new ArrayList<>();
        for (int app = 1; app <= APP_NUMBER; app++) {
            for (int service = 1; service <= SERVICE_NUMBER; service++) {
                for (int clazz = 1; clazz <= CLASS_NUMBER; clazz++) {
                    for (int instance = 1; instance <= INSTANCE_NUMBER; instance++) {
                        Map<String, String> tags = new HashMap<>(4);
                        tags.put("app", "app_" + app);
                        tags.put("service", "service_" + service);
                        tags.put("class", "class_" + clazz);
                        tags.put("instance", "instance_" + instance);
                        result.add(tags);
                    }
                }
            }
        }
        return result;
    }
}
