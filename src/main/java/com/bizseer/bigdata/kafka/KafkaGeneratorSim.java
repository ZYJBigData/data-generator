package com.bizseer.bigdata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author zhangyingjie
 */
public class KafkaGeneratorSim {
    public static void main(String[] args) {
        List<String> contexts = Arrays.asList("{\"a\":\"1\",\"b\":\"2\"}", "{\"a\":\"1\"\"b\":\"2\"}");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.0.1.149:9092,10.0.1.150:9092,10.0.1.151:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Executors.newFixedThreadPool(500).execute(() -> {
            {
                String metricString;
//                System.out.println(sdf.format(System.currentTimeMillis()) + ": start!");
                for (int i = 0; i < contexts.size(); i++) {
                    metricString = contexts.get(i);
                    kafkaProducer.send(new ProducerRecord<>("text_monitor_in", metricString));

                }
//                System.out.println(sdf.format(System.currentTimeMillis()) + ": done!");
            }

        });
    }

}
