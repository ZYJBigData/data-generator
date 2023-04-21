package com.bizseer.bigdata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.*;

public class KafkaData {
    public static final String TOPIC = "zyj-in";
    public static final String TOPIC_1 = "lag-in";
    public static final String TOPIC_2 = "kafka-in";
    public static final String TOPIC_3 = "last-in";

    public static void main(String[] args) {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 10, 5, TimeUnit.MINUTES, new LinkedBlockingQueue<>(1000));
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println(Thread.currentThread().getName());
                    while (true) {
                        sendData();
                    }
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        for (int i = 0; i < threadPoolExecutor.getCorePoolSize() - 1; i++) {
            threadPoolExecutor.execute(runnable);
        }
    }

    public static void sendData() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.90.74:9000,10.0.90.75:0,10.0.90.76:9000");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        long currentTimeMillis = System.currentTimeMillis();
        int number = 0;
        for (int i = 0; i <= 50; i++) {
            number = number + i;
            currentTimeMillis = currentTimeMillis + 60000;
            String a = "{\"time\":\"" + sdf.format(new Date(System.currentTimeMillis())) + "\",\"id\":1,\"name\":\"zhangyingjie\",\"age\":" + i + "}";
            System.out.println("a==={}" + a);
//            String a = "{\"a\":" + number + ",\"b\":\"zhangsan\"" + ",\"timestamp\":" + currentTimeMillis + "}";
//            String b = "{\"a\":" + number + ",\"b\":\"lisi\"" + ",\"timestamp\":" + currentTimeMillis + "}";
//            String c = "{\"a\":" + number + ",\"b\":\"wangermazi\"" + ",\"timestamp\":" + currentTimeMillis + "}";
            kafkaProducer.send(new ProducerRecord<>(TOPIC, a));
//            kafkaProducer.send(new ProducerRecord<>(TOPIC_1, a));
//            kafkaProducer.send(new ProducerRecord<>(TOPIC_2, a));
//            kafkaProducer.send(new ProducerRecord<>(TOPIC_3, a));
            Thread.sleep(12000);
//            Thread.sleep(60000);
//            kafkaProducer.send(new ProducerRecord<>(TOPIC, b)).get();
//            Thread.sleep(1000);
//            kafkaProducer.send(new ProducerRecord<>(TOPIC, c)).get();
//            System.out.println(a);
        }
    }
}
