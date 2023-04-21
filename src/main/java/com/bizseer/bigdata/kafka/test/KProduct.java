package com.bizseer.bigdata.kafka.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public abstract class KProduct<T> {
    private long interval = 1000L;
    private final KafkaProducer<String, String> producer;
    private final String topicName;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KProduct(long interval, String topicName) {
        this.producer = new KafkaProducer<>(new KafkaGetConfig(topicName).getKafkaConfig());
        this.interval = interval;
        this.topicName = topicName;
    }

    public KProduct(String topicName) {
        this.producer = new KafkaProducer<>(new KafkaGetConfig(topicName).getKafkaConfig());
        this.topicName = topicName;
    }


    public long send() {
        AtomicLong flag = new AtomicLong(0);
        try {
            this.setMsg().forEach(s -> {
                try {
                    producer.send(new ProducerRecord<>(topicName, null, objectMapper.writeValueAsString(s)));
                    System.out.println(objectMapper.writeValueAsString(s));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                flag.addAndGet(1);
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
        return flag.get();
    }

    public abstract List<T> setMsg();


    public static void main(String[] args) {
        int[] arr1 = {1, 2, 3, 4};
        int[] arr2 = {8, 7, 6, 5};
        initList(arr1, arr2);
    }

    private static void initList(int[] arr1, int[] arr2) {
        ArrayList<Integer> list1 = new ArrayList<Integer>();
        ArrayList<Integer> list2 = new ArrayList<Integer>();
        //测试数据 每个数组只有四个元素 不会越界
        for (int i = 0; i < 4; i++) {
            list1.add(arr1[i]);
            list2.add(arr2[i]);
        }
        //输出
        System.out.println(list2.retainAll(list1));
        System.out.println(list2);
    }
    
    
}
