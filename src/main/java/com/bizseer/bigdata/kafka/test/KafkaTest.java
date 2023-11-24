package com.bizseer.bigdata.kafka.test;

import com.bizseer.bigdata.kafka.bean.InputMessageData;

import java.util.*;

class KafkaTest {
    public static void main(String[] args) throws InterruptedException {
//        stream();
//       batch();
        
        stream_001();
    }
    
    public static void stream_001() throws InterruptedException {
        while (true){
            List<InputMessageData> metric = KafkaDataPlat1.getMetric();
            new KProduct<InputMessageData>(0L, "data_platform_metric") {
                @Override
                public List<InputMessageData> setMsg() {
                    return metric;
                }
            }.send();
            System.out.println("done");
//            //粒度
            Thread.sleep( 1000L);
        }
    }

    public static void stream() throws InterruptedException {
        while (true) {
            List<InputMessage> data = ProData.getMetrics3();
            new KProduct<InputMessage>(0L, "zyj-in") {
                @Override
                public List<InputMessage> setMsg() {
                    return data;
                }
            }.send();
            System.out.println("done");
//            //粒度
            Thread.sleep( 1000L);
        }
    }

    public static void batch() throws InterruptedException {
        List<InputMessage> list = new ArrayList<>();
        for (int i = 0; i < 1515; i++) {
//            List<InputMessage> data = ProData.getMetrics();
            InputMessage autoMetric = ProData.getAutoMetric();
            list.addAll(Collections.singletonList(autoMetric));
        }
        new KProduct<InputMessage>(0L, "data_platform_metric") {
            @Override
            public List<InputMessage> setMsg() {
                return list;
            }
        }.send();
    }

}
