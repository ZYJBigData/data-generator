package com.bizseer.bigdata.kafka.test;

import com.bizseer.bigdata.kafka.bean.InputMessageData;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.*;

import static com.bizseer.bigdata.common.JsonUtils.objectMapper;

class KafkaTest {
//    public static String bootstrapServers = "10.0.60.141:9092,0.0.60.142:9092,10.0.60.143:9092";

    //    public String bootstrapServers = "10.0.60.144:9092,10.0.60.145:9092,10.0.60.146:9092";
    public static String bootstrapServers = "10.0.100.16:9092";
    public static void main(String[] args) throws InterruptedException {
//        stream();
//       batch();

        stream_001();
    }

    public static void stream_001() throws InterruptedException {
        int count =0;
        while (true) {
            List<InputMessageData> metric = KafkaDataPlat1.getMetric(++count);
            new KProduct<InputMessageData>(0L, "data_platform_metric", bootstrapServers) {
                @Override
                public List<InputMessageData> setMsg() {
                    return metric;
                }
            }.send();
            System.out.println("done");
//            //粒度
            Thread.sleep(1000L);
        }
    }

    public static void stream() throws InterruptedException {
        while (true) {
            List<InputMessage> data = Collections.singletonList(ProData.getAutoMetric());
            new KProduct<InputMessage>(0L, "data_platform_metric", bootstrapServers) {
                @Override
                public List<InputMessage> setMsg() {
                    return data;
                }
            }.send();
            System.out.println("done");
////            //粒度
            Thread.sleep(3000L);
//        }
        }
    }

    public static void batch() throws InterruptedException {
        List<InputMessage> list = new ArrayList<>();
        for (int i = 0; i < 1515; i++) {
//            List<InputMessage> data = ProData.getMetrics();
            InputMessage autoMetric = ProData.getAutoMetric();
            list.addAll(Collections.singletonList(autoMetric));
        }
        new KProduct<InputMessage>(0L, "data_platform_metric", bootstrapServers) {
            @Override
            public List<InputMessage> setMsg() {
                return list;
            }
        }.send();
    }

}
