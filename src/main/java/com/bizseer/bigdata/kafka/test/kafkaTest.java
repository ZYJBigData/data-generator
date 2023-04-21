package com.bizseer.bigdata.kafka.test;

import com.alibaba.druid.DbType;
import com.bizseer.bigdata.mysql.SelectSqlBuilder;
import com.sun.deploy.util.StringUtils;

import java.util.*;

public class kafkaTest {
    static List<String> dim001 = Arrays.asList("10.8.80.161", "10.8.80.162", "10.8.80.163", "10.8.80.164", "10.8.80.165", "10.8.80.165");
    static List<String> dim002 = Arrays.asList("online_trade_system", "order_system");
        static String measurementName = "goldTrade_model_02";
//    static String measurementName = "zyj_test07";

//    static String measurementName = "derived_model_lyy";


    public static void main(String[] args) throws InterruptedException {
        stream();
//       batch();

    }

    public static void stream() throws InterruptedException {
        Long time = 15 * 1000L;
        Long latelyMinutesMillis = DateUtils.getLatelyMinutesMillis(System.currentTimeMillis() - time, DateUtils.TimeMillis.MINUTE_MILLIS);
        while (true) {
//            Long latelyMinutesMillis = DateUtils.getLatelyMinutesMillis(System.currentTimeMillis(), DateUtils.TimeMillis.MINUTE_MILLIS);
            latelyMinutesMillis = latelyMinutesMillis + 1000;
            List<InputMessage> data = getData(latelyMinutesMillis);
            new KProduct<InputMessage>(0L, "data_platform_metric") {
                @Override
                public List<InputMessage> setMsg() {
                    return data;
                }
            }.send();
            System.out.println("done");
            //粒度
            Thread.sleep(5 * 1000L);
        }
    }

    public static void batch() {

        Long latelyMinutesMillis = DateUtils.getLatelyMinutesMillis(System.currentTimeMillis(), DateUtils.TimeMillis.MINUTE_MILLIS);
        List<InputMessage> list = new ArrayList<>();
        for (int i = 0; i < 1440; i++) {
            List<InputMessage> data = getData(latelyMinutesMillis - (i * 1000L * 60));
            list.addAll(data);
        }
        new KProduct<InputMessage>(0L, "data_platform_metric") {
            @Override
            public List<InputMessage> setMsg() {
                return list;
            }
        }.send();
    }


    static Random random = new Random();
    static int num = 1;

    public static List<InputMessage> getData(long time) {

        HashMap<String, String> dim = new HashMap<>();
        dim.put("dim_01", dim001.get(random.nextInt(dim001.size())));
        dim.put("dim_02", dim002.get(random.nextInt(dim002.size())));

        HashMap<String, Integer> metric = new HashMap<>();
        metric.put("metric_01", 1);
        metric.put("metric_02", num++);
        metric.put("metric_03", num++);

        InputMessage.Header header = new InputMessage.Header();
        header.setBizseer_metric_version(1);
        header.setOwner("zyj");

        InputMessage inputMessage = new InputMessage();
        inputMessage.setHeader(header);
        inputMessage.setMeasurement(new InputMessage.Measurement(measurementName, time, metric, dim));
        System.out.println(inputMessage);
        return Collections.singletonList(inputMessage);
    }

}
