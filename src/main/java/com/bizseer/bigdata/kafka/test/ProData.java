package com.bizseer.bigdata.kafka.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class ProData {
    static List<String> dim001 = Arrays.asList("10.8.80.161", "10.8.80.162", "10.8.80.163", "10.8.80.164", "10.8.80.165", "10.8.80.166");
    static List<String> dim002 = Arrays.asList("online_trade_system", "order_system");
    static List<String> dim003 = Arrays.asList("PodName","app");
    static Random random = new Random();

    public static List<InputMessage> getMetrics() {
        // 普通指标
        List<InputMessage> list = new ArrayList<>();

        long time = 1688055045000L;

        for (int j = 1; j <= 1; j++) {
            for (int i = 0; i < 2; i++) {
                InputMessage.Metrics metrics = new InputMessage.Metrics();
                metrics.setMetricCode("zyj_test_017");
                metrics.setMetricName("指标中文名");
                metrics.setValue(String.valueOf(i + 1));
                metrics.setTimestamp(time + (i * 60000));
                HashMap<String, Object> header = new HashMap<>();
                header.put("metric_group", "zyj_test_001");
                header.put("value_type", "float");

                InputMessage inputMessage = new InputMessage();
                inputMessage.setMetrics(metrics);
                inputMessage.setHeader(header);
                InputMessage.DimensionInfo dimensionInfo2 = new InputMessage.DimensionInfo();
                dimensionInfo2.setDimensionCode("dim_01");
                dimensionInfo2.setDimensionValue(dim001.get(random.nextInt(5)));

                InputMessage.DimensionInfo dimensionInfo1 = new InputMessage.DimensionInfo();
                dimensionInfo1.setDimensionCode("dim_02");
                dimensionInfo1.setDimensionValue(dim002.get(random.nextInt(1)));

                metrics.setDimensions(Arrays.asList(dimensionInfo1, dimensionInfo2));
                list.add(inputMessage);
            }
        }
        return list;
    }

    public static List<InputMessage> getMetrics4() {
        String str = "" +
                "{\"header\": {\"metric_group\": \"linux_cpu\", \"key\": \"123\", \"biz_category_id\": 1, \"value_type\": \"string\", \"source \": \"OK\", \"is_auto_model\": \"true\", \"reserve\": 60}, \"metrics\": {\"timestamp\": 1695019555566, \"metric_code\": \"heiha_20230921\", \"metric_name\": \"LIXUEJIN_TESTING_STRING_20230918\", \"unit\": \"kg\", \"value\": \"AS_I0t8gufeXk9ElK4oTSd\", \"business_system\": \"\\u624b\\u673a\\u8bc1\\u5238\", \"granularity\": \"60\", \"important_level\": \"\\u9ec4\\u91d1\\u4ea4\\u6613\\u6307\\u6807\", \"dimensions\": [{\"dimension_code\": \"ip\", \"is_primary\": true, \"is_division\": true, \"not_allow_null\": true, \"dimension_value\": \"10.0.100.12\"}, {\"dimension_code\": \"adds\", \"is_primary\": true, \"is_division\": false, \"not_allow_null\": false, \"dimension_name\": \"adds\", \"dimension_value\": \"AS_cIyTn16X2WHRghtwAM9\"}, {\"dimension_code\": \"host\", \"is_primary\": true, \"is_division\": false, \"not_allow_null\": false, \"dimension_name\": \"host\", \"dimension_value\": false}, {\"dimension_code\": \"wd01\", \"is_primary\": false, \"is_division\": false, \"not_allow_null\": true, \"dimension_name\": \"wd01\", \"dimension_value\": true}]}}";
        ObjectMapper objectMapper = new ObjectMapper();
        InputMessage inputMessage;
        ArrayList<InputMessage> inputMessages = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                inputMessage = objectMapper.readValue(str, InputMessage.class);
                inputMessage.getMetrics().setTimestamp(System.currentTimeMillis());
                inputMessages.add(inputMessage);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return inputMessages;
    }

    public static List<InputMessage> getMetrics3() {

        List<InputMessage> list = new ArrayList<>();


        for (int j = 1; j <= 1; j++) {
            for (int i = 0; i < 5; i++) {
                long time = System.currentTimeMillis();
                InputMessage.Metrics metrics = new InputMessage.Metrics();
                metrics.setMetricCode("CPU usage");
                metrics.setMetricName("指标中文名");
                metrics.setValue(random.nextFloat());
                metrics.setTimestamp(time + (i * 60000));
                HashMap<String, Object> header = new HashMap<>();
                header.put("metric_group", "zyj_test_002");
                header.put("value_type", "float");
                header.put("is_auto_model", true);
                header.put("reserve", 7);


                InputMessage inputMessage = new InputMessage();
                inputMessage.setMetrics(metrics);
                inputMessage.setHeader(header);
                InputMessage.DimensionInfo dimensionInfo1 = new InputMessage.DimensionInfo();
                dimensionInfo1.setDimensionCode("class_name");
                dimensionInfo1.setDimensionValue(dim001.get(random.nextInt(5)));


                InputMessage.DimensionInfo dimensionInfo2 = new InputMessage.DimensionInfo();
                dimensionInfo2.setDimensionCode("ip");
                dimensionInfo2.setDimensionValue(dim002.get(random.nextInt(1)));

//                //增加分表维度
//                InputMessage.DimensionInfo dimensionInfo3 = new InputMessage.DimensionInfo();
//                dimensionInfo3.setDimensionCode("dim_003");
//                dimensionInfo3.setIsDivision("true");
//                dimensionInfo3.setIsPrimary("true");
//                dimensionInfo3.setDimensionValue("192.168." + random.nextInt() + "." + random.nextInt());

                metrics.setDimensions(Arrays.asList(dimensionInfo1, dimensionInfo2));
                list.add(inputMessage);
            }
        }
        return list;
    }

    public static List<InputMessage> getMetrics2() {

        List<InputMessage> list = new ArrayList<>();

        long time = 1686585600000L;

        for (int j = 1; j <= 10; j++) {
            for (int i = 0; i < 100; i++) {
                InputMessage.Metrics metrics = new InputMessage.Metrics();
                metrics.setMetricCode("test_02");
                metrics.setMetricName("test_02");
                metrics.setValue(i % 2 == 0 ? "true" : "false");
                metrics.setTimestamp(time + (i * 60000));
                HashMap<String, Object> header = new HashMap<>();
                header.put("metric_group", "test_02");
                header.put("value_type", "boolean");

                InputMessage inputMessage = new InputMessage();
                inputMessage.setMetrics(metrics);
                inputMessage.setHeader(header);
                InputMessage.DimensionInfo dimensionInfo1 = new InputMessage.DimensionInfo();
                dimensionInfo1.setDimensionCode("dim_02");
                dimensionInfo1.setDimensionValue(j + "");

                InputMessage.DimensionInfo dimensionInfo2 = new InputMessage.DimensionInfo();
                dimensionInfo2.setDimensionCode("dim_01");
                dimensionInfo2.setDimensionValue("1");

//            DimensionInfo dimensionInfo3 = new DimensionInfo();
//            dimensionInfo3.setDimensionCode("tag6");
//            dimensionInfo3.setDimensionValue("val6");

                metrics.setDimensions(Arrays.asList(dimensionInfo1, dimensionInfo2));
                list.add(inputMessage);
            }
        }


        return list;
    }

    public static InputMessage getBaseMetric() {

        InputMessage.Metrics metrics = new InputMessage.Metrics();

        metrics.setMetricCode("test_metric_code_15");
        metrics.setMetricName("指标名称_1");
        metrics.setValue("11");
        metrics.setGranularity("60");
        metrics.setLabels(Collections.singletonList("c"));
        metrics.setTimestamp(System.currentTimeMillis());
        HashMap<String, Object> header = new HashMap<>();
        header.put("reserve", "90");
        header.put("metric_group", "group_1");
        header.put("biz_category_id", "111");
        header.put("value_type", "float");
        InputMessage inputMessage = new InputMessage();

        inputMessage.setMetrics(metrics);
        inputMessage.setHeader(header);

        InputMessage.DimensionInfo dimensionInfo1 = new InputMessage.DimensionInfo();
        dimensionInfo1.setDimensionCode("twa");
        dimensionInfo1.setDimensionValue("2");

        InputMessage.DimensionInfo dimensionInfo2 = new InputMessage.DimensionInfo();
        dimensionInfo2.setDimensionCode("tag4");
        dimensionInfo2.setDimensionValue("4");

        InputMessage.DimensionInfo dimensionInfo3 = new InputMessage.DimensionInfo();
        dimensionInfo3.setDimensionCode("tag6");
        dimensionInfo3.setDimensionValue("6");

        metrics.setDimensions(Arrays.asList(dimensionInfo1, dimensionInfo2, dimensionInfo3));

        return inputMessage;
    }


    public static InputMessage getAutoMetric() {

        InputMessage.Metrics metrics = new InputMessage.Metrics();
        InputMessage.CustomFieldInfo customFieldInfo1 = new InputMessage.CustomFieldInfo("filedCode1", "fieldName1", "Array", "1,2,3");
        InputMessage.CustomFieldInfo customFieldInfo2 = new InputMessage.CustomFieldInfo("filedCode2", "fieldName2", "String", "filed_2");
        InputMessage.CustomFieldInfo customFieldInfo3 = new InputMessage.CustomFieldInfo("filedCode3", "fieldName3", "number", "100");

        metrics.setMetricNameEN("auto_1129_01");
        metrics.setCustomFields(Arrays.asList(customFieldInfo1, customFieldInfo2,customFieldInfo3));
        metrics.setMetricCode("auto_1129_01");
        metrics.setMetricName("指标中名称_1");
        metrics.setValue("11");
        metrics.setGranularity("60");
        metrics.setLabels(Collections.singletonList("c"));

        metrics.setTimestamp(6000 + System.currentTimeMillis());

        HashMap<String, Object> header = new HashMap<>();
        header.put("reserve", "90");
        header.put("metric_group", "group_1");
        header.put("biz_category_id", "111");

        header.put("is_auto_model", "true");
        header.put("value_type", "float");
        header.put("biz_category_title","PodName");


        InputMessage inputMessage = new InputMessage();

        inputMessage.setMetrics(metrics);
        inputMessage.setHeader(header);


        InputMessage.DimensionInfo dimensionInfo1 = new InputMessage.DimensionInfo();
        dimensionInfo1.setDimensionCode("twa");
        dimensionInfo1.setDimensionValue("2");

        InputMessage.DimensionInfo dimensionInfo2 = new InputMessage.DimensionInfo();
        dimensionInfo2.setDimensionCode("tag4");
        dimensionInfo2.setDimensionValue(dim001.get(random.nextInt(6)));

        InputMessage.DimensionInfo dimensionInfo3 = new InputMessage.DimensionInfo();

        dimensionInfo3.setDimensionCode("model");
        dimensionInfo3.setDimensionValue("PodName");
        dimensionInfo3.setIsPrimary("true");
        dimensionInfo3.setAllowNull("false");

        InputMessage.DimensionInfo dimensionInfo4 = new InputMessage.DimensionInfo();
        dimensionInfo4.setDimensionCode("tag2");
        dimensionInfo4.setDimensionValue("2");
        dimensionInfo4.setIsPrimary("true");
        dimensionInfo4.setAllowNull("false");

        InputMessage.DimensionInfo dimensionInfo5 = new InputMessage.DimensionInfo();

        dimensionInfo5.setDimensionCode("instance");
        dimensionInfo5.setDimensionValue("ts.paymentservice-1");
        dimensionInfo5.setIsPrimary("true");
        dimensionInfo5.setAllowNull("false");


        metrics.setDimensions(Arrays.asList(dimensionInfo1, dimensionInfo2,dimensionInfo3,dimensionInfo4,dimensionInfo5));

        return inputMessage;
    }
    
}
