package com.bizseer.bigdata.influxdb;

import com.bizseer.bigdata.common.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;


public class InfluxdbSelect {
    //初始化influxDB客户端
    public static InfluxDB influxDB = InfluxDBFactory.connect("http://10.0.60.142:7076","root","bizseer_aiops_2020")
            .setDatabase("data_platform_metric");
    
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Number{
        private Double a;
        private String b;
    }

    public static void main(String[] args) throws JsonProcessingException {
//        QueryResult query = influxDB.query(new Query("select * from add_test_1"), TimeUnit.MILLISECONDS);
//        System.out.println(query);
//        Class<?> aClass = query.getResults().get(0).getSeries().get(0).getValues().get(0).get(0).getClass();
//        System.out.println(aClass);
//        influxDB.close();
        
        String str="{\"a\":1,\"b\":\"2\"}";
        Number number = JsonUtils.objectMapper.readValue(str, Number.class);
        System.out.println(number);
    }
}
