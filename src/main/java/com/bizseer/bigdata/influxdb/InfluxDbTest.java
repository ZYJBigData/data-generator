package com.bizseer.bigdata.influxdb;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;


/**
 * @author zhangyingjie
 */
public class InfluxDbTest {
    public static void main(String[] args) {
        InfluxDB influxDb = InfluxDBFactory.connect("http://192.168.10.133:8086")
                .setDatabase("testDB")
                .setRetentionPolicy("autogen");
        QueryResult query = influxDb.query(new Query("select * from cpu"));
        System.out.println(query);
    }
}
