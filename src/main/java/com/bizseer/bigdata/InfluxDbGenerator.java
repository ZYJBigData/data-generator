package com.bizseer.bigdata;

import com.beust.jcommander.Parameter;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author zhangyingjie
 */
public class InfluxDbGenerator {
    final static int APPLICATION_NUMBER = 20;
    final static int SERVICE_NUMBER = 10;
    final static int PROCESS_NUMBER = 5;
    final static int HOST_NUMBER = 10;
    final static Random RANDOM = new Random();

    @Parameter(names = "--interval", description = "采样间隔")
    int interval = 10;
    @Parameter(names = "--cpu", description = "cpu数量,也就是measurement的个数")
    int cpuNum = 20;
    @Parameter(names = "--cpu-db", description = "influxdb数据库名字")
    String cpuDb = "metric";
    @Parameter(names = "--server", description = "influxdb服务地址")
    String server = "http://10.0.90.74:8086";
    @Parameter(names = "--executor", description = "任务执行的线程数")
    Integer executor = 3;

    public static void main(String[] args) {
        InfluxDbGenerator cli = new InfluxDbGenerator();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Map<String, String>> tags = initTag();
        long time = System.currentTimeMillis() / 1000 / cli.interval * cli.interval * 1000 - 24 * 60 * 60 * 1000;
        InfluxDB influxDb = InfluxDBFactory.connect(cli.server);
        ScheduledExecutorService es = Executors.newScheduledThreadPool(cli.executor);
        es.scheduleAtFixedRate(() -> {
            System.out.println(sdf.format(System.currentTimeMillis()) + ": start!");
            for (Map<String, String> tag : tags) {
                for (int i = 0; i < cli.cpuNum; i++) {
                    Point point = Point.measurement("cpu_" + i).tag(tag)
                            .addField("value", RANDOM.nextFloat() * 100)
                            .time(time, TimeUnit.MILLISECONDS).build();
                    influxDb.write(cli.cpuDb, "autogen", point);
                }
            }
            System.out.println(sdf.format(System.currentTimeMillis()) + ": done!");
        }, 0, cli.interval, TimeUnit.SECONDS);
    }

    private static List<Map<String, String>> initTag() {
        List<Map<String, String>> result = new ArrayList<>();
        for (int application = 1; application <= APPLICATION_NUMBER; application++) {
            for (int service = 1; service <= SERVICE_NUMBER; service++) {
                for (int process = 1; process <= PROCESS_NUMBER; process++) {
                    for (int host = 1; host <= HOST_NUMBER; host++) {
                        Map<String, String> tags = new HashMap<>(4);
                        tags.put("application", "application_" + application);
                        tags.put("service", "service_" + service);
                        tags.put("process", "process_" + process);
                        tags.put("host", "host_" + host);
                        result.add(tags);
                    }
                }
            }
        }
        return result;
    }
}
