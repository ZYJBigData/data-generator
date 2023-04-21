package com.bizseer.bigdata.clickhouse;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

/**
 * @author xiebo
 * @date 2021/11/23 11:46 上午
 */

@Data
@Accessors(chain = true)
public class CKConnectInfo extends ConnectInfo {

    private String userName;

    private String passWord;

    /**
     * 库名
     */
    private String dbName;

    private String tableName;

    /**
     * 表引擎
     */
    private CkEngine tableEngine;

    /**
     * 集群名
     */
    private String clusterName;

    /**
     * MergeTree/Distributed的排序键
     */
    private String orderByField;

    /**
     * MergeTree/Distributed的分区键
     */
    private String partitionField;

    /**
     * ReplicatedMergeTree-分片标识
     */
    private String replicatedMergeTreeShardName;

    /**
     * ReplicatedMergeTree-副本标识
     */
    private String replicatedMergeTreeReplica;

    /**
     * Distributed-本地表名称
     */
    private String distributedLocalTblName;

    /**
     * Distributed-分片规则
     */
    private String distributedShardRule;

    public String getUrl() {
//        return String.format("jdbc:clickhouse://%s:%d/%s", super.getHost(), super.getPort(), dbName);
//        return String.format("jdbc:clickhouse://10.0.100.237:9000，10.0.100.238:9000/cd", super.getHost(), super.getPort(), dbName);
//        return "jdbc:clickhouse://10.0.100.237:8123,10.0.100.238:8123/db?user=default&password=123";
        return "jdbc:clickhouse://10.0.60.144:8123/db?user=default&password=123";
    }

//    @Override
//    public String toString() {
//        return JsonUtil.toJsonString(this);
//    }

    @Override
    public SourceType getSourceType() {
        return SourceType.CLICKHOUSE;
    }

    @Override
    public String toDescription() {
        return String.format("host=%s, port=%d, dbname=%s, tableName=%s", getHost(), getPort(), dbName, tableName);
    }

    public static void main(String[] args) {
        Pattern par = Pattern.compile(JDBC_CLICKHOUSE_PREFIX + "" +
                "//([a-zA-Z0-9_:,.-]+)" +
                "(/[a-zA-Z0-9_]+" +
                "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+)*)?" +
                ")?");
//        String par = "jdbc:clickhouse://([a-zA-Z0-9_:,.-]+)(/[a-zA-Z0-9_]+ ([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+)*)?)?";
//        String url =   "jdbc:clickhouse://10.0.100.237:9000,10.0.100.238:9000/test?user=default&password=123";
        String JDBCurl="jdbc:clickhouse://10.0.100.237:9000,10.0.100.238:9000/t?user=default&password=123";
//        ArrayList<String> objects = new ArrayList<>();
//        objects.add("10.0.100.237:9000");
//        objects.add("10.0.100.238:9000");
//        String addressStr = objects.stream().map(StringUtils::trim).collect(Collectors.joining(","));
//        String jdbcUrl = "jdbc:clickhouse://" + addressStr + "/" + "db";
//        jdbcUrl = jdbcUrl + "?user=" + "default" + "&password=" + "123";
        System.out.println("clickhouse sink connection jdbc url = {}" + JDBCurl);
        Matcher matcher = par.matcher(JDBCurl);
        if (matcher.find()) {
            System.out.println(matcher.group());
        }
    }
}