package com.bizseer.bigdata.clickhouse;

import lombok.extern.slf4j.Slf4j;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class CKTest {
    public static void main(String[] args) throws InteractException, SQLException {
        Connection connection = getConnection(getCKConnectInfo());
        Statement stmt = connection.createStatement();
        String createTable=" CREATE TABLE db.zyj_test (\n" +
                "name Nullable(String),age Nullable(Int8),id Int8)\n" +
                " ENGINE=ReplicatedMergeTree('/ck/t/01/zyj_test', 'rep_01_02')\n" +
                " PRIMARY KEY (id)\n" +
                " ORDER BY id;";
        
        int resultSet = stmt.executeUpdate(createTable);
        
//        ResultSet resultSet = stmt.executeQuery("SELECT name FROM system.tables WHERE database= 'test' and name ='zyj_test' and engine='ReplicatedMergeTree'");
        log.info("返回的结果：=={}",resultSet);

    }
    public static Connection getConnection(CKConnectInfo connectInfo) throws InteractException {
        ClickHouseConnection clickHouseConnection;
        ClickHouseProperties ckProperties = new ClickHouseProperties();
//        ckProperties.setUser(connectInfo.getUserName());
//        ckProperties.setPassword(connectInfo.getPassWord());
        ckProperties.setDatabase(connectInfo.getDbName());
        BalancedClickhouseDataSource clickHouseDataSource = new BalancedClickhouseDataSource(connectInfo.getUrl());
        try {
            clickHouseConnection = clickHouseDataSource.getConnection();
            return clickHouseConnection;
        } catch (Exception e) {
            String msg = String.format("获取数据库连接失败:url=%s,userName=%s,password=%s\n" + e.getMessage(), connectInfo.getUrl(), connectInfo.getUserName(), connectInfo.getPassWord());
            throw new InteractException(msg).setConnectInfo(connectInfo);
        }
    }


    public static CKConnectInfo getCKConnectInfo() {
        String userName = "default";
        String passWord = "123";
        String dbName = "db";
        String host = "10.0.60.144:8123,10.0.60.145:8123";
        int port = 83;
        String tableName = "zyj_test";
        CkEngine tableEngine = CkEngine.MergeTree;
        String shardName = "/ck/t/01/zxg_test";
        String clusterName = "gmall_cluster";
        //ReplicatedMergeTree('/clickhouse/tables/{shard}/st_order_mt','{replica}')
        String replica = "zyj_test";
        //engine = Distributed(gmall_cluster,default, st_order_mt,hiveHash(sku_id));
        String shardRule = "rand()";
        String orderByFiled = "(col1,col4)";
        String partitionFiled = "toYYYYMMDD(col3)";

        CKConnectInfo ckConnectInfo = new CKConnectInfo();
        ckConnectInfo.setDbName(dbName).setUserName(userName).setPassWord(passWord).setDistributedLocalTblName("zxg_test1010")
                .setTableName(tableName).setTableEngine(tableEngine).setReplicatedMergeTreeReplica(replica).setDistributedShardRule(shardRule)
                .setReplicatedMergeTreeShardName(shardName).setClusterName(clusterName).setOrderByField(orderByFiled).setPartitionField(partitionFiled).setHost(host).setPort(port);

        return ckConnectInfo;
    }
}
