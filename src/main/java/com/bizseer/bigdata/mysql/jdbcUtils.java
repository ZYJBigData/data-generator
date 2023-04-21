package com.bizseer.bigdata.mysql;

import com.bizseer.bigdata.clickhouse.InteractException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class jdbcUtils {
    public static void main(String[] args) throws InteractException {
        MysqlConnectInfo connectionInfo = new MysqlConnectInfo();
        connectionInfo.setHost("10.0.90.76:3306");
        connectionInfo.setDbName("data_model");
        connectionInfo.setUserName("ro");
        connectionInfo.setPassword("Bizseer@2020");
        Boolean aBoolean = jdbcUtils.executeQuery(connectionInfo, "select 1", resultSet -> resultSet.next(), "连接数据库失败!");
        System.out.println("aBoolean==" + aBoolean);
    }

    public static <R> R executeQuery(MysqlConnectInfo connectInfo, String sql, ResultSetExtractor<R> resultSetExtractor, String errMsg) throws InteractException {
        ConnectionHolder holder = getConnectionHolder(connectInfo);
        try {
            ResultSet resultSet = holder.executeQuery(sql);
            return resultSetExtractor.extract(resultSet);
        } catch (SQLException e) {
            throw new InteractException(e.getMessage(), e, errMsg).setConnectInfo(connectInfo);
        }
    }

    private static ConnectionHolder getConnectionHolder(MysqlConnectInfo connectInfo) throws InteractException {
        Connection connection = getConnection(connectInfo);
        return new ConnectionHolder(connection);
    }

    private static Connection getConnection(MysqlConnectInfo connectInfo) throws InteractException {
        Connection conn;
        String url = connectInfo.getUrl();
        String userName = connectInfo.getUserName();
        String password = connectInfo.getPassword();
        try {
            conn = DriverManager.getConnection(url, userName, password);
        } catch (Exception e) {
            String msg = String.format("获取数据库连接失败:url=%s,userName=%s,password=%s\n" + e.getMessage(), url, userName, password);
            throw new InteractException(msg).setConnectInfo(connectInfo);
        }
        return conn;
    }
}

