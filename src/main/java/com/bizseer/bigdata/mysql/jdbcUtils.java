package com.bizseer.bigdata.mysql;

import com.bizseer.bigdata.clickhouse.InteractException;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class jdbcUtils {
    public static void main(String[] args) throws InteractException {
        MysqlConnectInfo connectionInfo = new MysqlConnectInfo();
        connectionInfo.setHost("10.0.60.147:3306");
        connectionInfo.setDbName("upm");
        connectionInfo.setUserName("root");
        connectionInfo.setPassword("Bizseer@2020");
//        Boolean aBoolean = jdbcUtils.executeQuery(connectionInfo, "select 1", resultSet -> resultSet.next(), "连接数据库失败!");
//        System.out.println("aBoolean==" + aBoolean);
        insertInData(connectionInfo);
//        Calendar calendar = Calendar.getInstance();
//        calendar.set(1970,01,01);
//        System.out.println(calendar);
    }

    public static void insertInData(MysqlConnectInfo mysqlConnectInfo) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {

            ConnectionHolder holder = getConnectionHolder(mysqlConnectInfo);

            // 创建SQL插入语句
            String sql = "INSERT INTO calendar_detail_test(date, group_id,start_time,end_time) VALUES ";
            StringBuffer stringBuffer = new StringBuffer();
            Calendar calendar = Calendar.getInstance();
            calendar.set(1400,01,01);
            for (int i =0; i < 10000; i++) {
                stringBuffer.append("(");
                calendar.add(Calendar.DATE, 1);//增加一天   
                stringBuffer.append(getDate(calendar)+"000000").append(",");
                stringBuffer.append(i).append(",");
                stringBuffer.append("'"+sdf.format(getStartOfDay(calendar.getTime()).getTime())+"'").append(",");
                stringBuffer.append("'"+sdf.format(getEndOfDay(calendar.getTime()).getTime())+"'");
                if (i < 9999) {
                    stringBuffer.append("),");
                } else {
                    stringBuffer.append(")");
                }
            }
            sql = sql + stringBuffer;
            System.out.println(sql);
            int rowsAffected = holder.executeUpdate(sql, null);
            if (rowsAffected > 0) {
                System.out.println("Insertion successful. Rows affected: " + rowsAffected);
            } else {
                System.out.println("Insertion failed.");
            }
        } catch (InteractException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static java.util.Date getStartOfDay(java.util.Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    public static java.util.Date getEndOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }


    public static String getDate(Calendar calendar) {
        String result = calendar.get(Calendar.YEAR) + "";
        int month = calendar.get(Calendar.MONTH);
        if (calendar.get(Calendar.MONTH) < 10) {
            result = result + "0" + month;
        } else {
            result += month;
        }
        int date = calendar.get(Calendar.DATE);
        if (calendar.get(Calendar.DATE) < 10) {
            result = result + "0" + date;
        } else {
            result += date;
        }
        return result;
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

