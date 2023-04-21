package com.bizseer.bigdata.mysql;

import com.bizseer.bigdata.clickhouse.ConnectInfo;
import com.bizseer.bigdata.clickhouse.SourceType;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author xiebo
 * @date 2021/11/23 11:35 上午
 */

@Data
@Accessors(chain = true)
public class MysqlConnectInfo extends ConnectInfo {

    /**
     * 连接超时时间，5s。
     */
    private final static int connectTimeout = 30000;

    /**
     * 用户名
     */
    private String userName;

    /**
     * 用户密码
     */
    private String password;

    /**
     * 数据库
     */
    private String dbName;
    /**
     * 表名
     */
    private String tableName;

    public String getUrl() {
        return String.format("jdbc:mysql://%s/%s?useUnicode=true&useSSL=false&characterEncoding=utf8&autoReconnect=true&connectTimeout=%d", super.getHost(), dbName, connectTimeout);
    }
    @Override
    public SourceType getSourceType() {
        return SourceType.MYSQL;
    }

    @Override
    public String toDescription() {
        return String.format("host=%s,dbname=%s, tableName=%s", getHost(), dbName, tableName);
    }
}