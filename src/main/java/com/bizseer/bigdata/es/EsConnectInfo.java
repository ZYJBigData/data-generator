package com.bizseer.bigdata.es;

import com.bizseer.bigdata.clickhouse.ConnectInfo;
import com.bizseer.bigdata.clickhouse.SourceType;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDate;

@Data
@Accessors(chain = true)
public class EsConnectInfo extends ConnectInfo {
    /**
     * 用户
     */
    private String username;
    /**
     * 密码
     */
    private String password;
    private String index;

    private EsPeriodicUnit periodicUnit;

    private int shard = 1;

    private int replica = 1;

    /**
     * 指定的创建时间后缀
     */
    private LocalDate indexCreateDate;

    @Override
    public String toString() {
        return null;
//        return JsonUtil.toJsonString(this);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ES;
    }

    @Override
    public String toDescription() {
        return String.format("host=%s , index=%s", getHost(), index);
    }
}