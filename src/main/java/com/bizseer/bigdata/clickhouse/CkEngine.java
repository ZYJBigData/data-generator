package com.bizseer.bigdata.clickhouse;

import org.apache.commons.lang3.StringUtils;

public enum CkEngine implements EnumValue<String> {

    MergeTree,
    ReplicatedMergeTree,
    Distributed;

    @Override
    public String getCode() {
        return this.name();
    }

    public static CkEngine byCode(String code){
        if(StringUtils.isBlank(code)){
            return null;
        }
        for(CkEngine engine : CkEngine.values()){
            if(engine.name().equals(code)){
                return engine;
            }
        }
        return null;
    }

}
