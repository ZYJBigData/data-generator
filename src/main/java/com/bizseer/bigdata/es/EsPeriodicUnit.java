package com.bizseer.bigdata.es;


public enum EsPeriodicUnit implements EnumValue<Integer> {

    SINGLE(0,"单个索引",""),

    DAY(1,"日", "yyyyMMdd"),

    MONTH(2,"月", "yyyyMM"),

    ;

    private int code;

    private String title;

    private String suffixPattern;

    EsPeriodicUnit(int code, String title, String suffixPattern) {
        this.code = code;
        this.title = title;
        this.suffixPattern = suffixPattern;
    }

    @Override
    public Integer getCode() {
        return code;
    }

    public String getTitle() {
        return title;
    }

    public String getSuffixPattern() {
        return suffixPattern;
    }

    public boolean isPeriodic(){
        return this != SINGLE;
    }

    public static EsPeriodicUnit byCode(Integer code){
        if(null == code){
            return null;
        }
        for(EsPeriodicUnit unit : EsPeriodicUnit.values()){
            if(code.intValue() == unit.getCode()){
                return unit;
            }
        }
        return null;
    }

}
