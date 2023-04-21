package com.bizseer.bigdata.clickhouse;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum SourceType implements EnumValue<Integer> {

    UNKNOWN(0, "", new ColDescriptor[]{}),

    MYSQL(1, "MySql",
            new ColDescriptor[]{
                    new ColDescriptor("tinyint", ColDescriptor.INT_LENGTH_VALIDATOR, false, true, ColDescriptor.TYPE_DESC_INTEGER).setLengthValidateErrMsg("tinyint长度格式错误"),
                    new ColDescriptor("smallint", ColDescriptor.INT_LENGTH_VALIDATOR, false, true, ColDescriptor.TYPE_DESC_INTEGER).setLengthValidateErrMsg("smallint长度格式错误"),
                    new ColDescriptor("int", ColDescriptor.INT_LENGTH_VALIDATOR, false, true, ColDescriptor.TYPE_DESC_INTEGER).setLengthValidateErrMsg("int长度格式错误"),
                    new ColDescriptor("bigint", ColDescriptor.INT_LENGTH_VALIDATOR, false, true, ColDescriptor.TYPE_DESC_INTEGER).setLengthValidateErrMsg("bigint长度格式错误"),
                    new ColDescriptor("double", ColDescriptor.FLOAT_LENGTH_VALIDATOR, false, true, ColDescriptor.TYPE_DESC_INTEGER).setLengthValidateErrMsg("double长度格式错误"),
                    new ColDescriptor("float", ColDescriptor.FLOAT_LENGTH_VALIDATOR, false, true, ColDescriptor.TYPE_DESC_NUMBER).setLengthValidateErrMsg("float长度格式错误"),
                    new ColDescriptor("varchar", ColDescriptor.STRING_LENGTH_VALIDATOR, false, true, ColDescriptor.TYPE_DESC_NUMBER).setLengthValidateErrMsg("varchar长度格式错误"),
                    new ColDescriptor("text", ColDescriptor.STRING_LENGTH_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_STRING),
                    new ColDescriptor("date", ColDescriptor.STRING_LENGTH_VALIDATOR, true, false, ColDescriptor.TYPE_DESC_STRING),
                    new ColDescriptor("datetime", ColDescriptor.STRING_LENGTH_VALIDATOR, true, false, ColDescriptor.TYPE_DESC_STRING),
                    new ColDescriptor("timestamp", ColDescriptor.STRING_LENGTH_VALIDATOR, true, false, ColDescriptor.TYPE_DESC_STRING)
            }),
    ES(2, "ElasticSearch", new ColDescriptor[]{
            new ColDescriptor("keyword", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_STRING),
            new ColDescriptor("text", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_STRING),
            new ColDescriptor("short", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
            new ColDescriptor("integer", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
            new ColDescriptor("long", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
            new ColDescriptor("double", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_NUMBER),
            new ColDescriptor("float", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_NUMBER),
            new ColDescriptor("date", ColDescriptor.IGNORE_VALIDATOR, true, false, ColDescriptor.TYPE_DESC_STRING),
            new ColDescriptor("boolean", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_BOOLEAN),
            new ColDescriptor("object", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_OBJECT)
    }),
    CLICKHOUSE(3, "ClickHouse",
            new ColDescriptor[]{
                    new ColDescriptor("Int8", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
                    new ColDescriptor("Int16", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
                    new ColDescriptor("Int32", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
                    new ColDescriptor("Int64", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
                    new ColDescriptor("UInt8", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
                    new ColDescriptor("UInt16", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
                    new ColDescriptor("UInt32", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
                    new ColDescriptor("UInt64", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_INTEGER),
                    new ColDescriptor("Float32", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_NUMBER),
                    new ColDescriptor("Float64", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_NUMBER),
                    new ColDescriptor("String", ColDescriptor.IGNORE_VALIDATOR, false, false, ColDescriptor.TYPE_DESC_STRING),
                    new ColDescriptor("Date", ColDescriptor.IGNORE_VALIDATOR, true, false, ColDescriptor.TYPE_DESC_STRING),
                    new ColDescriptor("Datetime", ColDescriptor.IGNORE_VALIDATOR, true, false, ColDescriptor.TYPE_DESC_STRING),
                    new ColDescriptor("Datetime64", ColDescriptor.IGNORE_VALIDATOR, true, false, ColDescriptor.TYPE_DESC_STRING)
            });

    private final int code;

    private final String title;

    /**
     * 该存储的字段类型
     */
    private final String[] fieldTypes;

    /**
     * 该存储的时间字段类型
     */
    private final String[] timedFieldTypes;

    /**
     * 存储CK的整形字段类型
     */
    private static final String[] ckNumFieldTypes = {"Int8","Int16","Int32","Int64","UInt8","UInt16","UInt32","UInt64","Float32","Float64"};

    /**
     * 存储CK的日期字段类型
     */
    private static final String[] ckDateFieldTypes = {"Date","Datetime","Datetime64"};

    /**
     * 存储Mysql的整形字段类型
     */
    private static final String[] mysqlNumFieldTypes = {"tinyint","smallint","int","bigint","double","float"};


    private ColDescriptor[] colDescriptors;


    SourceType(int code, String title, ColDescriptor[] colDescriptors) {
        this.code = code;
        this.title = title;
        this.fieldTypes = Arrays.stream(colDescriptors).map(ColDescriptor::getTypeName).collect(Collectors.toList()).toArray(new String[]{});
        this.timedFieldTypes = Arrays.stream(colDescriptors).filter(ColDescriptor::isTimedField).map(ColDescriptor::getTypeName).collect(Collectors.toList()).toArray(new String[]{});
        this.colDescriptors = colDescriptors;
    }

    @Override
    public Integer getCode() {
        return code;
    }

    public String getTitle() {
        return title;
    }

    public String[] getFieldTypes() {
        return fieldTypes;
    }

    public String[] getTimedFieldTypes() {
        return timedFieldTypes;
    }

    public ColDescriptor[] getColDescriptors() {
        return colDescriptors;
    }

    public ColDescriptor getColDescriptor(String colType) {
        if (StringUtils.isBlank(colType)) {
            return null;
        }
        for (ColDescriptor descriptor : colDescriptors) {
            if (descriptor.getTypeName().equals(colType)) {
                return descriptor;
            }
        }
        return null;
    }

    public boolean isCorrectColType(String colType) {
        if (StringUtils.isBlank(colType)) {
            return false;
        }
        for (String fieldType : fieldTypes) {
            if (fieldType.equals(colType)) {
                return true;
            }
        }
        return false;
    }

    public boolean isTimedColType(String colType) {
        if (StringUtils.isBlank(colType)) {
            return false;
        }
        for (String fieldType : timedFieldTypes) {
            if (fieldType.equalsIgnoreCase(colType)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isMysqlNumColType(String colType) {
        if (StringUtils.isBlank(colType)) {
            return false;
        }
        for (String fieldType : mysqlNumFieldTypes) {
            if (fieldType.equalsIgnoreCase(colType)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isCkNumColType(String colType) {
        if (StringUtils.isBlank(colType)) {
            return false;
        }
        for (String fieldType : ckNumFieldTypes) {
            if (fieldType.equalsIgnoreCase(colType)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isCkDateColType(String colType) {
        if (StringUtils.isBlank(colType)) {
            return false;
        }
        for (String fieldType : ckDateFieldTypes) {
            if (fieldType.equalsIgnoreCase(colType)) {
                return true;
            }
        }
        return false;
    }

    public String getColTypeDesc(String colType){
        if(StringUtils.isBlank(colType)){
            return "";
        }
        for(ColDescriptor colDescriptor : getColDescriptors()){
            if(colDescriptor.getTypeName().equals(colType)){
                return colDescriptor.getTypeDesc();
            }
        }
        return colType;
    }

    public static SourceType[] types() {
        return new SourceType[]{MYSQL, ES, CLICKHOUSE};
    }

    public static boolean isInvalid(SourceType sourceType) {
        return null == sourceType || sourceType == UNKNOWN;
    }

    public static boolean isInvalidCode(Integer sourceTypeCode) {
        if (null == sourceTypeCode) {
            return true;
        }
        return isInvalid(SourceType.byCode(sourceTypeCode));
    }

    public static SourceType byCode(Integer code) {
        if (null == code) {
            return null;
        }
        for (SourceType type : SourceType.types()) {
            if (type.code == code) {
                return type;
            }
        }
        return null;
    }

    public static boolean isNeedLength(SourceType sourceType, String colType) {
        if (null == sourceType || sourceType == UNKNOWN) {
            throw new CustomException("SourceType 错误 :" + sourceType);
        }
        if (StringUtils.isBlank(colType)) {
            throw new CustomException("缺少列类型");
        }
        ColDescriptor descriptor = sourceType.getColDescriptor(colType);
        if (null == descriptor) {
            throw new CustomException("列类型错误：" + colType);
        }

        return descriptor.isNeedLength();

    }

    public static void validateCol(SourceType sourceType, String colName, String colType, String inputColLength) {
        if (null == sourceType || sourceType == UNKNOWN) {
            throw new CustomException("SourceType 错误 :" + sourceType);
        }
        if (StringUtils.isBlank(colType)) {
            throw new CustomException(colName + " 缺少列类型");
        }
        if (!sourceType.isCorrectColType(colType)) {
            throw new CustomException(colName + " 列类型错误：" + colType);
        }

        ColDescriptor descriptor = sourceType.getColDescriptor(colType);
        if (null == descriptor) {
            throw new CustomException(colName + " 列类型错误：" + colType);
        }

        if (descriptor.isNeedLength()) {
            if (!descriptor.getInputLengthValidator().validate(inputColLength)) {
                throw new CustomException(String.format("`%s`的类型`%s`长度格式`%s`输入错误: %s", colName, colType, inputColLength, StringUtils.defaultString(descriptor.getLengthValidateErrMsg(), "")));
            }
        }

    }
}
