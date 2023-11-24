package com.bizseer.bigdata.es.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * @author xiebo
 */
public class JsonUtil {

    private final static Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    private final static ObjectMapper MAPPER = new ObjectMapper();

    static {
        //对象转换为json时，输出所有字段，不管是否为空。
        MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        //json解析为对象时，属性不存在时不报错。
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //解析json时，允许不带引号的字段名。
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        //解析json时，允许单引号。
        MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        //解析json时，数组中允许数据缺失，解析为null。
        MAPPER.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    public static ObjectMapper getObjectMapper(){
        return MAPPER;
    }

    public static byte[] toBytes(Object obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public static String toJsonString(Object obj) {
        return toJsonString(obj, false);
    }

    public static String toJsonString(Object obj, boolean pretty) {
        return toJsonString(obj, "{}", pretty);
    }


    public static String toJsonString(Object obj, String defaultStrIfNull) {
        return toJsonString(obj, defaultStrIfNull, false);

    }

    public static String toJsonString(Object obj, String defaultStrIfNull, boolean pretty) {
        if (null == obj) {
            return defaultStrIfNull;
        }
        try {
            if (pretty) {
                return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
            }
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
            return defaultStrIfNull;
        }

    }

    public static <T> T toObject(String jsonStr, Class<T> cls) {
        return toObject(jsonStr, cls, null);
    }

    public static <T> T toObject(String jsonStr, Class<T> cls, T defaultValue) {
        if (StringUtils.isEmpty(jsonStr)) {
            return defaultValue;
        }

        try {
            return MAPPER.readValue(jsonStr, cls);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return defaultValue;
        }

    }

    public static <T> T toObject(byte[] bytes, Class<T> cls) {

        try {
            return MAPPER.readValue(bytes, cls);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }

    }

    public static LinkedHashMap<String, Object> toMap(String jsonStr) {
        if (StringUtils.isBlank(jsonStr)) {
            return new LinkedHashMap<>(0);
        }

        try {
            return MAPPER.readValue(jsonStr, LinkedHashMap.class);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return new LinkedHashMap<>(0);
        }

    }

    public static JsonNode toJsonNode(String jsonStr) {
        try {
            return MAPPER.readTree(jsonStr);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public static ObjectNode toObjectNode(String jsonStr) {
        try {
            return (ObjectNode) MAPPER.readTree(jsonStr);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public static ObjectMapper getMapper() {
        return MAPPER;
    }
}
