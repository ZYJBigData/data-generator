package com.bizseer.bigdata.kafka.test;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class InputMessage {
    private Map<String, Object> header;
    private Metrics metrics;

    @Data
    public static class Metrics {
        private Long timestamp;
        @JsonProperty("metric_code")
        private String metricCode;
        private Object value;
        @JsonProperty("metric_name")
        private String metricName;
        @JsonProperty("metric_name_en")
        private String metricNameEN;
        @JsonProperty
        private String unit;
        @JsonProperty("custom_fields")
        private List<CustomFieldInfo> customFields;
        private String categoryTitle;
        @JsonProperty("important_level")
        private String importantLevel;
        private String granularity;
        private List<String> labels;
        private String business_system;
        private List<DimensionInfo> dimensions;
    }

    /**
     * 自定义字段
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomFieldInfo {
        @JsonProperty("field_code")
        private String fieldCode;
        @JsonProperty("field_name")
        private String fieldName;
        @JsonProperty("value_type")
        private String valueType;
        @JsonProperty("value")
        private Object value;
    }

    /**
     * 维度属性,用于自动创建数仓模型
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DimensionInfo {
        @JsonProperty("dimension_code")
        private String dimensionCode;
        @JsonProperty("dimension_value")
        private String dimensionValue;
        @JsonProperty("dimension_name")
        private String dimensionName;
        /**
         * 是否主键维度
         */
        @JsonProperty("is_primary")
        private String isPrimary;
        /**
         * 是否分表维度
         */
        @JsonProperty("is_division")
        private String isDivision;
        /**
         * 是否允许为空
         */
        @JsonProperty("allow_null")
        private String allowNull;
        /**
         * 维度说明
         */
        @JsonProperty("dimension_desc")
        private String dimensionDesc;
        @JsonProperty("not_allow_null")
        private String notAllowNull;


        public void setDimensionCode(String dimensionCode) {
            if (!StringUtils.isEmpty(dimensionCode)) {
                this.dimensionCode = dimensionCode.trim();
            }
        }

        public String getUpdateTag() {
            return toString();
        }

        @Override
        public String toString() {
            return "DimensionInfo{" +
                    "dimensionCode='" + dimensionCode + '\'' +
                    ", dimensionName='" + dimensionName + '\'' +
                    ", isPrimary='" + isPrimary + '\'' +
                    ", isDivision='" + isDivision + '\'' +
                    ", allowNull='" + allowNull + '\'' +
                    ", dimensionDesc='" + dimensionDesc + '\'' +
                    '}';
        }
    }

}
