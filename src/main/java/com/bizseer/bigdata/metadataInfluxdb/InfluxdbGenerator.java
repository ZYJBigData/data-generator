package com.bizseer.bigdata.metadataInfluxdb;

import com.bizseer.bigdata.common.HttpUtils;
import com.bizseer.bigdata.common.JsonUtils;
import com.bizseer.bigdata.influxdb.Metric;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zhangyingjie
 */
@Slf4j
public class InfluxdbGenerator {
    private static final String url_metadata = "http://192.168.1.113:9998";
    private static final String DATA = "data";
    private static final String CODE = "code";

    public static void main(String[] args) {
        InfluxdbGenerator influxDbGenerator = new InfluxdbGenerator();
        List<Metric> metrics = influxDbGenerator.initData();
        log.info("metrics===={}", metrics);
    }

    /**
     * 需要查询两张表来初始化数据
     * 1.从entity中按照model 查询所有的 instance和code
     * 2.从metrics中初始化中metrics名字和名字对应的code（去重）
     * 最终的数据格式为Map<Metrics,Map<instance,code>
     */
    public List<Metric> initData() {
        List<EntityResponseVo> entityResponseVos;
        Map<String, String> mapMetricsModelsCode;
        String metricsStr = HttpUtils.post(url_metadata + "/api/metadata/v1/meta/metrics/list","{}");
        try {
            JsonNode jsonNode = JsonUtils.objectMapper.readValue(metricsStr, JsonNode.class);
            List<MetricInfoResponseVo> metricsList;
            if (jsonNode.get(CODE).asInt() == 0) {
                metricsList = JsonUtils.objectMapper.readValue(jsonNode.get(DATA).toString(),
                        new TypeReference<List<MetricInfoResponseVo>>() {
                        });
            } else {
                log.error("调用metadata出错，错误信息是" + jsonNode.get("msg").asText());
                return null;
            }
            //map集合中 metrics name----->model code
            mapMetricsModelsCode = metricsList.stream().collect(Collectors
                    .toMap(MetricInfoResponseVo::getMetrics, MetricInfoResponseVo::getModelCode, (metricInfoResponseVo1, metricInfoResponseVo2) -> metricInfoResponseVo1));
            String entityStr = HttpUtils.post(url_metadata + "/api/metadata/v1/entity/list_by_model_codes", JsonUtils.
                    objectMapper.writeValueAsString(mapMetricsModelsCode.values()));
            JsonNode entityJsonNode = JsonUtils.objectMapper.readValue(entityStr, JsonNode.class);
            if (entityJsonNode.get(CODE).asInt() == 0) {
                entityResponseVos = JsonUtils.objectMapper.readValue(entityJsonNode.get(DATA).toString(), new TypeReference<List<EntityResponseVo>>() {
                });
            } else {
                log.error("调用metadata出错，错误信息是" + entityJsonNode.get("msg").asText());
                return null;
            }
        } catch (JsonProcessingException e) {
            log.error("json序列化失败");
            return null;
        }
        //map集合中 entity的code----->model的code
        List<Metric> metricObj = new ArrayList<>();
        Map<String, String> entityMap = entityResponseVos.stream().collect(Collectors.toMap(EntityResponseVo::getCode,
                EntityResponseVo::getModelCode,(entityResponseVo1,entityResponseVo2)->entityResponseVo1));
        for (Map.Entry<String, String> mapEntryModel : mapMetricsModelsCode.entrySet()) {
            for (Map.Entry<String, String> mapEntryEntity : entityMap.entrySet()) {
                if (mapEntryModel.getValue().equalsIgnoreCase(mapEntryEntity.getValue())) {
                    Metric metric = new Metric();
                    Map<String, String> tags = new HashMap<>(2);
                    tags.put("instance", mapEntryEntity.getKey());
                    tags.put("model", mapEntryEntity.getValue());
                    metric.measurement = mapEntryModel.getKey();
                    metric.tags = tags;
                    metricObj.add(metric);
                }
            }
        }
        return metricObj;
    }
}
