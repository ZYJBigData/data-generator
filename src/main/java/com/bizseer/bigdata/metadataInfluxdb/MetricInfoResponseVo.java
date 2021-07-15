package com.bizseer.bigdata.metadataInfluxdb;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetricInfoResponseVo implements Serializable {

    private static final long serialVersionUID = 93474066855537500L;
    /**
     * 主键ID
     */
    private Long id;

    /**
     * 描述
     */
    private String displayName;

    /**
     * 对象code
     */
    private String modelCode;

    /**
     * 对象名
     */
    private String modelName;

    /**
     * 模型分类名称
     */
    private String modelGroupName;
    /**
     * 指标名
     */
    private String metrics;

    /**
     * 指标来源
     */
    private String metricsSource;

    /**
     * 创建时间
     */
    private Long createTime;

    /**
     * 更新时间
     */
    private Long updateTime;

    /**
     * 删除标记
     */
    private Boolean deleted;

    /**
     * 采集颗粒度
     */
    private Long collectionIntervals;

    /**
     * 备注
     */
    private String remark;

    /**
     * 数据完整度
     */
    private float integrity;

    /**
     * 实际值
     */
    private Long actualNum;

    /**
     * 计算的值
     */
    private Long calculationNum;
}
