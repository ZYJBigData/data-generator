package com.bizseer.bigdata.metadataInfluxdb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.List;

/**
 * @author zhangyingjie
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EntityResponseVo implements Serializable {

    private static final long serialVersionUID = -7408340814299835212L;
    /**
     * 主键
     */
    private Integer id;

    /**
     * 实体唯一标识
     */
    @NotNull
    private String code;

    /**
     * 分类唯一标识
     */
    @NotNull
    private String groupCode;

    /**
     * 分类名称
     */
    private String groupName;

    /**
     * 对象唯一标识
     */
    @NotNull
    private String modelCode;

    /**
     * 对象名称
     */
    private String modelName;

    /**
     * 创建时间
     */
    private Long createTime;

    /**
     * 修改时间
     */
    private Long updateTime;

    /**
     * 删除标记
     */
    private Boolean deleted;

    /**
     * 实例属性值
     */
    private List<EntityPropertyResponseVo> properties;

    /**
     * 指标数量
     */
    private Integer metricCount;

    /**
     * 对象图标
     */
    private String icon;
}
