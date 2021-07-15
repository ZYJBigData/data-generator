package com.bizseer.bigdata.metadataInfluxdb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author zhangyingjie
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EntityPropertyResponseVo implements Serializable {

    private static final long serialVersionUID = 6548673437315538007L;
    /**
     * 主键
     */
    private Integer id;

    /**
     * 实体code
     */
    private String entityCode;

    /**
     * 对象code
     */
    private String modelCode;

    /**
     * 属性名
     */
    private String propertyName;

    /**
     * 属性code
     */
    private String propertyCode;

    /**
     * 属性值
     */
    private String propertyValue;

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
}
