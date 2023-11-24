package com.bizseer.bigdata.es.utils;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * 分页参数
 *
 * @author xuefeng
 */

@Data
public class PageParam<T> implements Serializable{

    private final static Logger logger = LoggerFactory.getLogger(PageParam.class);
    private static final long serialVersionUID = -8730288573373539948L;

    /**
     * 当前第几页
     */
   
    private int currentPage = 1;

    private int pageSize = 10;

    private String order;

    private boolean all;

    private String sort = "DESC";
    
    private T data;

    private String query;

    public PageParam() {
    }

    public PageParam(int currentPage, T data) {
        this(currentPage, 10, data);
    }

    public PageParam(int currentPage, int pageSize, T data) {
        this.currentPage = currentPage;
        if (pageSize>=1){
            this.pageSize = pageSize;
        }
        this.data = data;
    }

    public static <ORIGIN extends PageParam, TARGET extends PageParam> TARGET copyFrom(ORIGIN origin, Class<TARGET> cls, BiConsumer<ORIGIN, TARGET> fillFunc) {
        try {
            TARGET target = cls.newInstance();
            target.setCurrentPage(origin.getCurrentPage());
            target.setPageSize(origin.getPageSize());
            fillFunc.accept(origin, target);
            return target;
        } catch (InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(cls.getName() + " 没有默认无参构造函数", e);
        }
    }

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public int getPageSize() {
        return pageSize;
    }
    

    public static PageParam of(int page, int size) {
        return new PageParam(page, size, null);
    }

    public static PageParam of() {
        return of(1, 10000);
    }

    public int getSkip() {
        return (this.currentPage - 1) * this.pageSize;
    }

}
