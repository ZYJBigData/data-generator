package com.bizseer.bigdata.es.utils;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author xiebo
 * @date 2021/11/25 9:56 上午
 */

@Data
@Accessors(chain = true)
public class PageView<T> {
    public static final int DEFAULT_CURRENT_PAGE = 1;

    public static final int DEFAULT_PAGE_SIZE = 10;

    /**
     * 当前第几页
     */
    private int currentPage = DEFAULT_CURRENT_PAGE;

    /**
     * 单页记录数
     */
    private int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * 总页数
     */
    private int totalPage = 0;

    /**
     * 总记录数
     */
    private long totalRecord = 0;

    /**
     * 数据
     */
    private List<T> items;

    /**
     * ES 深度分页key
     */
    private String afterKey;

    //兼容知识图谱字段
//    @Deprecated
//    public List<T> getData() {
//        return getItems();
//    }
//    @Deprecated
//    public int getCurrent() {
//        return getCurrentPage();
//    }
//    @Deprecated
//    public long getTotal() {
//        return getTotalRecord();
//    }

    public List<T> getItems() {
        if (CollectionUtils.isEmpty(items)) {
            return Collections.emptyList();
        }
        return items;
    }

    public static <T> PageView<T> empty(int current, int pageSize) {
        return PageView.of(Collections.emptyList(),current,pageSize,0);
    }

    public static <T> PageView<T> of(List<T> content, int current, int pageSize, long total) {

        PageView<T> pageView = new PageView<T>()
                .setItems(content)
                .setCurrentPage(current)
                .setPageSize(pageSize)
                .setTotalRecord(total);
        pageView.setTotalPage((int)(total/pageSize));
        if (total%pageSize>0){
            pageView.setTotalPage(pageView.getTotalPage()+1);
        }
        return pageView;
    }

    public <R> PageView<R> map(Function<? super T, ? extends R> function) {
        if (CollectionUtils.isEmpty(items)) {
            return PageView.empty(currentPage, pageSize);
        }
        return PageView.of(items.stream().map(function).collect(Collectors.toList()), currentPage, pageSize, totalRecord);
    }
}
