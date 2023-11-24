package com.bizseer.bigdata.es;

import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class EsWriter {
    public static void main(String[] args) {
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        b.add("a");
        b.add("b");
        a.addAll(b);
        System.out.println(a.size());
        System.out.println(CollectionUtils.isEmpty(a));
    }
    
}
