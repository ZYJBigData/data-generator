package com.bizseer.bigdata.es;

import com.bizseer.bigdata.clickhouse.InteractException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.core.TimeValue;

import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

public class esConnection {
    private static final String indexSuffix = "_biz_alias";

    public final static String IP_ADDRESS_SEPARATOR = ",";

    public final static String IP_AND_PORT_SEPARATOR = ":";

    public final static String PROTOCOL_TYPE_SCHEMA = "http";

    private static CredentialsProvider credentialsProvider = null;

    private static final ObjectMapper MAPPER = new ObjectMapper();


    public static void main(String[] args) throws InteractException, IOException {

//        connectInfo.setHost("10.0.60.144:9200");
//        connectInfo.setHost("10.0.100.244:9200");
//        ClusterHealthResponse health = getClient(getConnection()).cluster().health(new ClusterHealthRequest().timeout("20s"), RequestOptions.DEFAULT);
//        System.out.println("ping==" + health);
        scrollPage();
    }


    //纯java 版本
    // spring boot  ElasticsearchTemplate 
    public static void scrollPage() throws InteractException, IOException {
        RestHighLevelClient client = getClient(getConnection());
//        Scroll scroll = new Scroll(timeValueMinutes(5L));
        String esSearchAfter = "1663343854807";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder
                .sort(SortBuilders.fieldSort("start_time").order(SortOrder.DESC))
                .searchAfter(Arrays.stream(esSearchAfter.split(",")).filter(org.springframework.util.StringUtils::hasText).toArray())
                .from(0)
                .size(20);
        SearchRequest searchRequest = new SearchRequest("skywalking-index_segment-20220916");
        searchRequest.source(searchSourceBuilder);
//        searchRequest.scroll(scroll);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//        String scrollId = searchResponse.getScrollId();
//        System.out.println("scrollId==" + scrollId);
        SearchHit[] hits = searchResponse.getHits().getHits();
        String searchAfterValue;
        List<Pair<String, String>> fields = getFields("skywalking-index_segment-20220916", client);
        List<String> fileNames = new ArrayList<>();
        for (Pair<String, String> pair : fields) {
            fileNames.add(pair.getKey());
        }
        bianli(hits, fileNames);
        System.out.println("第一遍 --------------------------");
        while (hits.length > 0) {
            Object[] sortValues = hits[hits.length - 1].getSortValues();
            if (null != sortValues && sortValues.length > 0) {
                searchSourceBuilder.searchAfter(sortValues);
                SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
                hits = search.getHits().getHits();
                searchAfterValue = Arrays.stream(sortValues).map(String::valueOf).collect(Collectors.joining(","));
                System.out.println("searchAfterValue =   " + searchAfterValue);
                bianli(hits, fileNames);
                System.out.println("中间--------------------------");
            }
        }


//            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
//            searchScrollRequest.scroll(scroll);
//            SearchResponse searchScrollResponse = client.scroll(searchScrollRequest, customRequestOptions());
//            scrollId = searchScrollResponse.getScrollId();
//            System.out.println("scrollId==" + scrollId);
//            hits = searchScrollResponse.getHits().getHits();
//        }

//        //及时清除es快照，释放资源
//        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
//        clearScrollRequest.addScrollId(scrollId);
//        client.clearScroll(clearScrollRequest, customRequestOptions());

        client.close();
    }

    public static void bianli(SearchHit[] hits, List<String> fieldNames) {
        for (SearchHit hit : hits) {
            Map<String, Object> sourceMap = hit.getSourceAsMap();
            List<Object> items = new ArrayList<>(fieldNames.size());
            for (int i = 0; i <= fieldNames.size() - 1; i++) {
                String fieldName = fieldNames.get(i);
                items.add(sourceMap.get(fieldName));
            }
            System.out.println("hit==" + items);
        }

    }

    public static EsConnectInfo getConnection() {
        EsConnectInfo connectInfo = new EsConnectInfo();
        connectInfo.setIndex("index_two_biz_alias");
        connectInfo.setUsername("elastic");
        connectInfo.setPassword("123456");
        connectInfo.setPeriodicUnit(EsPeriodicUnit.DAY);
        connectInfo.setReplica(3);
        connectInfo.setShard(3);
        connectInfo.setHost("10.0.60.144:9200,10.0.60.145:9200,10.0.60.146:9200");
        return connectInfo;
    }


    public static LinkedHashMap<String, Object> toMap(String jsonStr) {
        if (StringUtils.isBlank(jsonStr)) {
            return new LinkedHashMap(0);
        } else {
            try {
                return (LinkedHashMap) MAPPER.readValue(jsonStr, LinkedHashMap.class);
            } catch (IOException var2) {
                return new LinkedHashMap(0);
            }
        }
    }

    public static String getIndexAlias(String originIndexName) {
        return originIndexName + indexSuffix;
    }


    public static RestHighLevelClient getClient(EsConnectInfo connectInfo) throws InteractException {
        if (StringUtils.isBlank(connectInfo.getHost())) {
            throw new InteractException(String.format("wrong connect info : host=%s", connectInfo.getHost())).setConnectInfo(connectInfo);
        }
        HttpHost[] httpHosts = Arrays.stream(connectInfo.getHost().split(IP_ADDRESS_SEPARATOR)).map(host -> {
            String[] s = host.split(IP_AND_PORT_SEPARATOR);
            return new HttpHost(s[0], Integer.parseInt(s[1]), PROTOCOL_TYPE_SCHEMA);
        }).toArray(HttpHost[]::new);
        RestClientBuilder builder = RestClient.builder(httpHosts);

        if (!StringUtils.isEmpty(connectInfo.getUsername())) {
            if (credentialsProvider == null) {
                credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(connectInfo.getUsername(), connectInfo.getPassword()));
            }
            builder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        return new RestHighLevelClient(builder);
    }

    public static RequestOptions customRequestOptions() {
        return RequestOptions.DEFAULT.toBuilder()
                .build();
    }

    public static List<Pair<String, String>> getFields(String esIndexName, RestHighLevelClient client) throws IOException {
        GetIndexRequest request = new GetIndexRequest(esIndexName);
        GetIndexResponse getIndexResponse = client.indices().get(request, customRequestOptions());
        Map<String, MappingMetadata> mappings = getIndexResponse.getMappings();
        String key = mappings.keySet().stream().findFirst().get();
        MappingMetadata metadata = mappings.get(key);
        Map<String, Object> sourceAsMap = metadata.getSourceAsMap();
        if (null == sourceAsMap || sourceAsMap.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, Object> properties = (Map<String, Object>) sourceAsMap.get("properties");
        if (null == properties || properties.isEmpty()) {
            return Collections.emptyList();
        }

        List<Pair<String, String>> fields = new ArrayList<>();
        properties.forEach((fieldName, propMap) -> {
            String type = ((LinkedHashMap<String, String>) propMap).get("type");
            fields.add(Pair.of(fieldName, type));
        });

        return fields;
    }
}

