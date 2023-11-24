package com.bizseer.bigdata.es;

import com.bizseer.bigdata.clickhouse.InteractException;
import com.bizseer.bigdata.es.utils.ElasticsearchClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.*;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class esConnection {
    private static final String indexSuffix = "_biz_alias";

    public final static String IP_ADDRESS_SEPARATOR = ",";

    public final static String IP_AND_PORT_SEPARATOR = ":";

    public final static String PROTOCOL_TYPE_SCHEMA = "http";

    private static CredentialsProvider credentialsProvider = null;

    private static final ObjectMapper MAPPER = new ObjectMapper();


    public static void main(String[] args) throws InteractException, IOException {
        //connectInfo.setHost("10.0.60.144:9200");
//        connectInfo.setHost("10.0.100.244:9200");
//        ClusterHealthResponse health = getClient(getConnection()).cluster().health(new ClusterHealthRequest().timeout("20s"), RequestOptions.DEFAULT);
//        System.out.println("ping==" + health);
//        ESQueryParameter.exist("simpleLog");
//        elasticsearchTemplate.search(indexName,simpleLog);
        String indexName = "sales";

        String node1 = "10.0.100.16:9200";
        String metricsIndex = "dataplat-dev-metrics-series";
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(node1);
        ElasticsearchTemplate elasticsearchTemplate = new ElasticsearchTemplate(elasticsearchClient, Arrays.asList(""));
        RestHighLevelClient restHighLevelClient = elasticsearchClient.getRestHighLevelClient();
        SearchResponse search = restHighLevelClient.search(composite(metricsIndex), RequestOptions.DEFAULT);
        ElasticsearchTemplate.SearchResult searchResult = elasticsearchTemplate.buildSearchResult(search);
        System.out.println("searchResult==「」"+searchResult);
        shuchu(searchResult, true);
    }

    public static SearchRequest composite(String indexName) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        TermsValuesSourceBuilder metricIdTerms = new TermsValuesSourceBuilder("metricId").field("metricId").order("desc");
        sources.add(metricIdTerms);
        CompositeAggregationBuilder builder = AggregationBuilders.composite("composite_agg", sources).size(10);
        builder.aggregateAfter(ImmutableMap.of("metricId", "36"));
        searchSourceBuilder.aggregation(builder);

        SearchRequest searchRequest = new SearchRequest(indexName);
        return searchRequest.source(searchSourceBuilder);
    }

    public static QueryBuilder matchAll() {
        return new MatchAllQueryBuilder();
    }

    public static QueryBuilder matchEach() {
        return new MatchQueryBuilder("hobby", "music and movie");
    }

    public static QueryBuilder matchPhrase() {
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = new MatchPhraseQueryBuilder("hobby", "music and movie");
        return matchPhraseQueryBuilder;
    }

    public static QueryBuilder matchPhrasePrefix() {
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = new MatchPhrasePrefixQueryBuilder("hobby", "movie");
        return matchPhrasePrefixQueryBuilder;
    }

    public static QueryBuilder wildcard() {
        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder("measurement", "*c*");
        return wildcardQueryBuilder;
    }

    public static QueryBuilder prefix() {
        PrefixQueryBuilder prefixQueryBuilder = new PrefixQueryBuilder("measurement", "LINUX");
        return prefixQueryBuilder;
    }

    public static QueryBuilder range() {
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("metricId").from(1).to(2);
        return rangeQueryBuilder;
    }

    public static SearchRequest cardinality(String realIndex) {
//        CardinalityAggregationBuilder field = AggregationBuilders.cardinality(realIndex).field("tags.ip");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        CardinalityAggregationBuilder cardinalityAggregationBuilder = new CardinalityAggregationBuilder("unique_count").field("tags.ip");
        CollapseBuilder collapseBuilder = new CollapseBuilder("tags.ip");
        SearchRequest searchRequest = new SearchRequest(realIndex);
        searchSourceBuilder.collapse(collapseBuilder);
        searchSourceBuilder.aggregation(cardinalityAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    public static SearchRequest collapse(String realIndex) {
        CollapseBuilder collapseBuilder = new CollapseBuilder("tags.ip");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(realIndex);
        searchSourceBuilder.collapse(collapseBuilder);
        return searchRequest.source(searchSourceBuilder);
    }

    public static QueryBuilder terms() {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("metricCode", Arrays.asList("test_05", "njc_test_01"));
        return termsQueryBuilder;
    }

    public static QueryBuilder term() {
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("hobby", "music");
        return termQueryBuilder;
    }

    public static QueryBuilder exist() {
        ExistsQueryBuilder simpleLog = new ExistsQueryBuilder("simpleLog");
        return simpleLog;
    }

    public static SearchRequest query(QueryBuilder queryBuilder, String indexSuffix) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(indexSuffix);
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    public static void shuchu(ElasticsearchTemplate.SearchResult searchResult, boolean aggregations) {
//        if (aggregations) {
//            ParsedCardinality parsedCardinality = (ParsedCardinality) searchResult.getAggregations().asList().get(0);
//            System.out.println("value====" + parsedCardinality.value());
//            System.out.println("getValue====" + parsedCardinality.getValue());
//            System.out.println("getType====" + parsedCardinality.getType());
//            System.out.println("getValueAsString====" + parsedCardinality.getValueAsString());
//            System.out.println("getMetadata====" + parsedCardinality.getMetadata());
//            System.out.println("getName====" + parsedCardinality.getName());
//        }
        if (aggregations) {
            ParsedComposite composite = (ParsedComposite) searchResult.getAggregations().asList().get(0);
            if (Objects.nonNull(composite.afterKey())) {
                System.out.println("afterKey===" + composite.afterKey());
                System.out.println("afterKey===" + composite.afterKey().get("metricId"));
            }
            for (ParsedComposite.ParsedBucket parsedBucket : composite.getBuckets()) {
                int lastId = (Integer) parsedBucket.getKey().get("metricId");
                System.out.println("lastId==" + lastId);
                String keyAsString = parsedBucket.getKeyAsString();
                System.out.println("keyAsString===" + keyAsString);
            }
        }
        System.out.println("scrollId===" + searchResult.getScrollId());
        System.out.println("total==" + searchResult.getTotalHits());
        System.out.println("success=" + searchResult.isSuccess());
        for (ElasticsearchTemplate.GetResult getResult : searchResult.getResultList()) {
            System.out.println("id===" + getResult.getId());
            System.out.println("sourceAsString===" + getResult.getSourceAsString());
            System.out.println("index===" + getResult.getIndex());
            for (int i = 0; i < getResult.getSortValues().length; i++) {
                System.out.println("index===" + getResult.getSortValues()[i]);
            }
            for (Map.Entry<String, Object> entry : getResult.getSource().entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
        }
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

