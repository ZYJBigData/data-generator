package com.bizseer.bigdata.es;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import com.bizseer.bigdata.es.utils.*;
import com.bizseer.bigdata.es.utils.ElasticsearchClient;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoTileGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.ParsedGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.*;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.ParsedMissing;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.*;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.elasticsearch.search.aggregations.bucket.terms.*;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.pipeline.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.xcontent.*;
import org.elasticsearch.xcontent.json.JsonXContentParser;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author CharleyXu
 * @date 2020-11-24
 * <p>
 * ES客户端
 * <p>
 * 7.0及以上，使用restHighLevelClient
 * <p>
 * 7.0以下，使用restLowLevelClient
 */
@Slf4j
@SuppressWarnings({"WeakerAccess", "unused", "deprecation"})
public class ElasticsearchTemplate implements Closeable {

    private final RestHighLevelClient restHighLevelClient;

    private static final String ID = "_id";

    private static final String DEFAULT = "default_";

    private final List<String> initIndices;

    private boolean belowVersion7;

    private final String prefix;

    private final int indexNumberOfShards;

    private final int indexNumberOfReplicas;

    private final NamedXContentRegistry registry = new NamedXContentRegistry(getDefaultNamedXContents());

    public ElasticsearchTemplate(ElasticsearchClient elasticsearchClient, List<String> initIndices) {
        this.restHighLevelClient = elasticsearchClient.getRestHighLevelClient();
        this.prefix = elasticsearchClient.getPrefix();
        this.indexNumberOfShards = elasticsearchClient.getIndexNumberOfShards();
        this.indexNumberOfReplicas = elasticsearchClient.getIndexNumberOfReplicas();
        this.initIndices = initIndices;
        validateVersion();
    }

    public void init() {
        if (!existIndexTemplate()) {
            createIndexTemplate();
            log.info("成功创建默认index模版");
        }
        if (!CollectionUtils.isEmpty(initIndices)) {
            initIndices.forEach(index -> {
                if (!existIndex(index)) {
                    createIndex(index);
                }
            });
        }
    }

    public SearchResult search(String index, @NonNull ESQueryParameter parameter) {
        return search(index, parameter, PageParam.of(), Collections.emptyList());
    }

    public SearchResult search(String index, @NonNull ESQueryParameter parameter, @NonNull PageParam pageRequest) {
        return search(index, parameter, pageRequest, Collections.emptyList());
    }

    public long count(String index, @NonNull ESQueryParameter parameter) {
        return countAction(new CountRequest(prefix + index).query(parameter.getBuilder()));
    }

    public SearchResult search(String[] indices, @NonNull ESQueryParameter parameter, @NonNull PageParam pageRequest, List<AggregationBuilder> aggregations, String distinctFieldName) {
        SearchRequest request = new SearchRequest(indices).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED);
        if (Objects.nonNull(parameter.getTimeValue())) {
            request.scroll(parameter.getTimeValue());
        }
        SearchSourceBuilder builder = searchSourceBuilder(parameter, pageRequest, aggregations, distinctFieldName);
        log.debug("index: {}, result: {}", indices, builder.toString());
        return searchAction(request.source(builder));
    }

    public SearchResult search(String index, @NonNull ESQueryParameter parameter, @NonNull PageParam pageRequest, List<AggregationBuilder> aggregations) {
        return search(new String[]{prefix + index}, parameter, pageRequest, aggregations, null);
    }

    public SearchResult search(String index, @NonNull ESQueryParameter parameter, @NonNull PageParam pageRequest, List<AggregationBuilder> aggregations, String distinctFieldName) {
        return search(new String[]{prefix + index}, parameter, pageRequest, aggregations, distinctFieldName);
    }

    public <T> T searchDoc(String index, String id, Class<T> clazz) {
        return searchDoc(index, ESQueryParameter.query().must(ESQueryParameter.term(ID, id)), clazz);
    }

    public <T> T searchDoc(String index, ESQueryParameter esQueryParameter, Class<T> clazz) {
        try {
            List<GetResult> resultList = search(index, esQueryParameter).getResultList();
            if (Objects.nonNull(resultList) && !resultList.isEmpty()) {
                return JsonUtil.toObject(resultList.get(resultList.size() - 1).getSourceAsString(), clazz);
            }
        } catch (Exception e) {
            log.error("result index:{} search by id occurs error, e:{}", index, e.getMessage(), e);
        }
        return null;
    }

    public <T> List<T> findAllDoc(String index, ESQueryParameter parameter, Class<T> clazz) {
        return pageSearchDoc(index, parameter, PageParam.of(1, 10000), clazz).getItems();
    }

    public <T> List<T> findAllDoc(String index, List<String> ids, Class<T> clazz) {
        return findAllDoc(index, ESQueryParameter.query().must(ESQueryParameter.terms(ID, ids)), clazz);
    }

    public <T> PageView<T> pageSearchDoc(String index, ESQueryParameter parameter, PageParam pageRequest, Class<T> clazz) {
        SearchResult searchResult = search(index, parameter, pageRequest);
        if (!searchResult.isSuccess()) {
            return PageView.of(Collections.emptyList(), pageRequest.getCurrentPage(), pageRequest.getPageSize(), 0);
        }
        return PageView.of(searchResult.getResultList().stream().map(result -> JsonUtil.toObject(result.getSourceAsString(), clazz)).collect(Collectors.toList()),
                pageRequest.getCurrentPage(), pageRequest.getPageSize(), searchResult.getTotalHits());
    }

    public PageView<String> pageSearchDocId(String index, ESQueryParameter parameter, PageParam pageRequest) {
        parameter.includes(ID);
        SearchResult searchResult = search(index, parameter, pageRequest);
        if (!searchResult.isSuccess()) {
            return PageView.of(Collections.emptyList(), pageRequest.getCurrentPage(), pageRequest.getPageSize(), 0);
        }
        return PageView.of(searchResult.getResultList().stream().map(GetResult::getId).collect(Collectors.toList()),
                pageRequest.getCurrentPage(), pageRequest.getPageSize(), searchResult.getTotalHits());
    }

    @SuppressWarnings("unchecked")
    public <T> SearchAfterResult<T> searchAfter(String index, ESQueryParameter parameter, int pageSize, Class<T> clazz) {
        SearchResult searchResult = searchAfter(index, parameter, pageSize);
        List<GetResult> resultList = searchResult.getResultList();
        if (Objects.nonNull(resultList) && !resultList.isEmpty()) {
            List<T> collect = resultList.stream().map(result -> JsonUtil.toObject(result.getSourceAsString(), clazz)).collect(Collectors.toList());
            return new SearchAfterResult(collect, resultList.get(resultList.size() - 1).getSortValues());
        } else {
            return new SearchAfterResult<>();
        }
    }

    public SearchResult searchAfter(String index, @NonNull ESQueryParameter parameter, int pageSize) {
        SearchRequest request = new SearchRequest(prefix + index).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder builder = new SearchSourceBuilder().query(parameter.getBuilder());
        if (Objects.nonNull(parameter.getSortValues()) && parameter.getSortValues().length != 0) {
            builder.searchAfter(parameter.getSortValues());
        }
        for (String field : parameter.getDescList()) {
            builder.sort(SortBuilders.fieldSort(field).order(SortOrder.DESC));
        }
        for (String field : parameter.getAscList()) {
            builder.sort(SortBuilders.fieldSort(field).order(SortOrder.ASC));
        }
        builder.size(pageSize);
        log.debug("index: {}, dsl:{}", index, builder.toString());
        return searchAction(request.source(builder));
    }

    private long countAction(CountRequest request) {
        try {
            CountResponse countResponse;
            if (belowVersion7) {
                countResponse = count(request);
            } else {
                countResponse = restHighLevelClient.count(request, RequestOptions.DEFAULT);
            }
            if (countResponse != null) {
                return countResponse.getCount();
            }
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec count api error,message: {}", e.getMessage(), e);
        }
        return 0;
    }

    public SearchResult searchAction(SearchRequest request) {
        try {
            SearchResponse response;
            if (belowVersion7) {
                response = search(request);
            } else {
                response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            }
            return buildSearchResult(response);
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec search api error,message: {}", e.getMessage(), e);
        }
        return new SearchResult();
    }

    private SearchResponse search(SearchRequest searchRequest) throws IOException {
        Response response = restHighLevelClient.getLowLevelClient().performRequest(
                ElasticsearchConverters.search(searchRequest));
        String responseJson = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
        return fromXContent(responseJson);
    }

    private CountResponse count(CountRequest countRequest) throws IOException {
        HttpEntity entity = restHighLevelClient.getLowLevelClient().performRequest(ElasticsearchConverters.count(countRequest)).getEntity();
        try (InputStream content = entity.getContent()) {
            String response = IOUtils.toString(content, StandardCharsets.UTF_8);
            JsonXContentParser parser = new JsonXContentParser(registry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new JsonFactory().createParser(response));
            return CountResponse.fromXContent(parser);
        } catch (IOException e) {
            log.error("[ElasticsearchTemplate] exec count response analyze api error,message: {}", e.getMessage(), e);
            return null;
        }
    }

    public SearchResponse fromXContent(String response) throws IOException {
        JsonXContentParser parser = new JsonXContentParser(registry,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new JsonFactory().createParser(response));
        return SearchResponse.fromXContent(parser);
    }

    public SearchSourceBuilder searchSourceBuilder(@NonNull ESQueryParameter parameter, @NonNull PageParam pageRequest, List<AggregationBuilder> aggregations, String distinctFieldName) {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(parameter.getBuilder())
                .fetchSource(parameter.getIncludes(), parameter.getExcludes())
                .from(pageRequest.getSkip()).size(pageRequest.getPageSize());
        for (String field : parameter.getDescList()) {
            builder.sort(SortBuilders.fieldSort(field).order(SortOrder.DESC));
        }
        for (String field : parameter.getAscList()) {
            builder.sort(SortBuilders.fieldSort(field).order(SortOrder.ASC));
        }
        if (aggregations != null && !aggregations.isEmpty()) {
            for (AggregationBuilder agg : aggregations) {
                builder.aggregation(agg);
            }
        }
        if (!StringUtils.isEmpty(distinctFieldName)) {
            builder.collapse(new CollapseBuilder(distinctFieldName));
        }

//        System.out.println(builder.toString());
        return builder;
    }

    /**
     * 滚动search
     */
    public SearchResult searchScroll(String scrollId, TimeValue timeValue) {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(timeValue);
        try {
            return buildSearchResult(restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT));
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec search scroll api error,message: {}", e.getMessage(), e);
        }
        return new SearchResult();
    }

    /**
     * 批量search
     */
    public List<SearchResult> searchBatch(String index, List<TermQueryBuilder> paramList) {
        MultiSearchRequest request = new MultiSearchRequest();
        paramList.forEach(item -> {
            SearchRequest searchRequest = new SearchRequest(prefix + index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(item);
            searchRequest.source(searchSourceBuilder);
            log.debug("index: {}, search: {}", index, searchSourceBuilder.toString());
            request.add(searchRequest);
        });
        return searchBatch(request);
    }

    private List<SearchResult> searchBatch(MultiSearchRequest request) {
        MultiSearchResponse response = null;
        try {
            if (belowVersion7) {
                response = multiSearch(request);
            } else {
                response = restHighLevelClient.msearch(request, RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            log.error("[ElasticsearchTemplate] exec searchBatch api error,message: {}", e.getMessage(), e);
        }
        if (response != null) {
            return Arrays.stream(response.getResponses()).map(
                    item -> buildSearchResult(item.getResponse())).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private MultiSearchResponse multiSearch(MultiSearchRequest multiSearchRequest) throws IOException {
        Response response = restHighLevelClient.getLowLevelClient().performRequest(
                ElasticsearchConverters.multiSearch(multiSearchRequest)
        );
        String responseJson = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
        Map<String, Object> objectMap = JsonUtil.toMap(responseJson);
        objectMap.put("took", 1);
        JsonXContentParser parser = new JsonXContentParser(registry,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new JsonFactory().createParser(JsonUtil.toJsonString(objectMap)));
        return MultiSearchResponse.fromXContext(parser);
    }

    public DeleteResult deleteDoc(String index, String id) {
        return deleteAction(new DeleteRequest(prefix + index).id(id).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
    }

    public void batchDeleteDocs(String index, List<String> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (String id : ids) {
            bulkRequest.add(new DeleteRequest().index(prefix + index).id(id)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        bulkApi(bulkRequest);
    }

    public long deleteByQuery(String index, @NonNull ESQueryParameter parameter) {
        try {
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(prefix + index).setRefresh(true);
            deleteByQueryRequest.setQuery(parameter.getBuilder());
            return restHighLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT).getDeleted();
        } catch (IOException | ElasticsearchException e) {
            log.error("Fail to deleteByQuery,e:{}", e.getMessage(), e);
            return -1;
        }
    }

    /**
     * Clears one or more scroll ids
     */
    public void clearScroll(@NonNull List<String> scrollIds) {
        ClearScrollRequest scrollRequest = new ClearScrollRequest();
        try {
            scrollRequest.setScrollIds(scrollIds);
            restHighLevelClient.clearScroll(scrollRequest, RequestOptions.DEFAULT);
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec clear scroll api error,message: {}", e.getMessage(), e);
        }
    }

    public CreateResult createDoc(String index, String id, String content) {
        return indexAction(new IndexRequest(prefix + index).id(id).source(content, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
    }

    public boolean updateDoc(String index, String id, String content) {
        UpdateRequest updateRequest = new UpdateRequest(prefix + index, id).docAsUpsert(true).doc(content, XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).retryOnConflict(3);
        if (belowVersion7) {
            return updateDocLowLevelAction(updateRequest);
        } else {
            return updateDocAction(updateRequest);
        }
    }

    /**
     * 基于ID的批量更新
     *
     * @param index    索引名称
     * @param params   pair.left : id , pair.right : 要修改字段以及数据
     * @param asUpsert 是否支持 Upsert 操作
     */
    public void batchUpdateDocs(String index, List<Pair<String, Map<String, Object>>> params, boolean asUpsert) {
        if (CollectionUtils.isEmpty(params)) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Pair<String, Map<String, Object>> pair : params) {
            bulkRequest.add(new UpdateRequest().index(prefix + index).id(pair.getLeft()).doc(pair.getRight()).docAsUpsert(asUpsert)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        bulkApi(bulkRequest);
    }

    public long updateByQueryDoc(String index, ESQueryParameter queryParameter, String script) {
        if (belowVersion7) {
            updateByQueryDocAction(prefix + index, queryParameter, script);
            return -1;
        } else {
            return updateByQueryDocAction(new UpdateByQueryRequest(prefix + index).setRefresh(true).setMaxRetries(3)
                    .setQuery(queryParameter.getBuilder()).setScript(new Script(script)));
        }
    }

    public long updateByQueryDoc(String index, ESQueryParameter queryParameter, String script, Map<String, Object> params) {
        if (belowVersion7) {
            updateByQueryDocAction(prefix + index, queryParameter, script, params);
            return -1;
        } else {
            return updateByQueryDocAction(new UpdateByQueryRequest(prefix + index).setRefresh(true).setMaxRetries(3)
                    .setQuery(queryParameter.getBuilder()).setScript(new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", script, params)));
        }
    }

    public <T> T getDoc(String index, String id, Class<T> clazz) {
        GetResult getResult = getDoc(index, id);
        if (StrUtil.isBlank(getResult.getSourceAsString())) {
            log.warn("查询数据不存在! index[{}], id[{}]", index, id);
            return null;
        }
        return JsonUtil.toObject(getResult.getSourceAsString(), clazz);
    }

    public GetResult getDoc(String index, String id, String[] includes, String[] excludes) {
        return getAction(new GetRequest(prefix + index).id(id).fetchSourceContext(new FetchSourceContext(true, includes, excludes)));
    }

    public GetResult getDoc(String index, String id) {
        return getAction(new GetRequest(prefix + index).id(id));
    }

    public boolean existsDoc(String index, String id) {
        try {
            return restHighLevelClient.exists(new GetRequest(prefix + index).id(id), RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * 批量写入document
     */
    public void createDocs(String index, List<? extends DocumentId> documents) {
        if (Objects.isNull(documents) || documents.isEmpty()) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        documents.forEach(entry -> bulkRequest.add(new IndexRequest(prefix + index).id(entry.getId())
                .source(JsonUtil.toJsonString(entry), XContentType.JSON)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
        bulkApi(bulkRequest);
    }

    /**
     * 批量写入document，不指定ID
     */
    public void batchCreateAndRefreshWithoutId(String index, List<Object> documents) {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        documents.forEach(item -> bulkRequest.add(new IndexRequest(prefix + index).source(
                JsonUtil.toJsonString(item), XContentType.JSON)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
        bulkApi(bulkRequest);
    }

    public void bulkApi(BulkRequest bulkRequest) {
        try {
            if (belowVersion7) {
                bulkAction(bulkRequest);
            } else {
                restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            log.error("[ElasticsearchTemplate] exec bulk api occurs error,message: {}", e.getMessage(), e);
        }
    }

    public void bulkAction(BulkRequest bulkRequest) throws IOException {
        restHighLevelClient.getLowLevelClient().performRequest(ElasticsearchConverters.bulk(bulkRequest));
    }

    public SearchResult buildSearchResult(SearchResponse response) {
        log.debug("TookMillis::" + response.getTook().getMillis());
        SearchResult result = new SearchResult(Arrays.stream(response.getHits().getHits())
                .map(searchHit -> new GetResult(searchHit.getIndex(), searchHit.getId(), searchHit.getSourceAsMap(), searchHit.getSourceAsString(), searchHit.getSortValues()))
                .collect(Collectors.toList()));
        result.setTotalHits(response.getHits().getTotalHits().value);
        result.setScrollId(response.getScrollId());
        result.setAggregations(response.getAggregations());
        result.setSuccess(true);
        return result;
    }

    private GetResult getAction(GetRequest request) {
        try {
            GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            if (response.isExists()) {
                return new GetResult(response.getIndex(), response.getId(), response.getSource(), response.getSourceAsString());
            }
            return new GetResult();
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec get api error,message: {}", e.getMessage(), e);
        }
        return new GetResult();
    }

    private DeleteResult deleteAction(DeleteRequest request) {
        try {
            DeleteResponse response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
            return new DeleteResult(response.getIndex(), response.getId());
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec delete api error,message: {}", e.getMessage(), e);
        }
        return new DeleteResult();
    }

    private CreateResult indexAction(IndexRequest request) {
        try {
            IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED || response.getResult() == DocWriteResponse.Result.UPDATED) {
                return new CreateResult(response.getIndex(), response.getId(), response.getSeqNo(), response.getVersion());
            }
            return new CreateResult();
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec create api error,message: {}", e.getMessage(), e);
        }
        return new CreateResult();
    }

    private boolean updateDocAction(UpdateRequest request) {
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            RestStatus status = updateResponse.status();
            return status == RestStatus.OK || status == RestStatus.CREATED || status == RestStatus.ACCEPTED;
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec update api occurs error,message: {}", e.getMessage(), e);
        }
        return true;
    }

    private boolean updateDocLowLevelAction(UpdateRequest updateRequest) {
        try {
            restHighLevelClient.getLowLevelClient().performRequest(ElasticsearchConverters.update(updateRequest));
            return true;
        } catch (IOException e) {
            log.error("[ElasticsearchTemplate] restLowLevelClient exec update api occurs error,message: {}", e.getMessage(), e);
        }
        return false;
    }

    public long updateByQueryDocAction(UpdateByQueryRequest updateByQueryRequest) {
        try {
            log.debug(updateByQueryRequest.toString());
            BulkByScrollResponse bulkByScrollResponse = restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
            return bulkByScrollResponse.getUpdated();
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec updateByQuery api occurs error,message: {}", e.getMessage(), e);
            return 0L;
        }
    }

    private void updateByQueryDocAction(String index, ESQueryParameter esQueryParameter, String script) {
        updateByQueryDocAction(prefix + index, esQueryParameter, script, null);
    }

    private void updateByQueryDocAction(String index, ESQueryParameter esQueryParameter, String script, Map<String, Object> params) {
        Request performRequest = new Request(HttpPost.METHOD_NAME, "/" + index + "/_doc/_update_by_query?conflicts=proceed&refresh=true&timeout=1m");
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(esQueryParameter.getBuilder());
        Map<String, Object> jsonMap = JsonUtil.toMap(builder.toString());
        if (CollectionUtils.isEmpty(params)) {
            jsonMap.put("script", ImmutableMap.of("source", script, "lang", "painless"));
        } else {
            jsonMap.put("script", ImmutableMap.of("source", script, "lang", "painless", "params", params));
        }
        String json = JsonUtil.toJsonString(jsonMap);
        log.debug("exec low level updateByQuery api, request body: {}", json);
        performRequest.setJsonEntity(json);
        try {
            restHighLevelClient.getLowLevelClient().performRequest(performRequest);
        } catch (IOException e) {
            log.error("[ElasticsearchTemplate] restLowLevelClient exec update api occurs error,message: {}", e.getMessage(), e);
        }
    }

    /**
     * set the alias point to a write-index
     */
    public void createIndexWithAlias(String index) {
        try {
            restHighLevelClient.indices().create(new CreateIndexRequest(prefix + DEFAULT + index).alias(new Alias(prefix + index).writeIndex(true)), RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("[ElasticsearchTemplate] create index and alias api, message: {}", e.getMessage(), e);
        }
    }

    public void createIndex(String index) {
        try {
            restHighLevelClient.indices().create(new CreateIndexRequest(prefix + index), RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("[ElasticsearchTemplate] create index api, message: {}", e.getMessage(), e);
        }
    }

    public GetIndexResponse getIndexMapping(String index) {
        try {
            String indexName = prefix + index;
            GetIndexResponse indexResponse = restHighLevelClient.indices().get(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (indexResponse != null) {
                return indexResponse;
            }
        } catch (Exception e) {
            log.error("[ElasticsearchTemplate]-get indexes[{}], failed message: {}", index, e.getMessage(), e);
        }
        return null;
    }

    public String[] getIndexes(String prefix) {
        try {
            return restHighLevelClient.indices().get(new GetIndexRequest(prefix + "*"), RequestOptions.DEFAULT).getIndices();
        } catch (Exception e) {
            log.error("[ElasticsearchTemplate]-get all indexes, failed message: {}", e.getMessage(), e);
        }
        return new String[0];
    }

    public void deleteIndex(String... indexes) {
        if (indexes == null || indexes.length == 0) {
            return;
        }
        DeleteIndexRequest request = new DeleteIndexRequest(Arrays.stream(indexes).map(index -> prefix + index).toArray(size -> new String[size]));
        try {
            boolean acknowledged = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT).isAcknowledged();
            log.info("delete index: {}, acknowledged: {}", indexes, acknowledged);
        } catch (IOException e) {
            log.error("[ElasticsearchTemplate]-Index delete error, {}", e.getMessage(), e);
        } catch (ElasticsearchException e) {
            log.error("[ElasticsearchTemplate]-Failed to delete index, error message: {}", e.getDetailedMessage(), e);
        }
    }

    public boolean existIndex(String index) {
        if (belowVersion7) {
            return existIndexAction(prefix + index);
        } else {
            try {
                return restHighLevelClient.indices().exists(new GetIndexRequest(prefix + index), RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("Fail to exist index, error message : {}", e.getMessage(), e);
            }
            return false;
        }
    }

    private boolean existIndexAction(String indices) {
        Request performRequest = new Request(HttpHead.METHOD_NAME, "/" + indices);
        performRequest.addParameter(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "true");
        try {
            Response response = restHighLevelClient.getLowLevelClient().performRequest(performRequest);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (Exception e) {
            log.error("[ElasticsearchTemplate] restLowLevelClient exec index exist api error,message: {}", e.getMessage(), e);
        }
        return false;
    }

    /**
     * 创建index别名，默认指定为写索引
     */
    public void updateIndexAlias(String index, String alias, boolean isWriteIndex) {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.ADD).index(index).alias(alias).writeIndex(isWriteIndex));
        try {
            restHighLevelClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("[ElasticsearchTemplate] restHighLevelClient exec updateAliases api, message: {}", e.getMessage(), e);
        }
    }

    /**
     * 获取别名
     */
    public Map<String, Set<AliasMetadata>> getIndexAlias(String alias) {
        try {
            GetAliasesResponse aliasesResponse = restHighLevelClient.indices().getAlias(new GetAliasesRequest(alias), RequestOptions.DEFAULT);
            return aliasesResponse.getAliases();
        } catch (IOException e) {
            log.error("[ElasticsearchTemplate] restHighLevelClient exec getIndexAlias api occurs error,message: {}", e.getMessage(), e);
        }
        return Collections.emptyMap();
    }

    /**
     * 是否存在别名
     */
    public boolean existAlias(String alias) {
        try {
            return restHighLevelClient.indices().existsAlias(new GetAliasesRequest(prefix + alias), RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    public long countAll(String index) {
        try {
            CountResponse countResponse = restHighLevelClient.count(new CountRequest(prefix + index), RequestOptions.DEFAULT);
            return countResponse.getCount();
        } catch (IOException e) {
            log.error("Fail to count index, error message : {}", e.getMessage(), e);
            return 0L;
        }
    }

    public long countAll(String index, ESQueryParameter parameter) {
        try {
            CountRequest countRequest = new CountRequest(prefix + index);
            countRequest.query(parameter.getBuilder());
            CountResponse countResponse = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);
            return countResponse.getCount();
        } catch (IOException e) {
            log.error("Fail to count index, error message : {}", e.getMessage(), e);
            return 0L;
        }
    }

    @SuppressWarnings("unchecked")
    public void createIndexTemplate() {
        try {
            String templateJson = IOUtils.toString(ResourceUtil.getStream("es-template.json"), StandardCharsets.UTF_8);
            Map<String, Object> templateMap = JsonUtil.toMap(templateJson);
            if (belowVersion7) {
                Map<String, Serializable> immutableMap = ImmutableMap.of("order", Integer.MAX_VALUE, "index_patterns", prefix + "*",
                        "mappings", ImmutableMap.of("_doc", templateMap),
                        "settings", ImmutableMap.of("index", ImmutableMap.of("number_of_shards", indexNumberOfShards,
                                "number_of_replicas", indexNumberOfReplicas, "analysis", getAnalysisNormalizer())));
                Request request = new Request(HttpPut.METHOD_NAME, "/_template/" + prefix);
                request.setJsonEntity(JsonUtil.toJsonString(immutableMap));
                request.addParameter(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "true");
                restHighLevelClient.getLowLevelClient().performRequest(request);
            } else {
                putIndexTemplate(templateMap);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

    }

    private Map<String, Object> getAnalysisNormalizer() {
        Map<String, Object> map = new HashMap<>();
        map.put("normalizer", ImmutableMap.of("lowercase_normalizer", ImmutableMap.of("filter", new String[]{"lowercase"}, "type", "custom", "char_filter", new String[]{})));
        return map;
    }

    private void putIndexTemplate(Map<String, Object> source) {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(prefix).order(Integer.MAX_VALUE);
        request.patterns(Lists.newArrayList(prefix + "*"));
        request.mapping(source);
        request.settings(ImmutableMap.of("number_of_shards", indexNumberOfShards,
                "number_of_replicas", indexNumberOfReplicas, "analysis", getAnalysisNormalizer()));
        try {
            AcknowledgedResponse response = restHighLevelClient.indices().putTemplate(request, RequestOptions.DEFAULT);
            log.info("[ElasticsearchTemplate] put index template，status: {}", response.isAcknowledged());
        } catch (IOException | ElasticsearchException e) {
            log.error("[ElasticsearchTemplate] exec put index template api error,message: {}", e.getMessage(), e);
        }
    }

    public boolean existIndexTemplate() {
        try {
            return restHighLevelClient.indices().existsTemplate(
                    new IndexTemplatesExistRequest(prefix), RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    private void validateVersion() {
        String version = version();
        log.info("current es version: {}", version);
        String string = version.replaceAll("[^(\\d*)]", "");
        if (Integer.parseInt(string.substring(0, 1)) < 7) {
            belowVersion7 = true;
        }
    }

    public boolean belowVersion7() {
        return this.belowVersion7;
    }

    public String version() {
        try {
            return restHighLevelClient.info(RequestOptions.DEFAULT).getVersion().getNumber();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return "";
    }

    @Override
    public void close() throws IOException {
        if (restHighLevelClient != null) {
            restHighLevelClient.close();
        }
        log.info("Elasticsearch Template closed.");
    }

    @NoArgsConstructor
    @Setter
    @Getter
    @AllArgsConstructor
    public static class SearchAfterResult<T> {

        private List<T> list = Collections.emptyList();

        private Object[] sortValues;

    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    public static class ExecuteResult {
        @JsonProperty("total_hits")
        protected long totalHits;

        protected boolean success;

    }

    @Getter
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class GetResult extends ExecuteResult {
        private String id;
        private String index;
        private Map<String, Object> source;
        private String sourceAsString;
        private Object[] sortValues;

        private GetResult(String index, String id, Map<String, Object> source) {
            this.index = index;
            this.id = id;
            this.source = source;
            this.sourceAsString = JsonUtil.toJsonString(source);
            super.success = true;
        }

        private GetResult(String index, String id, Map<String, Object> source, String sourceAsString) {
            this.index = index;
            this.id = id;
            this.source = source;
            this.sourceAsString = sourceAsString;
            super.success = true;
        }

        private GetResult(String index, String id, Map<String, Object> source, String sourceAsString, Object[] sortValues) {
            this.index = index;
            this.id = id;
            this.source = source;
            this.sourceAsString = sourceAsString;
            this.sortValues = sortValues;
            super.success = true;
        }
    }

    @Getter
    @Setter
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class SearchResult extends ExecuteResult {
        private List<GetResult> resultList = Collections.emptyList();
        private String scrollId;
        private Aggregations aggregations = new Aggregations(Collections.emptyList());

        private SearchResult(List<GetResult> resultList) {
            this.resultList = resultList;
        }

    }

    @Getter
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class DeleteResult extends ExecuteResult {
        private String id;
        private String index;

        private DeleteResult(String index, String id) {
            this.index = index;
            this.id = id;
            super.success = true;
        }
    }

    @Getter
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class CreateResult extends ExecuteResult {
        private String id;
        private String index;
        private Long seqNo;
        private Long version;

        private CreateResult(String index, String id, Long seqNo, Long version) {
            this.id = id;
            this.index = index;
            this.seqNo = seqNo;
            this.version = version;
            super.success = true;
        }
    }

    public ESResult buildESResult(SearchResult searchResult) {
        if (!searchResult.isSuccess()) {
            return new ESResult(0, Collections.emptyList());
        }
        List<Map<String, Object>> logs = searchResult.getResultList().stream()
                .map(GetResult::getSource).filter(Objects::nonNull)
                .collect(Collectors.toList());
        return new ESResult(searchResult.getTotalHits(), logs);
    }

    public ESResult buildESResultString(SearchResult searchResult) {
        if (!searchResult.isSuccess()) {
            return new ESResult(Collections.emptyList(), 0);
        }
        List<String> stringList = searchResult.getResultList().stream()
                .map(GetResult::getSourceAsString).filter(Objects::nonNull)
                .collect(Collectors.toList());
        return new ESResult(stringList, searchResult.getTotalHits());
    }

    @Getter
    public static class ESResult {
        private long count;
        private List<Map<String, Object>> data = Collections.emptyList();
        private List<String> strings = Collections.emptyList();

        ESResult(long count, List<Map<String, Object>> data) {
            this.count = count;
            this.data = data;
        }

        ESResult(List<String> strings, long count) {
            this.count = count;
            this.strings = strings;
        }

    }

    private static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        map.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        map.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        map.put(MedianAbsoluteDeviationAggregationBuilder.NAME, (p, c) -> ParsedMedianAbsoluteDeviation.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(WeightedAvgAggregationBuilder.NAME, (p, c) -> ParsedWeightedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        map.put(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c));
        map.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME,
                (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));
        map.put(GeoBoundsAggregationBuilder.NAME, (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c));
        map.put(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c));
        map.put(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c));
        map.put(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c));
        map.put(AutoDateHistogramAggregationBuilder.NAME, (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));
        map.put(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c));
        map.put(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c));
        map.put(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c));
        map.put(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c));
        map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
        map.put(InternalSampler.PARSER_NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c));
        map.put(GeoHashGridAggregationBuilder.NAME, (p, c) -> ParsedGeoHashGrid.fromXContent(p, (String) c));
        map.put(GeoTileGridAggregationBuilder.NAME, (p, c) -> ParsedGeoTileGrid.fromXContent(p, (String) c));
        map.put(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c));
        map.put(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c));
        map.put(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c));
        map.put(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c));
        map.put(AdjacencyMatrixAggregationBuilder.NAME, (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c));
        map.put(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c));
        map.put(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c));
        map.put(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c));
        map.put(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c));
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        map.put(CompositeAggregationBuilder.NAME, (p, c) -> ParsedComposite.fromXContent(p, (String) c));
        List<NamedXContentRegistry.Entry> entries = map.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(TermSuggestionBuilder.SUGGESTION_NAME),
                (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)));
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(PhraseSuggestionBuilder.SUGGESTION_NAME),
                (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context)));
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(CompletionSuggestionBuilder.SUGGESTION_NAME),
                (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context)));
        return entries;
    }

}
