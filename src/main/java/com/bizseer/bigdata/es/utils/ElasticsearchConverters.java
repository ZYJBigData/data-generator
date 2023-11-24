package com.bizseer.bigdata.es.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xcontent.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * @author CharleyXu
 * @date 2020-12-23
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings({"UnusedReturnValue", "WeakerAccess"})
public class ElasticsearchConverters {

    private static final XContentType REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;

    private static final String TOTAL_HIT_AS_INT_PARAM = "rest_total_hits_as_int";

    private static final String SCROLL = "scroll";

    public static Request bulk(BulkRequest bulkRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_bulk");
        Params parameters = new Params();
        parameters.withTimeout(bulkRequest.timeout());
        parameters.withRefreshPolicy(bulkRequest.getRefreshPolicy());
        parameters.withPipeline(bulkRequest.pipeline());
        parameters.withRouting(bulkRequest.routing());
        XContentType bulkContentType = null;
        for (int i = 0; i < bulkRequest.numberOfActions(); i++) {
            DocWriteRequest<?> action = bulkRequest.requests().get(i);

            DocWriteRequest.OpType opType = action.opType();
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                bulkContentType = enforceSameContentType((IndexRequest) action, bulkContentType);

            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                UpdateRequest updateRequest = (UpdateRequest) action;
                if (updateRequest.doc() != null) {
                    bulkContentType = enforceSameContentType(updateRequest.doc(), bulkContentType);
                }
                if (updateRequest.upsertRequest() != null) {
                    bulkContentType = enforceSameContentType(updateRequest.upsertRequest(), bulkContentType);
                }
            }
        }

        if (bulkContentType == null) {
            bulkContentType = XContentType.JSON;
        }

        final byte separator = bulkContentType.xContent().streamSeparator();
        final ContentType requestContentType = createContentType(bulkContentType);

        ByteArrayOutputStream content = new ByteArrayOutputStream();
        for (DocWriteRequest<?> action : bulkRequest.requests()) {
            DocWriteRequest.OpType opType = action.opType();

            try (XContentBuilder metadata = XContentBuilder.builder(bulkContentType.xContent())) {
                metadata.startObject();
                {
                    metadata.startObject(opType.getLowercase());
                    if (Strings.hasLength(action.index())) {
                        metadata.field("_index", action.index());
                    }
                    metadata.field("_type", "_doc");
                    if (Strings.hasLength(action.id())) {
                        metadata.field("_id", action.id());
                    }
                    if (Strings.hasLength(action.routing())) {
                        metadata.field("routing", action.routing());
                    }
                    if (action.version() != Versions.MATCH_ANY) {
                        metadata.field("version", action.version());
                    }

                    VersionType versionType = action.versionType();
                    if (versionType != VersionType.INTERNAL) {
                        if (versionType == VersionType.EXTERNAL) {
                            metadata.field("version_type", "external");
                        } else if (versionType == VersionType.EXTERNAL_GTE) {
                            metadata.field("version_type", "external_gte");
                        }
                    }

                    if (action.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        metadata.field("if_seq_no", action.ifSeqNo());
                        metadata.field("if_primary_term", action.ifPrimaryTerm());
                    }

                    if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                        IndexRequest indexRequest = (IndexRequest) action;
                        if (Strings.hasLength(indexRequest.getPipeline())) {
                            metadata.field("pipeline", indexRequest.getPipeline());
                        }
                    } else if (opType == DocWriteRequest.OpType.UPDATE) {
                        UpdateRequest updateRequest = (UpdateRequest) action;
                        if (updateRequest.retryOnConflict() > 0) {
                            metadata.field("retry_on_conflict", updateRequest.retryOnConflict());
                        }
                        if (updateRequest.fetchSource() != null) {
                            metadata.field("_source", updateRequest.fetchSource());
                        }
                    }
                    metadata.endObject();
                }
                metadata.endObject();

                BytesRef metadataSource = BytesReference.bytes(metadata).toBytesRef();
                content.write(metadataSource.bytes, metadataSource.offset, metadataSource.length);
                content.write(separator);
            }
            BytesRef source = null;
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest indexRequest = (IndexRequest) action;
                BytesReference indexSource = indexRequest.source();
                XContentType indexXContentType = indexRequest.getContentType();
                try (XContentParser parser = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    indexSource, indexXContentType)) {
                    try (XContentBuilder builder = XContentBuilder.builder(bulkContentType.xContent())) {
                        builder.copyCurrentStructure(parser);
                        source = BytesReference.bytes(builder).toBytesRef();
                    }
                }
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                source = XContentHelper.toXContent((UpdateRequest) action, bulkContentType, false).toBytesRef();
            }

            if (source != null) {
                content.write(source.bytes, source.offset, source.length);
                content.write(separator);
            }
        }
        request.addParameters(parameters.asMap());
        request.setEntity(new NByteArrayEntity(content.toByteArray(), 0, content.size(), requestContentType));
        return request;
    }

    public static Request search(SearchRequest searchRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME,
            endpoint(searchRequest.indices(), "_search"));
        Params params = new Params();
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        params.withIndicesOptions(searchRequest.indicesOptions());
        if (Objects.nonNull(searchRequest.scroll())) {
            params.putParam(SCROLL, searchRequest.scroll().keepAlive().toString());
        }
        if (searchRequest.source() != null) {
            request.setEntity(createEntity(searchRequest.source()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    public static Request count(CountRequest countRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME,
            endpoint(countRequest.indices(), "_count"));
        Params params = new Params();
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        params.withIndicesOptions(countRequest.indicesOptions());
        if (countRequest.query() != null) {
            request.setEntity(createEntity(countRequest.query()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    public static Request multiSearch(MultiSearchRequest multiSearchRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_msearch");
        Params params = new Params();
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        params.putParam(TOTAL_HIT_AS_INT_PARAM, "true");
        if (multiSearchRequest.maxConcurrentSearchRequests() != MultiSearchRequest.MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT) {
            params.putParam("max_concurrent_searches", Integer.toString(multiSearchRequest.maxConcurrentSearchRequests()));
        }
        XContent xContent = REQUEST_BODY_CONTENT_TYPE.xContent();
        byte[] source = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, xContent);
        request.addParameters(params.asMap());
        request.setEntity(new NByteArrayEntity(source, createContentType(xContent.type())));
        return request;
    }

    public static Request refresh(RefreshRequest refreshRequest) {
        String[] indices = refreshRequest.indices() == null ? Strings.EMPTY_ARRAY : refreshRequest.indices();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint(indices, "_refresh"));
        Params parameters = new Params();
        parameters.withIndicesOptions(refreshRequest.indicesOptions());
        request.addParameters(parameters.asMap());
        return request;
    }

    public static Request update(UpdateRequest updateRequest) throws IOException {
        String endpoint = endpointUpdate(updateRequest.index(), updateRequest.id());
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        Params parameters = new Params();
        parameters.withRouting(updateRequest.routing());
        parameters.withTimeout(updateRequest.timeout());
        parameters.withRefreshPolicy(updateRequest.getRefreshPolicy());
        parameters.withDocAsUpsert(updateRequest.docAsUpsert());
        XContentType xContentType = null;
        if (updateRequest.doc() != null) {
            xContentType = updateRequest.doc().getContentType();
        }
        if (updateRequest.upsertRequest() != null) {
            XContentType upsertContentType = updateRequest.upsertRequest().getContentType();
            if ((xContentType != null) && (xContentType != upsertContentType)) {
                throw new IllegalStateException("Update request cannot have different content types for doc [" + xContentType + "]" +
                    " and upsert [" + upsertContentType + "] documents");
            } else {
                xContentType = upsertContentType;
            }
        }
        if (xContentType == null) {
            xContentType = Requests.INDEX_CONTENT_TYPE;
        }
        request.addParameters(parameters.asMap());
        request.setEntity(createEntity(updateRequest, xContentType));
        return request;
    }

    private static HttpEntity createEntity(ToXContent toXContent)
        throws IOException {
        BytesRef source = XContentHelper.toXContent(toXContent, REQUEST_BODY_CONTENT_TYPE, ToXContent.EMPTY_PARAMS, false).toBytesRef();
        return new NByteArrayEntity(source.bytes, source.offset, source.length, createContentType(REQUEST_BODY_CONTENT_TYPE));
    }

    private static String endpoint(String[] indices, String endpoint) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices)
            .addPathPartAsIs(endpoint).build();
    }

    private static String endpointUpdate(String index, String id) {
        return new EndpointBuilder().addPathPart(index, "_doc", id).addPathPartAsIs("_update").build();
    }

    /**
     * Utility class to build request's endpoint given its parts as strings
     */
    private static class EndpointBuilder {

        private final StringJoiner joiner = new StringJoiner("/", "/", "");

        EndpointBuilder addPathPart(String... parts) {
            for (String part : parts) {
                if (Strings.hasLength(part)) {
                    joiner.add(encodePart(part));
                }
            }
            return this;
        }

        EndpointBuilder addCommaSeparatedPathParts(String[] parts) {
            addPathPart(String.join(",", parts));
            return this;
        }

        EndpointBuilder addPathPartAsIs(String... parts) {
            for (String part : parts) {
                if (Strings.hasLength(part)) {
                    joiner.add(part);
                }
            }
            return this;
        }

        String build() {
            return joiner.toString();
        }

        private static String encodePart(String pathPart) {
            try {
                URI uri = new URI(null, "", "/" + pathPart, null, null);
                return uri.getRawPath().substring(1).replaceAll("/", "%2F");
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Path part [" + pathPart + "] couldn't be encoded", e);
            }
        }
    }


    private static class Params {

        private final Map<String, String> parameters = new HashMap<>();

        Params() {
        }

        Params putParam(String name, String value) {
            if (Strings.hasLength(value)) {
                parameters.put(name, value);
            }
            return this;
        }

        Params putParam(TimeValue value) {
            if (value != null) {
                return putParam("timeout", value.getStringRep());
            }
            return this;
        }

        Map<String, String> asMap() {
            return parameters;
        }

        Params withPipeline(String pipeline) {
            return putParam("pipeline", pipeline);
        }

        Params withRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                return putParam("refresh", refreshPolicy.getValue());
            }
            return this;
        }

        Params withRouting(String routing) {
            return putParam("routing", routing);
        }

        Params withTimeout(TimeValue timeout) {
            return putParam(timeout);
        }

        Params withIndicesOptions(IndicesOptions indicesOptions) {
            if (indicesOptions != null) {
                withIgnoreUnavailable(indicesOptions.ignoreUnavailable());
                putParam("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
                String expandWildcards;
                if (!indicesOptions.expandWildcardsOpen() && !indicesOptions.expandWildcardsClosed()) {
                    expandWildcards = "none";
                } else {
                    StringJoiner joiner = new StringJoiner(",");
                    if (indicesOptions.expandWildcardsOpen()) {
                        joiner.add("open");
                    }
                    if (indicesOptions.expandWildcardsClosed()) {
                        joiner.add("closed");
                    }
                    expandWildcards = joiner.toString();
                }
                putParam("expand_wildcards", expandWildcards);
                putParam("ignore_throttled", Boolean.toString(indicesOptions.ignoreThrottled()));
            }
            return this;
        }

        Params withIgnoreUnavailable(boolean ignoreUnavailable) {
            // Always explicitly place the ignore_unavailable value.
            putParam("ignore_unavailable", Boolean.toString(ignoreUnavailable));
            return this;
        }

        Params withDocAsUpsert(boolean docAsUpsert) {
            if (docAsUpsert) {
                return putParam("doc_as_upsert", Boolean.TRUE.toString());
            }
            return this;
        }

    }

    private static XContentType enforceSameContentType(IndexRequest indexRequest, XContentType xContentType) {
        XContentType requestContentType = indexRequest.getContentType();
        if (requestContentType != XContentType.JSON && requestContentType != XContentType.SMILE) {
            throw new IllegalArgumentException("Unsupported content-type found for request with content-type [" + requestContentType
                + "], only JSON and SMILE are supported");
        }
        if (xContentType == null) {
            return requestContentType;
        }
        if (requestContentType != xContentType) {
            throw new IllegalArgumentException("Mismatching content-type found for request with content-type [" + requestContentType
                + "], previous requests have content-type [" + xContentType + "]");
        }
        return xContentType;
    }

    private static ContentType createContentType(final XContentType xContentType) {
        return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
    }

    private static HttpEntity createEntity(ToXContent toXContent, XContentType xContentType) throws IOException {
        BytesRef source = XContentHelper.toXContent(toXContent, xContentType, ToXContent.EMPTY_PARAMS, false).toBytesRef();
        return new NByteArrayEntity(source.bytes, source.offset, source.length, createContentType(xContentType));
    }

}
