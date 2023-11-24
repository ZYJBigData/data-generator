package com.bizseer.bigdata.es.utils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.collapse.CollapseBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xuzhenchang
 */
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
@SuppressWarnings({"unused"})
public class ESQueryParameter {

    private final BoolQueryBuilder builder = QueryBuilders.boolQuery();

    private final List<String> ascList = new ArrayList<>();

    private final List<String> descList = new ArrayList<>();

    private String[] includes;

    private String[] excludes;

    private TimeValue timeValue;

    private Object[] sortValues;

    public ESQueryParameter() {
    }


    public ESQueryParameter sortValues(Object[] sortValues) {
        this.sortValues = sortValues;
        return this;
    }

    public ESQueryParameter asc(@NonNull String... fields) {
        ascList.addAll(Arrays.asList(fields));
        return this;
    }

    public ESQueryParameter desc(@NonNull String... fields) {
        descList.addAll(Arrays.asList(fields));
        return this;
    }

    public ESQueryParameter includes(@NonNull String... includeFields) {
        includes = includeFields;
        return this;
    }

    public ESQueryParameter excludes(@NonNull String... excludeFields) {
        excludes = excludeFields;
        return this;
    }

    public ESQueryParameter must(QueryBuilder... clauses) {
        for (QueryBuilder clause : clauses) {
            builder.must(clause);
        }
        return this;
    }

    public ESQueryParameter mustNot(QueryBuilder... clauses) {
        for (QueryBuilder clause : clauses) {
            builder.mustNot(clause);
        }
        return this;
    }

    public ESQueryParameter filter(QueryBuilder... clauses) {
        for (QueryBuilder clause : clauses) {
            builder.filter(clause);
        }
        return this;
    }

    public ESQueryParameter filter(List<QueryBuilder> clauses) {
        for (QueryBuilder clause : clauses) {
            builder.filter(clause);
        }
        return this;
    }

    public ESQueryParameter should(QueryBuilder... clauses) {
        for (QueryBuilder clause : clauses) {
            builder.should(clause);
        }
        return this;
    }

    public ESQueryParameter scroll(TimeValue scrollTimeValue) {
        timeValue = scrollTimeValue;
        return this;
    }

    public static ESQueryParameter query() {
        return new ESQueryParameter();
    }

    public static MatchAllQueryBuilder matchAll() {
        return new MatchAllQueryBuilder();
    }

    public static MatchQueryBuilder match(String field, Object value) {
        return new MatchQueryBuilder(field, value);
    }

    public static MatchQueryBuilder matchEach(String field, String value) {
        return new MatchQueryBuilder(field, value).operator(Operator.AND);
    }

    public static MatchPhraseQueryBuilder matchPhrase(String field, String value) {
        return new MatchPhraseQueryBuilder(field, value);
    }

    public static MatchPhrasePrefixQueryBuilder matchPhrasePrefix(String field, String value) {
        return new MatchPhrasePrefixQueryBuilder(field, value);
    }

    public static TermQueryBuilder term(String field, Object value) {
        return new TermQueryBuilder(field, value);
    }

    public static TermsQueryBuilder terms(String field, List<?> value) {
        return new TermsQueryBuilder(field, value);
    }

    public static CollapseBuilder collapse(String distinctField) {
        return new CollapseBuilder(distinctField);
    }

    public static RangeQueryBuilder range(String field) {
        return new RangeQueryBuilder(field);
    }

    public static ExistsQueryBuilder exist(String field) {
        return new ExistsQueryBuilder(field);
    }

    public static PrefixQueryBuilder prefix(String field, String prefix) {
        return new PrefixQueryBuilder(field, prefix);
    }

    public static WildcardQueryBuilder wildcard(String field, String wildcard) {
        return new WildcardQueryBuilder(field, String.format("*%s*", wildcard));
    }

    public static WildcardQueryBuilder prefixWildcard(String field, String wildcard) {
        return new WildcardQueryBuilder(field, String.format("*%s", wildcard));
    }

}
