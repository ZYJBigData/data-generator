package com.bizseer.bigdata.mysql;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.List;

public class SelectSqlBuilder {

    private final static SQLUtils.FormatOption FORMAT_OPTION = new SQLUtils.FormatOption(false, false);

    private DbType dbType;
    private SQLSelectQueryBlock block;

    private String sql;

    public SelectSqlBuilder(DbType dbType) {
        this.dbType = dbType;
        this.block = new SQLSelectQueryBlock(dbType);
    }

    public SelectSqlBuilder select(String column) {
        if (StringUtils.hasText(column)) {
            this.block.addSelectItem(column, null);
        }
        return this;
    }

    public SelectSqlBuilder select(String column, String alias) {
        if (StringUtils.hasText(column)) {
            this.block.addSelectItem(column, alias);
        }
        return this;
    }

    public SelectSqlBuilder select(List<String> columns) {
        if (!CollectionUtils.isEmpty(columns)) {
            columns.forEach(column -> {
                if (StringUtils.hasText(column)) {
                    this.block.addSelectItem(column, null);
                }
            });
        }
        return this;
    }

    public SelectSqlBuilder from(String tableName) {
        if (StringUtils.hasText(tableName)) {
            this.block.setFrom(tableName, null);
        }
        return this;
    }

    public SelectSqlBuilder addCondition(String condition) {
        if (StringUtils.hasText(condition)) {
            block.addWhere(SQLUtils.toSQLExpr(condition, dbType));
        }
        return this;
    }

    public SelectSqlBuilder addConditions(List<String> conditions) {
        if (!CollectionUtils.isEmpty(conditions)) {
            for (String condition : conditions) {
                if (StringUtils.hasText(condition)) {
                    block.addWhere(SQLUtils.toSQLExpr(condition, dbType));
                }
            }
        }
        return this;
    }

    public SelectSqlBuilder orderBy(String column, SQLOrderingSpecification type) {
        if (StringUtils.hasText(column)) {
            SQLExpr sqlExpr = SQLUtils.toSQLExpr(column, dbType);
            ;
            SQLOrderBy sqlOrderBy = null != type ? new SQLOrderBy(sqlExpr, type) : new SQLOrderBy(sqlExpr);
            block.setOrderBy(sqlOrderBy);
        }
        return this;
    }

    public SelectSqlBuilder limit(Integer rowCount) {
        return this.limit(0, rowCount);
    }

    public SelectSqlBuilder limit(Integer offset, Integer rowCount) {
        if (null != rowCount) {
            block.limit(rowCount, null != offset ? offset : 0);
        }
        return this;
    }

    public String build() {
        if (null == sql) {
            sql = SQLUtils.toSQLString(block, dbType, FORMAT_OPTION);
        }
        return sql;
    }

    @Override
    public String toString() {
        return this.build();
    }

    public static void main(String[] args) {

//        SelectSqlBuilder builder = new SelectSqlBuilder(DbType.mysql)
//                .select(Arrays.asList("col_1", "col_2", "col_3"))
//                .from("test_table")
//                .addCondition("a = 1")
//                .addCondition("b in (1,2,3)")
//                .addCondition("c like 'test%' or d like '%test'")
//                .orderBy("id", SQLOrderingSpecification.DESC)
//                .limit(100, 20);
//
//        System.out.println(builder.build());

                SelectSqlBuilder builder = new SelectSqlBuilder(DbType.mysql)
                .select("count(*)")
                .from("test_table");

        System.out.println(builder.build());
    }

}
