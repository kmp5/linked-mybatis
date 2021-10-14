package com.kzow3n.jdbcplus.core.wrapper;

import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.kzow3n.jdbcplus.core.wrapper.column.AggregateWrapper;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import com.kzow3n.jdbcplus.utils.ColumnUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.Configuration;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Sql构造器
 *
 * @author owen
 * @since 2021/8/4
 */
public class LinkedQueryWrapper extends BaseLinkedQueryWrapper {

    public LinkedQueryWrapper() {
        init();
    }

    public LinkedQueryWrapper(Configuration configuration) {
        init();
        initConfiguration(configuration);
    }

    public void initSql() {
        baseSql = null;
        fullSql = null;
        blnFormatSql = false;
    }

    //region 拼接基本Sql

    public LinkedQueryWrapper distinct() {
        blnDistinct = true;
        return this;
    }

    public LinkedQueryWrapper selectAll(Integer... tableIndexes) {
        for (Integer tableIndex : tableIndexes) {
            TableInfo selectAllTableInfo = new TableInfo(tableIndex, true);
            selectAllTableInfos.add(selectAllTableInfo);
        }
        return this;
    }

    @SafeVarargs
    public final <K> LinkedQueryWrapper select(Integer tableIndex, SFunction<K, ?>... fns) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        for (SFunction<K, ?> fn : fns) {
            Field field = ColumnUtils.getField(fn);
            addColumnInfoByField(tableInfo, field, true, null, null);
        }
        return this;
    }

    public final <K, M> LinkedQueryWrapper select(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, null);
        return this;
    }

    public final <K> LinkedQueryWrapper select(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, null);
        return this;
    }

    public final <M> LinkedQueryWrapper select(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, null);
        return this;
    }

    public LinkedQueryWrapper select(Integer tableIndex, String tableColumns, @Nullable String beanColumns) {
        addColumnInfo(tableIndex, tableColumns, beanColumns, null);
        return this;
    }

    public LinkedQueryWrapper from(Class<?> clazz, String tableId) {
        appendFrom(clazz, tableId);
        return this;
    }

    public LinkedQueryWrapper from(Consumer<LinkedQueryWrapper> consumer, String tableId) {
        appendFrom(consumer, tableId);
        return this;
    }

    public LinkedQueryWrapper innerJoin(Class<?> clazz, String tableId) {
        appendJoin(clazz, tableId, "inner join");
        return this;
    }

    public LinkedQueryWrapper innerJoin(Consumer<LinkedQueryWrapper> consumer, String tableId) {
        appendJoin(consumer, tableId, "inner join");
        return this;
    }

    public LinkedQueryWrapper leftJoin(Class<?> clazz, String tableId) {
        appendJoin(clazz, tableId, "left join");
        return this;
    }

    public LinkedQueryWrapper leftJoin(Consumer<LinkedQueryWrapper> consumer, String tableId) {
        appendJoin(consumer, tableId, "left join");
        return this;
    }

    public LinkedQueryWrapper rightJoin(Class<?> clazz, String tableId) {
        appendJoin(clazz, tableId, "right join");
        return this;
    }

    public LinkedQueryWrapper rightJoin(Consumer<LinkedQueryWrapper> consumer, String tableId) {
        appendJoin(consumer, tableId, "right join");
        return this;
    }

    public LinkedQueryWrapper fullJoin(Class<?> clazz, String tableId) {
        appendJoin(clazz, tableId, "full join");
        return this;
    }

    public LinkedQueryWrapper fullJoin(Consumer<LinkedQueryWrapper> consumer, String tableId) {
        appendJoin(consumer, tableId, "full join");
        return this;
    }

    public <K, M> LinkedQueryWrapper on(Integer tableIndex1, SFunction<K, ?> fn1, Integer tableIndex2, SFunction<M, ?> fn2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        TableInfo tableInfo2 = getTableInfoByIndex(tableIndex2);
        String tableId1 = tableInfo1.getTableId();
        String tableId2 = tableInfo2.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        String column1 = ColumnUtils.getColumn(tableInfo1, field1, mapUnderscoreToCamelCase);
        Field field2 = ColumnUtils.getField(fn2);
        String column2 = ColumnUtils.getColumn(tableInfo2, field2, mapUnderscoreToCamelCase);
        appendOn(tableId1, column1, tableId2, column2);
        return this;
    }

    public <K> LinkedQueryWrapper on(Integer tableIndex1, SFunction<K, ?> fn1, Integer tableIndex2, String column2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        TableInfo tableInfo2 = getTableInfoByIndex(tableIndex2);
        String tableId1 = tableInfo1.getTableId();
        String tableId2 = tableInfo2.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        String column1 = ColumnUtils.getColumn(tableInfo1, field1, mapUnderscoreToCamelCase);
        appendOn(tableId1, column1, tableId2, column2);
        return this;
    }

    public <M> LinkedQueryWrapper on(Integer tableIndex1, String column1, Integer tableIndex2, SFunction<M, ?> fn2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        TableInfo tableInfo2 = getTableInfoByIndex(tableIndex2);
        String tableId1 = tableInfo1.getTableId();
        String tableId2 = tableInfo2.getTableId();
        Field field2 = ColumnUtils.getField(fn2);
        String column2 = ColumnUtils.getColumn(tableInfo2, field2, mapUnderscoreToCamelCase);
        appendOn(tableId1, column1, tableId2, column2);
        return this;
    }

    public LinkedQueryWrapper on(Integer tableIndex1, String column1, Integer tableIndex2, String column2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        TableInfo tableInfo2 = getTableInfoByIndex(tableIndex2);
        String tableId1 = tableInfo1.getTableId();
        String tableId2 = tableInfo2.getTableId();
        appendOn(tableId1, column1, tableId2, column2);
        return this;
    }

    public final <K> LinkedQueryWrapper count(SFunction<K, ?> fn) {
        Field field = ColumnUtils.getField(fn);
        String beanColumn = field.getName();
        addColumnInfo(null, "count(*)", beanColumn, null);
        return this;
    }

    public LinkedQueryWrapper count(String beanColumn) {
        addColumnInfo(null, "count(*)", beanColumn, null);
        return this;
    }

    public final <K, M> LinkedQueryWrapper count(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "count(%s)");
        return this;
    }

    public final <K> LinkedQueryWrapper count(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "count(%s)");
        return this;
    }

    public final <M> LinkedQueryWrapper count(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "count(%s)");
        return this;
    }

    public LinkedQueryWrapper count(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "count(%s)");
        return this;
    }

    public final <K, M> LinkedQueryWrapper avg(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "avg(%s)");
        return this;
    }

    public final <K> LinkedQueryWrapper avg(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "avg(%s)");
        return this;
    }

    public final <M> LinkedQueryWrapper avg(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "avg(%s)");
        return this;
    }

    public LinkedQueryWrapper avg(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "avg(%s)");
        return this;
    }

    public final <K, M> LinkedQueryWrapper sum(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "sum(%s)");
        return this;
    }

    public final <K> LinkedQueryWrapper sum(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "sum(%s)");
        return this;
    }

    public final <M> LinkedQueryWrapper sum(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "sum(%s)");
        return this;
    }

    public LinkedQueryWrapper sum(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "sum(%s)");
        return this;
    }

    public final <K, M> LinkedQueryWrapper max(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "max(%s)");
        return this;
    }

    public final <K> LinkedQueryWrapper max(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "max(%s)");
        return this;
    }

    public final <M> LinkedQueryWrapper max(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "max(%s)");
        return this;
    }

    public LinkedQueryWrapper max(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "max(%s)");
        return this;
    }

    public final <K, M> LinkedQueryWrapper min(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "min(%s)");
        return this;
    }

    public final <K> LinkedQueryWrapper min(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "min(%s)");
        return this;
    }

    public final <M> LinkedQueryWrapper min(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "min(%s)");
        return this;
    }

    public LinkedQueryWrapper min(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "min(%s)");
        return this;
    }

    public final <K, M> LinkedQueryWrapper groupConcat(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "group_concat(%s)");
        return this;
    }

    public final <K> LinkedQueryWrapper groupConcat(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "group_concat(%s)");
        return this;
    }

    public final <M> LinkedQueryWrapper groupConcat(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "group_concat(%s)");
        return this;
    }

    public LinkedQueryWrapper groupConcat(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "group_concat(%s)");
        return this;
    }

    public LinkedQueryWrapper formatSql() {
        formatFullSql();
        return this;
    }

    //endregion

    //region 条件构造器

    public LinkedQueryWrapper or() {
        blnOr = true;
        return this;
    }

    public LinkedQueryWrapper or(Consumer<LinkedQueryWrapper> consumer) {
        blnOr = true;
        spendOperator();
        sqlBuilder.append("(");
        blnOpenBracket = true;
        consumer.accept(this);
        sqlBuilder.append(") ");
        return this;
    }

    public LinkedQueryWrapper and(Consumer<LinkedQueryWrapper> consumer) {
        spendOperator();
        sqlBuilder.append("(");
        blnOpenBracket = true;
        consumer.accept(this);
        sqlBuilder.append(") ");
        return this;
    }

    public <K> LinkedQueryWrapper isNull(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        isNull(tableId, column);
        return this;
    }

    public LinkedQueryWrapper isNull(Integer tableIndex, String column) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        isNull(tableId, column);
        return this;
    }

    public <K> LinkedQueryWrapper isNotNull(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        isNotNull(tableId, column);
        return this;
    }

    public LinkedQueryWrapper isNotNull(Integer tableIndex, String column) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        isNotNull(tableId, column);
        return this;
    }

    public <K> LinkedQueryWrapper eq(Integer tableIndex, SFunction<K, ?> fn, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        eq(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper eq(Integer tableIndex, String column, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        eq(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper eq(Integer tableIndex, Consumer<AggregateWrapper> consumer, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        eq(tableId, column, arg, null);
        return this;
    }

    public <K> LinkedQueryWrapper eq(Integer tableIndex, SFunction<K, ?> fn, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        eq(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper eq(Integer tableIndex, String column, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        eq(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper eq(Integer tableIndex, Consumer<AggregateWrapper> consumer, Consumer<LinkedQueryWrapper> consumer2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        eq(tableId, column, null, consumer2);
        return this;
    }

    public <K, M> LinkedQueryWrapper eq(Integer tableIndex1, SFunction<K, ?> fn1, String tableId2, SFunction<M, ?> fn2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        String tableId1 = tableInfo1.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        String column1 = ColumnUtils.getColumn(tableInfo1, field1, mapUnderscoreToCamelCase);
        Field field2 = ColumnUtils.getField(fn2);
        TableInfo tableInfo2 = getTableInfoById(parentTableInfos, tableId2);
        String column2 = ColumnUtils.getColumn(tableInfo2, field2, mapUnderscoreToCamelCase);
        eq(tableId1, column1, tableId2, column2);
        return this;
    }

    public <K> LinkedQueryWrapper eq(Integer tableIndex1, SFunction<K, ?> fn1, String tableId2, String column2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        String tableId1 = tableInfo1.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        String column1 = ColumnUtils.getColumn(tableInfo1, field1, mapUnderscoreToCamelCase);
        eq(tableId1, column1, tableId2, column2);
        return this;
    }

    public <K> LinkedQueryWrapper ne(Integer tableIndex, SFunction<K, ?> fn, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        ne(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper ne(Integer tableIndex, String column, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        ne(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper ne(Integer tableIndex, Consumer<AggregateWrapper> consumer, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        ne(tableId, column, arg, null);
        return this;
    }

    public <K> LinkedQueryWrapper ne(Integer tableIndex, SFunction<K, ?> fn, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        ne(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper ne(Integer tableIndex, String column, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        ne(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper ne(Integer tableIndex, Consumer<AggregateWrapper> consumer, Consumer<LinkedQueryWrapper> consumer2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        ne(tableId, column, null, consumer2);
        return this;
    }

    public <K> LinkedQueryWrapper gt(Integer tableIndex, SFunction<K, ?> fn, Object arg) {
        if (arg == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        gt(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper gt(Integer tableIndex, String column, Object arg) {
        if (arg == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        gt(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper gt(Integer tableIndex, Consumer<AggregateWrapper> consumer, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        gt(tableId, column, arg, null);
        return this;
    }

    public <K> LinkedQueryWrapper gt(Integer tableIndex, SFunction<K, ?> fn, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        gt(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper gt(Integer tableIndex, String column, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        gt(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper gt(Integer tableIndex, Consumer<AggregateWrapper> consumer, Consumer<LinkedQueryWrapper> consumer2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        gt(tableId, column, null, consumer2);
        return this;
    }

    public <K> LinkedQueryWrapper ge(Integer tableIndex, SFunction<K, ?> fn, Object arg) {
        if (arg == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        ge(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper ge(Integer tableIndex, String column, Object arg) {
        if (arg == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        ge(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper ge(Integer tableIndex, Consumer<AggregateWrapper> consumer, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        ge(tableId, column, arg, null);
        return this;
    }

    public <K> LinkedQueryWrapper ge(Integer tableIndex, SFunction<K, ?> fn, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        ge(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper ge(Integer tableIndex, String column, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        ge(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper ge(Integer tableIndex, Consumer<AggregateWrapper> consumer, Consumer<LinkedQueryWrapper> consumer2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        ge(tableId, column, null, consumer2);
        return this;
    }

    public <K> LinkedQueryWrapper lt(Integer tableIndex, SFunction<K, ?> fn, Object arg) {
        if (arg == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        lt(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper lt(Integer tableIndex, String column, Object arg) {
        if (arg == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        lt(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper lt(Integer tableIndex, Consumer<AggregateWrapper> consumer, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        lt(tableId, column, arg, null);
        return this;
    }

    public <K> LinkedQueryWrapper lt(Integer tableIndex, SFunction<K, ?> fn, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        lt(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper lt(Integer tableIndex, String column, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        lt(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper lt(Integer tableIndex, Consumer<AggregateWrapper> consumer, Consumer<LinkedQueryWrapper> consumer2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        lt(tableId, column, null, consumer2);
        return this;
    }

    public <K> LinkedQueryWrapper le(Integer tableIndex, SFunction<K, ?> fn, Object arg) {
        if (arg == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        le(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper le(Integer tableIndex, String column, Object arg) {
        if (arg == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        le(tableId, column, arg, null);
        return this;
    }

    public LinkedQueryWrapper le(Integer tableIndex, Consumer<AggregateWrapper> consumer, Object arg) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        le(tableId, column, arg, null);
        return this;
    }

    public <K> LinkedQueryWrapper le(Integer tableIndex, SFunction<K, ?> fn, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        le(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper le(Integer tableIndex, String column, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        le(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper le(Integer tableIndex, Consumer<AggregateWrapper> consumer, Consumer<LinkedQueryWrapper> consumer2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        le(tableId, column, null, consumer2);
        return this;
    }

    public <K> LinkedQueryWrapper like(Integer tableIndex, SFunction<K, ?> fn, String arg) {
        if (StringUtils.isBlank(arg)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        like(tableId, column, arg);
        return this;
    }

    public LinkedQueryWrapper like(Integer tableIndex, String column, String arg) {
        if (StringUtils.isBlank(arg)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        like(tableId, column, arg);
        return this;
    }

    public <K> LinkedQueryWrapper likeLeft(Integer tableIndex, SFunction<K, ?> fn, String arg) {
        if (StringUtils.isBlank(arg)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        likeLeft(tableId, column, arg);
        return this;
    }

    public LinkedQueryWrapper likeLeft(Integer tableIndex, String column, String arg) {
        if (StringUtils.isBlank(arg)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        likeLeft(tableId, column, arg);
        return this;
    }

    public <K> LinkedQueryWrapper likeRight(Integer tableIndex, SFunction<K, ?> fn, String arg) {
        if (StringUtils.isBlank(arg)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        likeRight(tableId, column, arg);
        return this;
    }

    public LinkedQueryWrapper likeRight(Integer tableIndex, String column, String arg) {
        if (StringUtils.isBlank(arg)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        likeRight(tableId, column, arg);
        return this;
    }

    public <K> LinkedQueryWrapper notLike(Integer tableIndex, SFunction<K, ?> fn, String arg) {
        if (StringUtils.isBlank(arg)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        notLike(tableId, column, arg);
        return this;
    }

    public LinkedQueryWrapper notLike(Integer tableIndex, String column, String arg) {
        if (StringUtils.isBlank(arg)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        notLike(tableId, column, arg);
        return this;
    }

    public <K> LinkedQueryWrapper between(Integer tableIndex, SFunction<K, ?> fn, Object arg1, Object arg2) {
        if (arg1 == null || arg2 == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        between(tableId, column, arg1, arg2);
        return this;
    }

    public LinkedQueryWrapper between(Integer tableIndex, String column, Object arg1, Object arg2) {
        if (arg1 == null || arg2 == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        between(tableId, column, arg1, arg2);
        return this;
    }

    public LinkedQueryWrapper between(Integer tableIndex, Consumer<AggregateWrapper> consumer, Object arg1, Object arg2) {
        if (arg1 == null || arg2 == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        between(tableId, column, arg1, arg2);
        return this;
    }

    public <K> LinkedQueryWrapper notBetween(Integer tableIndex, SFunction<K, ?> fn, Object arg1, Object arg2) {
        if (arg1 == null || arg2 == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        notBetween(tableId, column, arg1, arg2);
        return this;
    }

    public LinkedQueryWrapper notBetween(Integer tableIndex, String column, Object arg1, Object arg2) {
        if (arg1 == null || arg2 == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        notBetween(tableId, column, arg1, arg2);
        return this;
    }

    public LinkedQueryWrapper notBetween(Integer tableIndex, Consumer<AggregateWrapper> consumer, Object arg1, Object arg2) {
        if (arg1 == null || arg2 == null) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        AggregateWrapper aggregateWrapper = buildAggregateWrapper();
        consumer.accept(aggregateWrapper);
        String column = aggregateWrapper.getColumn();
        notBetween(tableId, column, arg1, arg2);
        return this;
    }

    public <K> LinkedQueryWrapper in(Integer tableIndex, SFunction<K, ?> fn, Object... args) {
        if (args == null || args.length == 0) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        List<Object> list = Arrays.asList(args);
        in(tableId, column, list, null);
        return this;
    }

    public LinkedQueryWrapper in(Integer tableIndex, String column, Object... args) {
        if (args == null || args.length == 0) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        List<Object> list = Arrays.asList(args);
        in(tableId, column, list, null);
        return this;
    }

    public <K> LinkedQueryWrapper in(Integer tableIndex, SFunction<K, ?> fn, List<?> args) {
        if (CollectionUtils.isEmpty(args)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        in(tableId, column, args, null);
        return this;
    }

    public LinkedQueryWrapper in(Integer tableIndex, String column, List<?> args) {
        if (CollectionUtils.isEmpty(args)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        in(tableId, column, args, null);
        return this;
    }

    public <K> LinkedQueryWrapper in(Integer tableIndex, SFunction<K, ?> fn, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        in(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper in(Integer tableIndex, String column, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        in(tableId, column, null, consumer);
        return this;
    }

    public <K> LinkedQueryWrapper notIn(Integer tableIndex, SFunction<K, ?> fn, Object... args) {
        if (args == null || args.length == 0) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        List<Object> list = Arrays.asList(args);
        notIn(tableId, column, list, null);
        return this;
    }

    public LinkedQueryWrapper notIn(Integer tableIndex, String column, Object... args) {
        if (args == null || args.length == 0) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        List<Object> list = Arrays.asList(args);
        notIn(tableId, column, list, null);
        return this;
    }

    public <K> LinkedQueryWrapper notIn(Integer tableIndex, SFunction<K, ?> fn, List<?> args) {
        if (CollectionUtils.isEmpty(args)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        notIn(tableId, column, args, null);
        return this;
    }

    public LinkedQueryWrapper notIn(Integer tableIndex, String column, List<?> args) {
        if (CollectionUtils.isEmpty(args)) {
            return this;
        }
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        notIn(tableId, column, args, null);
        return this;
    }

    public <K> LinkedQueryWrapper notIn(Integer tableIndex, SFunction<K, ?> fn, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        notIn(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper notIn(Integer tableIndex, String column, Consumer<LinkedQueryWrapper> consumer) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        notIn(tableId, column, null, consumer);
        return this;
    }

    public LinkedQueryWrapper exists(String sql, @Nullable Object... args) {
        appendExists(sql, args);
        return this;
    }

    public LinkedQueryWrapper exists(Consumer<LinkedQueryWrapper> consumer) {
        appendExists(consumer);
        return this;
    }

    public LinkedQueryWrapper notExists(String sql, @Nullable Object... args) {
        appendNotExists(sql, args);
        return this;
    }

    public LinkedQueryWrapper notExists(Consumer<LinkedQueryWrapper> consumer) {
        appendNotExists(consumer);
        return this;
    }

    @SafeVarargs
    public final <K> LinkedQueryWrapper groupBy(Integer tableIndex, SFunction<K, ?>... fns) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        groupBy(tableInfo, fns);
        return this;
    }

    public final <K, M> LinkedQueryWrapper groupBy(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        groupBy(tableInfo, fn1, fn2);
        return this;
    }

    public final <K> LinkedQueryWrapper groupBy(Integer tableIndex, SFunction<K, ?> fn1, @Nullable String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        groupBy(tableInfo, fn1, beanColumn);
        return this;
    }

    public final <M> LinkedQueryWrapper groupBy(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        groupBy(tableInfo, tableColumn, fn2);
        return this;
    }

    public LinkedQueryWrapper groupBy(Integer tableIndex, String tableColumn, @Nullable String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        groupBy(tableInfo, tableColumn, beanColumn);
        return this;
    }

    public LinkedQueryWrapper having(String sql, @Nullable Object... args) {
        appendHaving(sql, args);
        return this;
    }

    public LinkedQueryWrapper having(Consumer<LinkedQueryWrapper> consumer) {
        appendHaving(consumer);
        return this;
    }

    public <K> LinkedQueryWrapper orderBy(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        orderBy(tableId, column, true, false);
        return this;
    }

    public LinkedQueryWrapper orderBy(Integer tableIndex, String column) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        orderBy(tableId, column, true, false);
        return this;
    }

    public <K> LinkedQueryWrapper orderByDesc(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        orderBy(tableId, column, true, true);
        return this;
    }

    public LinkedQueryWrapper orderByDesc(Integer tableIndex, String column) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        orderBy(tableId, column, true, true);
        return this;
    }

    public <K> LinkedQueryWrapper thenBy(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        orderBy(tableId, column, false, false);
        return this;
    }

    public LinkedQueryWrapper thenBy(Integer tableIndex, String column) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        orderBy(tableId, column, false, false);
        return this;
    }

    public <K> LinkedQueryWrapper thenByDesc(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        Field field = ColumnUtils.getField(fn);
        String column = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        orderBy(tableId, column, false, true);
        return this;
    }

    public LinkedQueryWrapper thenByDesc(Integer tableIndex, String column) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        String tableId = tableInfo.getTableId();
        orderBy(tableId, column, false, true);
        return this;
    }

    public LinkedQueryWrapper limit(int limit) {
        appendLimit(limit);
        return this;
    }

    public LinkedQueryWrapper limit(int offset, int limit) {
        appendLimit(offset, limit);
        return this;
    }

    //endregion
}
