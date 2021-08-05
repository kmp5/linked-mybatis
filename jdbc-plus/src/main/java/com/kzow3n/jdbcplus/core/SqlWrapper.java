package com.kzow3n.jdbcplus.core;

import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.primitives.Ints;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import com.kzow3n.jdbcplus.utils.ColumnUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Sql构造器
 *
 * @author owen
 * @since 2021/8/4
 */
@Slf4j
public class SqlWrapper extends AbstractSqlWrapper {

    public SqlWrapper() {
        init();
    }

    //region 拼接基本Sql

    public SqlWrapper distinct() {
        blnDistinct = true;
        return this;
    }

    public SqlWrapper selectAll(Integer... tableIndexes) {
        for (Integer tableIndex : tableIndexes) {
            TableInfo selectAllTableInfo = new TableInfo(tableIndex, true);
            selectAllTableInfos.add(selectAllTableInfo);
        }
        return this;
    }

    @SafeVarargs
    public final <K> SqlWrapper select(Integer tableIndex, SFunction<K, ?>... fns) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        for (SFunction<K, ?> fn : fns) {
            Field field = ColumnUtils.getField(fn);
            addColumnInfoByField(tableInfo, field, true, null, null);
        }
        return this;
    }

    public final <K, M> SqlWrapper select(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, null);
        return this;
    }

    public final <K> SqlWrapper select(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, null);
        return this;
    }

    public final <M> SqlWrapper select(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, null);
        return this;
    }

    public SqlWrapper select(Integer tableIndex, String tableColumns, @Nullable String beanColumns) {
        addColumnInfo(tableIndex, tableColumns, beanColumns, null);
        return this;
    }

    public SqlWrapper from(Class<?> clazz, String tableId) {
        appendFrom(clazz, tableId);
        return this;
    }

    public SqlWrapper from(Consumer<SqlWrapper> consumer, String tableId) {
        appendFrom(consumer, tableId);
        return this;
    }

    public SqlWrapper innerJoin(Class<?> clazz, String tableId) {
        appendJoin(clazz, tableId, "inner join");
        return this;
    }

    public SqlWrapper innerJoin(Consumer<SqlWrapper> consumer, String tableId) {
        appendJoin(consumer, tableId, "inner join");
        return this;
    }

    public SqlWrapper leftJoin(Class<?> clazz, String tableId) {
        appendJoin(clazz, tableId, "left join");
        return this;
    }

    public SqlWrapper leftJoin(Consumer<SqlWrapper> consumer, String tableId) {
        appendJoin(consumer, tableId, "left join");
        return this;
    }

    public SqlWrapper rightJoin(Class<?> clazz, String tableId) {
        appendJoin(clazz, tableId, "right join");
        return this;
    }

    public SqlWrapper rightJoin(Consumer<SqlWrapper> consumer, String tableId) {
        appendJoin(consumer, tableId, "right join");
        return this;
    }

    public SqlWrapper fullJoin(Class<?> clazz, String tableId) {
        appendJoin(clazz, tableId, "full join");
        return this;
    }

    public SqlWrapper fullJoin(Consumer<SqlWrapper> consumer, String tableId) {
        appendJoin(consumer, tableId, "full join");
        return this;
    }

    public <K, M> SqlWrapper on(Integer tableIndex1, SFunction<K, ?> fn1, Integer tableIndex2, SFunction<M, ?> fn2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        TableInfo tableInfo2 = getTableInfoByIndex(tableIndex2);
        String tableId1 = tableInfo1.getTableId();
        String tableId2 = tableInfo2.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        String column1 = getColumn(tableInfo1, field1);
        Field field2 = ColumnUtils.getField(fn2);
        String column2 = getColumn(tableInfo2, field2);
        appendOn(tableId1, column1, tableId2, column2);
        return this;
    }

    public <K> SqlWrapper on(Integer tableIndex1, SFunction<K, ?> fn1, Integer tableIndex2, String column2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        TableInfo tableInfo2 = getTableInfoByIndex(tableIndex2);
        String tableId1 = tableInfo1.getTableId();
        String tableId2 = tableInfo2.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        String column1 = getColumn(tableInfo1, field1);
        appendOn(tableId1, column1, tableId2, column2);
        return this;
    }

    public <M> SqlWrapper on(Integer tableIndex1, String column1, Integer tableIndex2, SFunction<M, ?> fn2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        TableInfo tableInfo2 = getTableInfoByIndex(tableIndex2);
        String tableId1 = tableInfo1.getTableId();
        String tableId2 = tableInfo2.getTableId();
        Field field2 = ColumnUtils.getField(fn2);
        String column2 = getColumn(tableInfo2, field2);
        appendOn(tableId1, column1, tableId2, column2);
        return this;
    }

    public SqlWrapper on(Integer tableIndex1, String column1, Integer tableIndex2, String column2) {
        TableInfo tableInfo1 = getTableInfoByIndex(tableIndex1);
        TableInfo tableInfo2 = getTableInfoByIndex(tableIndex2);
        String tableId1 = tableInfo1.getTableId();
        String tableId2 = tableInfo2.getTableId();
        appendOn(tableId1, column1, tableId2, column2);
        return this;
    }

    public final <K> SqlWrapper count(SFunction<K, ?> fn) {
        Field field = ColumnUtils.getField(fn);
        String beanColumn = field.getName();
        addColumnInfo(null, "count(*)", beanColumn, null);
        return this;
    }

    public SqlWrapper count(String beanColumn) {
        addColumnInfo(null, "count(*)", beanColumn, null);
        return this;
    }

    public final <K, M> SqlWrapper avg(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "avg(%s)");
        return this;
    }

    public final <K> SqlWrapper avg(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "avg(%s)");
        return this;
    }

    public final <M> SqlWrapper avg(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "avg(%s)");
        return this;
    }

    public SqlWrapper avg(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "avg(%s)");
        return this;
    }

    public final <K, M> SqlWrapper sum(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "sum(%s)");
        return this;
    }

    public final <K> SqlWrapper sum(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "sum(%s)");
        return this;
    }

    public final <M> SqlWrapper sum(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "sum(%s)");
        return this;
    }

    public SqlWrapper sum(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "sum(%s)");
        return this;
    }

    public final <K, M> SqlWrapper max(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "max(%s)");
        return this;
    }

    public final <K> SqlWrapper max(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "max(%s)");
        return this;
    }

    public final <M> SqlWrapper max(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "max(%s)");
        return this;
    }

    public SqlWrapper max(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "max(%s)");
        return this;
    }

    public final <K, M> SqlWrapper min(Integer tableIndex, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        addColumnInfoByFields(tableInfo, field1, true, field2, "min(%s)");
        return this;
    }

    public final <K> SqlWrapper min(Integer tableIndex, SFunction<K, ?> fn1, String beanColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field1 = ColumnUtils.getField(fn1);
        addColumnInfoByField(tableInfo, field1, true, beanColumn, "min(%s)");
        return this;
    }

    public final <M> SqlWrapper min(Integer tableIndex, String tableColumn, SFunction<M, ?> fn2) {
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableIndex, tableColumn, beanColumn, "min(%s)");
        return this;
    }

    public SqlWrapper min(Integer tableIndex, String tableColumn, String beanColumn) {
        addColumnInfo(tableIndex, tableColumn, beanColumn, "min(%s)");
        return this;
    }

    public SqlWrapper formatSql() {
        formatFullSql();
        return this;
    }

    //endregion

    //region 条件构造器

    public SqlWrapper or() {
        blnOr = true;
        return this;
    }

    public SqlWrapper or(Consumer<SqlWrapper> consumer) {
        blnOr = true;
        spendOperator();
        sqlBuilder.append("(");
        blnOpenBracket = true;
        consumer.accept(this);
        sqlBuilder.append(") ");
        return this;
    }

    public SqlWrapper and(Consumer<SqlWrapper> consumer) {
        spendOperator();
        sqlBuilder.append("(");
        blnOpenBracket = true;
        consumer.accept(this);
        sqlBuilder.append(") ");
        return this;
    }

    //endregion

    //region 查询器

    public long queryForCount(JdbcTemplate jdbcTemplate) {
        String sqlCount;
        if (CollectionUtils.isEmpty(columnInfos)) {
            sqlCount = String.format(sqlBuilder.toString(), "count(*) selectCount");
        }
        else {
            sqlCount = String.format("select count(*) selectCount from (%s) t", sql);
        }
        log.info(sqlCount);
        int[] typeArr = Ints.toArray(argTypes);
        Map<String, Object> map = jdbcTemplate.queryForMap(sqlCount, args.toArray(), typeArr);
        return (long) map.get("selectCount");
    }

    public <T> T queryForObject(Class<T> clazz, JdbcTemplate jdbcTemplate) {
        sql += orderBy.toString();
        log.info(sql);
        int[] typeArr = Ints.toArray(argTypes);
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql, args.toArray(), typeArr);
        Map<String, Object> map = mapList.stream().findFirst().orElse(null);
        if (map == null) {
            return null;
        }
        updateMap(map, clazz);
        return mapToBean(map, clazz);
    }

    public Map<String, Object> queryForMap(JdbcTemplate jdbcTemplate) {
        sql += orderBy.toString();
        log.info(sql);
        int[] typeArr = Ints.toArray(argTypes);
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql, args.toArray(), typeArr);
        return mapList.stream().findFirst().orElse(null);
    }

    public <T> List<T> queryForObjects(Class<T> clazz, JdbcTemplate jdbcTemplate) {
        sql += orderBy.toString();
        log.info(sql);
        int[] typeArr = Ints.toArray(argTypes);
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql, args.toArray(), typeArr);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        updateMapList(mapList, clazz);
        return mapsToBeans(mapList, clazz);
    }

    public List<Map<String, Object>> queryForMaps(JdbcTemplate jdbcTemplate) {
        sql += orderBy.toString();
        log.info(sql);
        int[] typeArr = Ints.toArray(argTypes);
        return jdbcTemplate.queryForList(sql, args.toArray(), typeArr);
    }

    public <T> Page<T> queryForObjectPage(Class<T> clazz, JdbcTemplate jdbcTemplate, int pageIndex, int pageSize) {
        int total = (int) queryForCount(jdbcTemplate);
        String orderBy = this.orderBy.toString();
        if (!StringUtils.isBlank(orderBy)) {
            sql += orderBy;
        }
        int pages = total % pageSize > 0 ? (total / pageSize) + 1 : total / pageSize;
        Page<T> page = new Page<>();
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);
        sql += String.format(" LIMIT %d,%d", (pageIndex - 1) * pageSize, pageSize);
        log.info(sql);
        int[] typeArr = Ints.toArray(argTypes);
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql, args.toArray(), typeArr);
        if (CollectionUtils.isEmpty(mapList)) {
            return page;
        }
        updateMapList(mapList, clazz);
        List<T> list = mapsToBeans(mapList, clazz);
        page.setRecords(list);
        return page;
    }

    public Page<Map<String, Object>> queryForMapPage(JdbcTemplate jdbcTemplate, int pageIndex, int pageSize) {
        int total = (int) queryForCount(jdbcTemplate);
        String orderBy = this.orderBy.toString();
        if (!StringUtils.isBlank(orderBy)) {
            sql += orderBy;
        }
        int pages = total % pageSize > 0 ? (total / pageSize) + 1 : total / pageSize;
        Page<Map<String, Object>> page = new Page<>();
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);
        sql += String.format(" LIMIT %d,%d", (pageIndex - 1) * pageSize, pageSize);
        log.info(sql);
        int[] typeArr = Ints.toArray(argTypes);
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql, args.toArray(), typeArr);
        page.setRecords(mapList);
        return page;
    }

    //endregion

}
