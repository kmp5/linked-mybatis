package com.kzow3n.jdbcplus.core.condition;

import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import com.kzow3n.jdbcplus.utils.ColumnUtils;

import java.lang.reflect.Field;

/**
 * 聚合Column构造器
 *
 * @author owen
 * @since 2021/8/10
 */
public class AggregateWrapper extends AbstractAggregateWrapper {

    public <K> void count(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field = ColumnUtils.getField(fn);
        String tableColumn = getColumn(tableInfo, field);
        column = String.format("count(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public void count(Integer tableIndex, String tableColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        column = String.format("count(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public <K> void avg(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field = ColumnUtils.getField(fn);
        String tableColumn = getColumn(tableInfo, field);
        column = String.format("avg(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public void avg(Integer tableIndex, String tableColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        column = String.format("avg(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public <K> void sum(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field = ColumnUtils.getField(fn);
        String tableColumn = getColumn(tableInfo, field);
        column = String.format("sum(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public void sum(Integer tableIndex, String tableColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        column = String.format("sum(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public <K> void max(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field = ColumnUtils.getField(fn);
        String tableColumn = getColumn(tableInfo, field);
        column = String.format("max(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public void max(Integer tableIndex, String tableColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        column = String.format("max(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public <K> void min(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field = ColumnUtils.getField(fn);
        String tableColumn = getColumn(tableInfo, field);
        column = String.format("min(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public void min(Integer tableIndex, String tableColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        column = String.format("min(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public <K> void groupConcat(Integer tableIndex, SFunction<K, ?> fn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        Field field = ColumnUtils.getField(fn);
        String tableColumn = getColumn(tableInfo, field);
        column = String.format("group_concat(%s.%s)", tableInfo.getTableId(), tableColumn);
    }

    public void groupConcat(Integer tableIndex, String tableColumn) {
        TableInfo tableInfo = getTableInfoByIndex(tableIndex);
        column = String.format("group_concat(%s.%s)", tableInfo.getTableId(), tableColumn);
    }
}
