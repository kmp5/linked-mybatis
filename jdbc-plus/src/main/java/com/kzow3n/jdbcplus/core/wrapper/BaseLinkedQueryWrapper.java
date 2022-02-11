package com.kzow3n.jdbcplus.core.wrapper;

import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.kzow3n.jdbcplus.core.wrapper.column.AggregateWrapper;
import com.kzow3n.jdbcplus.pojo.ColumnInfo;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import com.kzow3n.jdbcplus.utils.ClazzUtils;
import com.kzow3n.jdbcplus.utils.ColumnUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.Configuration;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Sql构造器基本方法类
 *
 * @author owen
 * @since 2021/8/4
 */
@Data
public class BaseLinkedQueryWrapper {

    protected List<TableInfo> tableInfos;
    protected List<TableInfo> selectAllTableInfos;
    protected List<TableInfo> parentTableInfos;
    protected StringBuilder sqlBuilder;
    protected List<ColumnInfo> columnInfos;
    protected List<String> groupColumns;
    protected String having;
    protected StringBuilder orderBy;
    protected Integer limit;
    protected Integer offset;
    protected String baseSql;
    protected String fullSql;
    protected List<Object> args;
    protected boolean blnFormatSql;
    protected boolean blnDistinct;
    protected boolean blnWhere;
    protected boolean blnOr;
    protected boolean blnOpenBracket;

    protected boolean mapUnderscoreToCamelCase = true;

    protected void init() {
        tableInfos = new ArrayList<>();
        selectAllTableInfos = new ArrayList<>();
        parentTableInfos = new ArrayList<>();
        sqlBuilder = new StringBuilder();
        columnInfos = new LinkedList<>();
        groupColumns = new LinkedList<>();
        orderBy = new StringBuilder();
        args = new LinkedList<>();
        blnFormatSql = false;
        blnDistinct = false;
        blnWhere = false;
        blnOr = false;
        blnOpenBracket = false;
    }

    protected void initConfiguration(Configuration configuration) {
        if (configuration == null) {
            return;
        }
        mapUnderscoreToCamelCase = configuration.isMapUnderscoreToCamelCase();
    }

    protected AggregateWrapper buildAggregateWrapper() {
        AggregateWrapper aggregateWrapper = new AggregateWrapper();
        aggregateWrapper.setTableInfos(tableInfos);
        aggregateWrapper.setMapUnderscoreToCamelCase(mapUnderscoreToCamelCase);
        return aggregateWrapper;
    }

    protected void spendOperator() {
        if (!blnWhere) {
            sqlBuilder.append("where ");
            blnWhere = true;
            return;
        }
        if (blnOpenBracket) {
            blnOpenBracket = false;
            return;
        }
        if (blnOr) {
            sqlBuilder.append("or ");
            blnOr = false;
            return;
        }
        sqlBuilder.append("and ");
    }

    protected TableInfo getTableInfoByIndex(Integer tableIndex) {
        if (tableIndex == null) {
            return null;
        }
        if (tableIndex < 1) {
            return null;
        }
        return tableInfos.get(tableIndex - 1);
    }

    protected TableInfo getTableInfoById(List<TableInfo> tableInfos, String tableId) {
        if (StringUtils.isBlank(tableId)) {
            return null;
        }
        return tableInfos.stream().filter(s -> s.getTableId().equals(tableId)).findFirst().orElse(null);
    }

    protected void addColumnInfo(Integer tableIndex, String tableColumns, String beanColumns, String columnFormat) {
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setTableIndex(tableIndex);
        columnInfo.setTableColumns(tableColumns);
        columnInfo.setBeanColumns(beanColumns);
        columnInfo.setColumnFormat(columnFormat);
        this.columnInfos.add(columnInfo);
    }

    protected String addColumnInfo(TableInfo tableInfo, Field field, boolean formatBeanColumn, String beanColumn, String columnFormat) {
        //不处理静态字段
        if (Modifier.isStatic(field.getModifiers())) {
            return null;
        }

        String tableColumn = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setTableIndex(tableInfo.getTableIndex());
        columnInfo.setTableColumns(tableColumn);
        if (formatBeanColumn) {
            if (StringUtils.isBlank(beanColumn)) {
                beanColumn = field.getName();
            }
            columnInfo.setBeanColumns(beanColumn);
        }
        if (!StringUtils.isBlank(columnFormat)) {
            columnInfo.setColumnFormat(columnFormat);
        }
        this.columnInfos.add(columnInfo);
        return tableColumn;
    }

    protected void addColumnInfo(Integer tableIndex, Field field, boolean formatBeanColumn, String beanColumn, String columnFormat) {
        //不处理静态字段
        if (Modifier.isStatic(field.getModifiers())) {
            return;
        }

        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setTableIndex(tableIndex);
        columnInfo.setField(field);
        if (formatBeanColumn) {
            if (StringUtils.isBlank(beanColumn)) {
                beanColumn = field.getName();
            }
            columnInfo.setBeanColumns(beanColumn);
        }
        if (!StringUtils.isBlank(columnFormat)) {
            columnInfo.setColumnFormat(columnFormat);
        }
        this.columnInfos.add(columnInfo);
    }

    protected void appendFrom(Class<?> clazz, String tableId) {
        String tableName = ClazzUtils.getTableName(clazz, mapUnderscoreToCamelCase);
        TableInfo tableInfo = new TableInfo(tableId, clazz);
        tableInfos.add(tableInfo);
        int tableIndex = tableInfos.indexOf(tableInfo);
        tableInfos.get(tableIndex).setTableIndex(tableIndex + 1);
        if (blnDistinct) {
            blnDistinct = false;
            sqlBuilder.append("select distinct %s from ").append(String.format("%s %s ", tableName, tableId));
        }
        else {
            sqlBuilder.append("select %s from ").append(String.format("%s %s ", tableName, tableId));
        }
    }

    protected void appendFrom(Consumer<LinkedQueryWrapper> consumer, String tableId) {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId(tableId);
        tableInfos.add(tableInfo);
        int tableIndex = tableInfos.indexOf(tableInfo);
        tableInfos.get(tableIndex).setTableIndex(tableIndex + 1);
        LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
        consumer.accept(linkedQueryWrapper);
        linkedQueryWrapper.formatSql();
        args.addAll(linkedQueryWrapper.getArgs());
        if (blnDistinct) {
            blnDistinct = false;
            sqlBuilder.append("select distinct %s from ").append(String.format("(%s) %s ", linkedQueryWrapper.getFullSql(), tableId));
        }
        else {
            sqlBuilder.append("select %s from ").append(String.format("(%s) %s ", linkedQueryWrapper.getFullSql(), tableId));
        }
    }

    protected void appendJoin(Class<?> clazz, String tableId, String joinType) {
        String tableName = ClazzUtils.getTableName(clazz, mapUnderscoreToCamelCase);
        TableInfo tableInfo = new TableInfo(tableId, clazz);
        tableInfos.add(tableInfo);
        int tableIndex = tableInfos.indexOf(tableInfo);
        tableInfos.get(tableIndex).setTableIndex(tableIndex + 1);
        sqlBuilder.append(String.format("%s %s %s ", joinType, tableName, tableId));
    }

    protected void appendJoin(Consumer<LinkedQueryWrapper> consumer, String tableId, String joinType) {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId(tableId);
        tableInfos.add(tableInfo);
        int tableIndex = tableInfos.indexOf(tableInfo);
        tableInfos.get(tableIndex).setTableIndex(tableIndex + 1);
        LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
        consumer.accept(linkedQueryWrapper);
        linkedQueryWrapper.formatSql();
        args.addAll(linkedQueryWrapper.getArgs());
        sqlBuilder.append(String.format("%s (%s) %s ", joinType, linkedQueryWrapper.getFullSql(), tableId));
    }

    protected void appendOn(String tableId1, String column1, String tableId2, String column2) {
        sqlBuilder.append(String.format("on %s.%s = %s.%s ", tableId1, column1, tableId2, column2));
    }

    protected void formatFullSql() {
        if (blnFormatSql) {
            return;
        }
        blnFormatSql = true;
        StringBuilder baseSqlBuilder = new StringBuilder();
        baseSqlBuilder.append(sqlBuilder.toString());
        if (!CollectionUtils.isEmpty(groupColumns)) {
            baseSqlBuilder.append(String.format("group by %s ", String.join(",", groupColumns)));
            if (StringUtils.isNotBlank(having)) {
                baseSqlBuilder.append(having);
            }
        }
        String columnString = formatColumns();
        baseSql = String.format(baseSqlBuilder.toString(), columnString);

        StringBuilder fullSqlBuilder = new StringBuilder();
        fullSqlBuilder.append(baseSql).append(orderBy.toString());
        if (limit != null) {
            String paging;
            if (offset != null) {
                paging = String.format(" limit %d offset %d", limit, offset);
            }
            else {
                paging = String.format(" limit %d", limit);
            }
            fullSqlBuilder.append(paging);
        }
        fullSql = fullSqlBuilder.toString();
    }

    protected String formatColumns() {
        addAllTableColumns();
        if (CollectionUtils.isEmpty(columnInfos)) {
            return "*";
        }
        List<String> formats = new ArrayList<>();

        for (ColumnInfo columnInfo : columnInfos) {
            Integer tableIndex = columnInfo.getTableIndex();
            if (tableIndex == null) {
                continue;
            }
            TableInfo tableInfo = getTableInfoByIndex(tableIndex);
            String tableId = tableInfo.getTableId();
            String tableColumns = columnInfo.getTableColumns();
            String beanColumns = columnInfo.getBeanColumns();
            String columnFormat = columnInfo.getColumnFormat();
            Field field = columnInfo.getField();
            if (StringUtils.isBlank(tableColumns)) {
                if (field == null) {
                    continue;
                }
                tableColumns = ColumnUtils.getColumn(tableInfo, field, mapUnderscoreToCamelCase);
            }
            List<String> tableColumnList = Arrays.stream(tableColumns.split(",")).collect(Collectors.toList());
            List<String> beanColumnList = StringUtils.isBlank(beanColumns) ?
                    new ArrayList<>() : Arrays.stream(beanColumns.split(",")).collect(Collectors.toList());
            int beanColumnSize = beanColumnList.size();
            boolean blnTableIdBlank = StringUtils.isBlank(tableId);
            boolean blnColumnFormatBlank = StringUtils.isBlank(columnFormat);
            for (int i = 0; i < tableColumnList.size(); i ++) {
                String tableColumn = tableColumnList.get(i);
                String format = blnTableIdBlank ?
                        tableColumn : String.format("%s.%s", tableId, tableColumn);
                if (blnColumnFormatBlank) {
                    if (beanColumnSize > i) {
                        String beanColumn = beanColumnList.get(i);
                        if (!tableColumn.equals(beanColumn)) {
                            format += String.format(" as %s", beanColumn);
                        }
                    }
                }
                else {
                    format = String.format(columnFormat, format);
                    if (beanColumnSize > i) {
                        format += String.format(" as %s", beanColumnList.get(i));
                    }
                }
                formats.add(format);
            }
        }
        return String.join(",", formats);
    }

    private void addAllTableColumns() {
        if (CollectionUtils.isEmpty(selectAllTableInfos)) {
            return;
        }
        for (TableInfo selectAllTableInfo : selectAllTableInfos) {
            Integer tableIndex = selectAllTableInfo.getTableIndex();
            Boolean formatBeanColumn = selectAllTableInfo.getFormatBeanColumn();
            TableInfo tableInfo = getTableInfoByIndex(tableIndex);
            Class<?> clazz = tableInfo.getTableClass();
            if (clazz == null) {
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setTableIndex(tableIndex);
                columnInfo.setTableColumns("*");
                columnInfos.add(columnInfo);
                continue;
            }
            List<Field> fields = ClazzUtils.getAllFields(clazz);
            for (Field field : fields) {
                addColumnInfo(tableInfo, field, formatBeanColumn, null, null);
            }
        }
    }

    protected void isNull(String tableId, String column) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s is null ", tableId, column));
    }

    protected void isNotNull(String tableId, String column) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s is not null ", tableId, column));
    }

    protected void eq(String tableId, String column, Object arg, Consumer<LinkedQueryWrapper> consumer) {
        spendOperator();
        if (consumer == null) {
            if (arg != null) {
                sqlBuilder.append(String.format("%s.%s = ? ", tableId, column));
                args.add(arg);
            }
            else {
                sqlBuilder.append(String.format("%s.%s is null ", tableId, column));
            }
        }
        else {
            LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
            consumer.accept(linkedQueryWrapper);
            linkedQueryWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s = (%s) ", tableId, column, linkedQueryWrapper.getFullSql()));
            args.addAll(linkedQueryWrapper.getArgs());
        }
    }

    protected void eq(String tableId1, String column1, String tableId2, String column2) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s = %s.%s ", tableId1, column1, tableId2, column2));
    }


    protected void ne(String tableId, String column, Object arg, Consumer<LinkedQueryWrapper> consumer) {
        spendOperator();
        if (consumer == null) {
            if (arg != null) {
                sqlBuilder.append(String.format("%s.%s <> ? ", tableId, column));
                args.add(arg);
            }
            else {
                sqlBuilder.append(String.format("%s.%s is not null ", tableId, column));
            }
        }
        else {
            LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
            consumer.accept(linkedQueryWrapper);
            linkedQueryWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s <> (%s) ", tableId, column, linkedQueryWrapper.getFullSql()));
            args.addAll(linkedQueryWrapper.getArgs());
        }
    }

    protected void gt(String tableId, String column, Object arg, Consumer<LinkedQueryWrapper> consumer) {
        compare(tableId, column, arg, consumer, ">");
    }

    protected void ge(String tableId, String column, Object arg, Consumer<LinkedQueryWrapper> consumer) {
        compare(tableId, column, arg, consumer, ">=");
    }

    protected void lt(String tableId, String column, Object arg, Consumer<LinkedQueryWrapper> consumer) {
        compare(tableId, column, arg, consumer, "<");
    }

    protected void le(String tableId, String column, Object arg, Consumer<LinkedQueryWrapper> consumer) {
        compare(tableId, column, arg, consumer, "<=");
    }

    private void compare(String tableId, String column, Object arg, Consumer<LinkedQueryWrapper> consumer, String compareType) {
        spendOperator();
        if (consumer == null) {
            sqlBuilder.append(String.format("%s.%s %s ? ", tableId, column, compareType));
            args.add(arg);
        }
        else {
            LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
            consumer.accept(linkedQueryWrapper);
            linkedQueryWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s %s (%s) ", tableId, column, compareType, linkedQueryWrapper.getFullSql()));
            args.addAll(linkedQueryWrapper.getArgs());
        }
    }

    protected void like(String tableId, String column, String arg) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s like concat(?, ?, ?) ", tableId, column));
        args.add("%");
        args.add(arg);
        args.add("%");
    }

    protected void likeLeft(String tableId, String column, String arg) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s like concat(?, ?) ", tableId, column));
        args.add("%");
        args.add(arg);
    }

    protected void likeRight(String tableId, String column, String arg) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s like concat(?, ?) ", tableId, column));
        args.add(arg);
        args.add("%");
    }

    protected void notLike(String tableId, String column, String arg) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s not like concat(?, ?, ?) ", tableId, column));
        args.add("%");
        args.add(arg);
        args.add("%");
    }

    protected void between(String tableId, String column, Object arg1, Object arg2) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s between ? and ? ", tableId, column));
        args.add(arg1);
        args.add(arg2);
    }

    protected void notBetween(String tableId, String column, Object arg1, Object arg2) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s not between ? and ? ", tableId, column));
        args.add(arg1);
        args.add(arg2);
    }

    protected void in(String tableId, String column, List<?> args, Consumer<LinkedQueryWrapper> consumer) {
        spendOperator();
        if (consumer == null) {
            this.args.addAll(args);
            List<String> params = new ArrayList<>();
            for (int i = 0; i < args.size(); i ++) {
                params.add("?");
            }
            sqlBuilder.append(String.format("%s.%s in (%s) ", tableId, column, String.join(",", params)));
        }
        else {
            LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
            consumer.accept(linkedQueryWrapper);
            linkedQueryWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s in (%s) ", tableId, column, linkedQueryWrapper.getFullSql()));
            this.args.addAll(linkedQueryWrapper.getArgs());
        }
    }

    protected void notIn(String tableId, String column, List<?> args, Consumer<LinkedQueryWrapper> consumer) {
        spendOperator();
        if (consumer == null) {
            this.args.addAll(args);
            List<String> params = new ArrayList<>();
            for (int i = 0; i < args.size(); i ++) {
                params.add("?");
            }
            sqlBuilder.append(String.format("%s.%s not in (%s) ", tableId, column, String.join(",", params)));
        }
        else {
            LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
            consumer.accept(linkedQueryWrapper);
            linkedQueryWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s not in (%s) ", tableId, column, linkedQueryWrapper.getFullSql()));
            this.args.addAll(linkedQueryWrapper.getArgs());
        }
    }

    protected void appendExists(String sql, Object... args) {
        spendOperator();
        if (args != null) {
            this.args.addAll(Arrays.asList(args));
        }
        sqlBuilder.append(String.format("exists(%s) ", sql));
    }

    protected void appendExists(Consumer<LinkedQueryWrapper> consumer) {
        spendOperator();
        LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
        linkedQueryWrapper.setParentTableInfos(this.tableInfos);
        consumer.accept(linkedQueryWrapper);
        linkedQueryWrapper.formatSql();
        sqlBuilder.append(String.format("exists(%s) ", linkedQueryWrapper.getFullSql()));
        this.args.addAll(linkedQueryWrapper.getArgs());
    }

    protected void appendNotExists(String sql, Object... args) {
        spendOperator();
        if (args != null) {
            this.args.addAll(Arrays.asList(args));
        }
        sqlBuilder.append(String.format("not exists(%s) ", sql));
    }

    protected void appendNotExists(Consumer<LinkedQueryWrapper> consumer) {
        spendOperator();
        LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
        linkedQueryWrapper.setParentTableInfos(this.tableInfos);
        consumer.accept(linkedQueryWrapper);
        linkedQueryWrapper.formatSql();
        sqlBuilder.append(String.format("not exists(%s) ", linkedQueryWrapper.getFullSql()));
        this.args.addAll(linkedQueryWrapper.getArgs());
    }

    @SafeVarargs
    protected final <K> void groupBy(TableInfo tableInfo, SFunction<K, ?>... fns) {
        String tableId = tableInfo.getTableId();
        for (SFunction<K, ?> fn : fns) {
            Field field = ColumnUtils.getField(fn);
            String tableColumn = addColumnInfo(tableInfo, field, true, null, null);
            groupColumns.add(String.format("%s.%s", tableId, tableColumn));
        }
    }

    protected final <K, M> void groupBy(TableInfo tableInfo, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        String tableId = tableInfo.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        String tableColumn = addColumnInfo(tableInfo, field1, true, beanColumn, null);
        groupColumns.add(String.format("%s.%s", tableId, tableColumn));
    }

    protected final <K> void groupBy(TableInfo tableInfo, SFunction<K, ?> fn1, String beanColumn) {
        String tableId = tableInfo.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        String tableColumn = addColumnInfo(tableInfo, field1, true, beanColumn, null);
        groupColumns.add(String.format("%s.%s", tableId, tableColumn));
    }

    protected final <M> void groupBy(TableInfo tableInfo, String tableColumn, SFunction<M, ?> fn2) {
        String tableId = tableInfo.getTableId();
        Field field2 = ColumnUtils.getField(fn2);
        String beanColumn = field2.getName();
        addColumnInfo(tableInfo.getTableIndex(), tableColumn, beanColumn, null);
        groupColumns.add(String.format("%s.%s", tableId, tableColumn));
    }

    protected void groupBy(TableInfo tableInfo, String tableColumn, String beanColumn) {
        String tableId = tableInfo.getTableId();
        addColumnInfo(tableInfo.getTableIndex(), tableColumn, beanColumn, null);
        groupColumns.add(String.format("%s.%s", tableId, tableColumn));
    }

    protected void appendHaving(String sql, Object... args) {
        if (args != null) {
            this.args.addAll(Arrays.asList(args));
        }
        having = String.format("having %s ", sql);
    }

    protected void appendHaving(Consumer<LinkedQueryWrapper> consumer) {
        LinkedQueryWrapper linkedQueryWrapper = new LinkedQueryWrapper();
        linkedQueryWrapper.setTableInfos(this.tableInfos);
        consumer.accept(linkedQueryWrapper);
        linkedQueryWrapper.formatSql();
        String having = linkedQueryWrapper.getFullSql();
        this.having = having.replaceFirst("where", "having");
    }

    protected void orderBy(String tableId, String column, boolean blnFirst, boolean blnDesc) {
        if (blnFirst) {
            orderBy.append(String.format("order by %s.%s", tableId, column));
        }
        else {
            orderBy.append(String.format(",%s.%s", tableId, column));
        }
        if (blnDesc) {
            orderBy.append(" desc");
        }
    }
}
