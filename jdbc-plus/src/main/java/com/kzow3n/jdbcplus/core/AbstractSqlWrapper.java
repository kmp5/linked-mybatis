package com.kzow3n.jdbcplus.core;

import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.kzow3n.jdbcplus.core.jdbc.MySqlRunner;
import com.kzow3n.jdbcplus.pojo.ColumnInfo;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import com.kzow3n.jdbcplus.utils.ColumnUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Sql构造器基本方法类
 *
 * @author owen
 * @since 2021/8/4
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class AbstractSqlWrapper extends SqlWrapperBase {

    protected List<TableInfo> tableInfos;
    protected List<TableInfo> selectAllTableInfos;
    protected List<TableInfo> parentTableInfos;
    protected List<ColumnInfo> columnInfos;
    protected List<String> groupColumns;
    protected StringBuffer sqlBuilder;
    protected StringBuffer orderBy;
    protected String sql;
    protected String having;
    protected boolean blnFormatSql;
    protected boolean blnDistinct;
    protected boolean blnWhere;
    protected boolean blnOr;
    protected boolean blnOpenBracket;
    protected List<Object> args;

    protected void init() {
        tableInfos = new ArrayList<>();
        selectAllTableInfos = new ArrayList<>();
        parentTableInfos = new ArrayList<>();
        sql = "";
        having = "";
        sqlBuilder = new StringBuffer();
        orderBy = new StringBuffer();
        columnInfos = new ArrayList<>();
        groupColumns = new ArrayList<>();
        this.args = new ArrayList<>();
        blnFormatSql = false;
        blnDistinct = false;
        blnWhere = false;
        blnOr = false;
        blnOpenBracket = false;
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

    protected String addColumnInfoByFields(TableInfo tableInfo, Field field1, boolean formatBeanColumn, Field field2, String columnFormat) {
        //不处理静态字段
        if (Modifier.isStatic(field1.getModifiers())) {
            return null;
        }

        String tableColumn = getColumn(tableInfo, field1);
        String beanColumn = field2.getName();
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setTableIndex(tableInfo.getTableIndex());
        columnInfo.setTableColumns(tableColumn);
        if (formatBeanColumn) {
            columnInfo.setBeanColumns(beanColumn);
        }
        if (!StringUtils.isBlank(columnFormat)) {
            columnInfo.setColumnFormat(columnFormat);
        }
        this.columnInfos.add(columnInfo);
        return tableColumn;
    }

    protected String addColumnInfoByField(TableInfo tableInfo, Field field, boolean formatBeanColumn, String beanColumn, String columnFormat) {
        //不处理静态字段
        if (Modifier.isStatic(field.getModifiers())) {
            return null;
        }

        String tableColumn = getColumn(tableInfo, field);
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

    protected void appendFrom(Class<?> clazz, String tableId) {
        String tableName = getTableNameByClass(clazz);
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

    protected void appendFrom(Consumer<SqlWrapper> consumer, String tableId) {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId(tableId);
        tableInfos.add(tableInfo);
        int tableIndex = tableInfos.indexOf(tableInfo);
        tableInfos.get(tableIndex).setTableIndex(tableIndex + 1);
        SqlWrapper sqlWrapper = new SqlWrapper();
        consumer.accept(sqlWrapper);
        sqlWrapper.formatSql();
        args.addAll(sqlWrapper.getArgs());
        if (blnDistinct) {
            blnDistinct = false;
            sqlBuilder.append("select distinct %s from ").append(String.format("(%s) %s ", sqlWrapper.getSql(), tableId));
        }
        else {
            sqlBuilder.append("select %s from ").append(String.format("(%s) %s ", sqlWrapper.getSql(), tableId));
        }
    }

    protected void appendJoin(Class<?> clazz, String tableId, String joinType) {
        String tableName = getTableNameByClass(clazz);
        TableInfo tableInfo = new TableInfo(tableId, clazz);
        tableInfos.add(tableInfo);
        int tableIndex = tableInfos.indexOf(tableInfo);
        tableInfos.get(tableIndex).setTableIndex(tableIndex + 1);
        sqlBuilder.append(String.format("%s %s %s ", joinType, tableName, tableId));
    }

    protected void appendJoin(Consumer<SqlWrapper> consumer, String tableId, String joinType) {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId(tableId);
        tableInfos.add(tableInfo);
        int tableIndex = tableInfos.indexOf(tableInfo);
        tableInfos.get(tableIndex).setTableIndex(tableIndex + 1);
        SqlWrapper sqlWrapper = new SqlWrapper();
        consumer.accept(sqlWrapper);
        sqlWrapper.formatSql();
        sqlBuilder.append(String.format("%s (%s) %s ", joinType, sqlWrapper.getSql(), tableId));
    }

    protected void appendOn(String tableId1, String column1, String tableId2, String column2) {
        sqlBuilder.append(String.format("on %s.%s = %s.%s ", tableId1, column1, tableId2, column2));
    }

    protected void formatFullSql() {
        if (blnFormatSql) {
            return;
        }
        blnFormatSql = true;
        sql = sqlBuilder.toString();
        if (!CollectionUtils.isEmpty(groupColumns)) {
            sql += String.format("group by %s ", String.join(",", groupColumns));
        }
        if (StringUtils.isNotBlank(having)) {
            sql += String.format("having %s ", having);
        }

        String columnString = formatColumns();
        sql = String.format(sql, columnString);
    }

    protected String formatColumns() {
        if (!CollectionUtils.isEmpty(selectAllTableInfos)) {
            for (TableInfo selectAllTableInfo : selectAllTableInfos) {
                Integer tableIndex = selectAllTableInfo.getTableIndex();
                Boolean formatBeanColumn = selectAllTableInfo.getFormatBeanColumn();
                TableInfo tableInfo = getTableInfoByIndex(tableIndex);
                Class<?> clazz = tableInfo.getTableClass();
                if (clazz == null) {
                    ColumnInfo columnInfo = new ColumnInfo();
                    columnInfo.setTableIndex(tableIndex);
                    columnInfos.add(columnInfo);
                    continue;
                }
                List<Field> fields = getAllFields(clazz);
                for (Field field : fields) {
                    addColumnInfoByField(tableInfo, field, formatBeanColumn, null, null);
                }
            }
        }
        if (CollectionUtils.isEmpty(columnInfos)) {
            return "*";
        }
        List<String> formats = new ArrayList<>();

        for (ColumnInfo columnInfo : columnInfos) {
            Integer tableIndex = columnInfo.getTableIndex();
            if (tableIndex == null) {
                formats.add(String.format("%s %s", columnInfo.getTableColumns(), columnInfo.getBeanColumns()));
                continue;
            }
            TableInfo tableInfo = getTableInfoByIndex(tableIndex);
            String tableId = tableInfo.getTableId();
            String tableColumns = columnInfo.getTableColumns();
            String beanColumns = columnInfo.getBeanColumns();
            String columnFormat = columnInfo.getColumnFormat();
            if (StringUtils.isBlank(tableColumns)) {
                if (StringUtils.isNotBlank(tableId)) {
                    formats.add(String.format("%s.*", tableId));
                }
                continue;
            }
            List<String> tableColumnList = Arrays.stream(tableColumns.split(",")).collect(Collectors.toList());
            List<String> beanColumnList;
            if (StringUtils.isBlank(beanColumns)) {
                beanColumnList = new ArrayList<>();
            }
            else {
                beanColumnList= Arrays.stream(beanColumns.split(",")).collect(Collectors.toList());
            }
            int beanColumnSize = beanColumnList.size();
            if (StringUtils.isBlank(columnFormat)) {
                for (int i = 0; i < tableColumnList.size(); i ++) {
                    String format;
                    if (StringUtils.isBlank(tableId)) {
                        format = tableColumnList.get(i);
                    }
                    else {
                        format = String.format("%s.%s", tableId, tableColumnList.get(i));
                    }
                    if (beanColumnSize > i) {
                        format += String.format(" %s", beanColumnList.get(i));
                    }
                    formats.add(format);
                }
            }
            else {
                for (int i = 0; i < tableColumnList.size(); i ++) {
                    String format;
                    if (StringUtils.isBlank(tableId)) {
                        format = tableColumnList.get(i);
                    }
                    else {
                        format = String.format("%s.%s", tableId, tableColumnList.get(i));
                    }
                    format = String.format(columnFormat, format);
                    if (beanColumnSize > i) {
                        format += String.format(" %s", beanColumnList.get(i));
                    }
                    formats.add(format);
                }
            }
        }
        return String.join(",", formats);
    }

    protected void isNull(String tableId, String column) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s is null ", tableId, column));
    }

    protected void isNotNull(String tableId, String column) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s is not null ", tableId, column));
    }

    protected void eq(String tableId, String column, Object arg, Consumer<SqlWrapper> consumer) {
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
            SqlWrapper sqlWrapper = new SqlWrapper();
            consumer.accept(sqlWrapper);
            sqlWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s = (%s) ", tableId, column, sqlWrapper.getSql()));
            args.addAll(sqlWrapper.getArgs());
        }
    }

    protected void eq(String tableId1, String column1, String tableId2, String column2) {
        spendOperator();
        sqlBuilder.append(String.format("%s.%s = %s.%s ", tableId1, column1, tableId2, column2));
    }


    protected void ne(String tableId, String column, Object arg, Consumer<SqlWrapper> consumer) {
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
            SqlWrapper sqlWrapper = new SqlWrapper();
            consumer.accept(sqlWrapper);
            sqlWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s <> (%s) ", tableId, column, sqlWrapper.getSql()));
            args.addAll(sqlWrapper.getArgs());
        }
    }

    protected void gt(String tableId, String column, Object arg, Consumer<SqlWrapper> consumer) {
        compare(tableId, column, arg, consumer, ">");
    }

    protected void ge(String tableId, String column, Object arg, Consumer<SqlWrapper> consumer) {
        compare(tableId, column, arg, consumer, ">=");
    }

    protected void lt(String tableId, String column, Object arg, Consumer<SqlWrapper> consumer) {
        compare(tableId, column, arg, consumer, "<");
    }

    protected void le(String tableId, String column, Object arg, Consumer<SqlWrapper> consumer) {
        compare(tableId, column, arg, consumer, "<=");
    }

    private void compare(String tableId, String column, Object arg, Consumer<SqlWrapper> consumer, String compareType) {
        spendOperator();
        if (consumer == null) {
            sqlBuilder.append(String.format("%s.%s %s ? ", tableId, column, compareType));
            args.add(arg);
        }
        else {
            SqlWrapper sqlWrapper = new SqlWrapper();
            consumer.accept(sqlWrapper);
            sqlWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s %s (%s) ", tableId, column, compareType, sqlWrapper.getSql()));
            args.addAll(sqlWrapper.getArgs());
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

    protected void in(String tableId, String column, List<?> args, Consumer<SqlWrapper> consumer) {
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
            SqlWrapper sqlWrapper = new SqlWrapper();
            consumer.accept(sqlWrapper);
            sqlWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s in (%s) ", tableId, column, sqlWrapper.getSql()));
            this.args.addAll(sqlWrapper.getArgs());
        }
    }

    protected void notIn(String tableId, String column, List<?> args, Consumer<SqlWrapper> consumer) {
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
            SqlWrapper sqlWrapper = new SqlWrapper();
            consumer.accept(sqlWrapper);
            sqlWrapper.formatSql();
            sqlBuilder.append(String.format("%s.%s not in (%s) ", tableId, column, sqlWrapper.getSql()));
            this.args.addAll(sqlWrapper.getArgs());
        }
    }

    protected void appendExists(String sql, Object... args) {
        spendOperator();
        if (args != null) {
            this.args.addAll(Arrays.asList(args));
        }
        sqlBuilder.append(String.format("exists(%s) ", sql));
    }

    protected void appendExists(Consumer<SqlWrapper> consumer) {
        spendOperator();
        SqlWrapper sqlWrapper = new SqlWrapper();
        sqlWrapper.setParentTableInfos(this.tableInfos);
        consumer.accept(sqlWrapper);
        sqlWrapper.formatSql();
        sqlBuilder.append(String.format("exists(%s) ", sqlWrapper.getSql()));
        this.args.addAll(sqlWrapper.getArgs());
    }

    protected void appendNotExists(String sql, Object... args) {
        spendOperator();
        if (args != null) {
            this.args.addAll(Arrays.asList(args));
        }
        sqlBuilder.append(String.format("not exists(%s) ", sql));
    }

    protected void appendNotExists(Consumer<SqlWrapper> consumer) {
        spendOperator();
        SqlWrapper sqlWrapper = new SqlWrapper();
        sqlWrapper.setParentTableInfos(this.tableInfos);
        consumer.accept(sqlWrapper);
        sqlWrapper.formatSql();
        sqlBuilder.append(String.format("not exists(%s) ", sqlWrapper.getSql()));
        this.args.addAll(sqlWrapper.getArgs());
    }

    @SafeVarargs
    protected final <K> void groupBy(TableInfo tableInfo, SFunction<K, ?>... fns) {
        String tableId = tableInfo.getTableId();
        for (SFunction<K, ?> fn : fns) {
            Field field = ColumnUtils.getField(fn);
            String tableColumn = addColumnInfoByField(tableInfo, field, true, null, null);
            groupColumns.add(String.format("%s.%s", tableId, tableColumn));
        }
    }

    protected final <K, M> void groupBy(TableInfo tableInfo, SFunction<K, ?> fn1, SFunction<M, ?> fn2) {
        String tableId = tableInfo.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        Field field2 = ColumnUtils.getField(fn2);
        String tableColumn = addColumnInfoByFields(tableInfo, field1, true, field2, null);
        groupColumns.add(String.format("%s.%s", tableId, tableColumn));
    }

    protected final <K> void groupBy(TableInfo tableInfo, SFunction<K, ?> fn1, String beanColumn) {
        String tableId = tableInfo.getTableId();
        Field field1 = ColumnUtils.getField(fn1);
        String tableColumn = addColumnInfoByField(tableInfo, field1, true, beanColumn, null);
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
        having = sql;
    }

    protected void appendHaving(Consumer<SqlWrapper> consumer) {
        SqlWrapper sqlWrapper = new SqlWrapper();
        sqlWrapper.setTableInfos(this.tableInfos);
        consumer.accept(sqlWrapper);
        sqlWrapper.formatSql();
        String having = sqlWrapper.getSql();
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

    protected long selectCount(SqlSession sqlSession) {
        formatFullSql();
        String sqlCount = String.format(sqlBuilder.toString(), "count(1) selectCount");
        log.info(sqlCount);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        Map<String, Object> map;
        try {
            map = sqlRunner.selectOne(sqlCount, args.toArray());
            return (long) map.get("selectCount");
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return 0;
    }

    protected List<Map<String, Object>> selectList(SqlSession sqlSession) {
        formatFullSql();
        sql += orderBy.toString();
        log.info(sql);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        List<Map<String, Object>> mapList = null;
        try {
            mapList = sqlRunner.selectAll(sql, args.toArray());
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }

    protected <T> List<Map<String, Object>> selectPage(SqlSession sqlSession, Page<T> page, int pageIndex, int pageSize) {
        int total = (int) selectCount(sqlSession);
        int pages = total % pageSize > 0 ? (total / pageSize) + 1 : total / pageSize;
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);

        formatFullSql();
        sql += orderBy.toString();
        sql += String.format(" limit %d,%d", (pageIndex - 1) * pageSize, pageSize);
        log.info(sql);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        List<Map<String, Object>> mapList = null;
        try {
            mapList = sqlRunner.selectAll(sql, args.toArray());
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }

    protected List<Map<String, Object>> execPro(SqlSession sqlSession, String proName, Object... args) {
        init();
        sqlBuilder.append(String.format("call %s", proName));
        if (args != null) {
            List<String> formats = new ArrayList<>();
            for (int i = 0;i < args.length;i ++) {
                formats.add("?");
            }
            sqlBuilder.append(String.format("(%s)", String.join(",", formats)));
        }
        sql = sqlBuilder.toString();
        log.info(sql);
        List<Map<String, Object>> mapList = null;
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        try {
            mapList = sqlRunner.selectAll(sql, args);
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }
}
