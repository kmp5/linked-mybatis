package com.kzow3n.jdbcplus.core;

import com.kzow3n.jdbcplus.pojo.ColumnInfo;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Sql构筑器基本方法类
 *
 * @author owen
 * @since 2021/8/4
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class AbstractSqlWrapper extends SqlWrapperBase {

    protected List<TableInfo> tableInfos;
    protected List<TableInfo> selectAllTableInfos;
    protected List<ColumnInfo> columnInfos;
    protected List<String> groupColumns;
    protected StringBuffer sqlBuilder;
    protected StringBuffer orderBy;
    protected String sql;
    protected String having;
    protected boolean blnDistinct;
    protected boolean blnWhere;
    protected boolean blnOr;
    protected boolean blnOpenBracket;
    protected List<Object> args;
    protected List<Integer> argTypes;

    /**
     * 初始化
     */
    protected void init() {
        tableInfos = new ArrayList<>();
        selectAllTableInfos = new ArrayList<>();
        sql = "";
        having = "";
        sqlBuilder = new StringBuffer();
        orderBy = new StringBuffer();
        columnInfos = new ArrayList<>();
        groupColumns = new ArrayList<>();
        this.args = new ArrayList<>();
        argTypes = new ArrayList<>();
        blnDistinct = false;
        blnWhere = false;
        blnOr = false;
        blnOpenBracket = false;
    }

    /**
     * 填充运算符where、or、and
     */
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

    protected Class<?> getTableClassByIndex(Integer tableIndex) {
        if (tableIndex == null) {
            return null;
        }
        if (tableIndex < 1) {
            return null;
        }
        return tableInfos.get(tableIndex - 1).getTableClass();
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
        SqlWrapper sqlWrapper = new SqlWrapper();
        consumer.accept(sqlWrapper);
        args.addAll(sqlWrapper.getArgs());
        argTypes.addAll(sqlWrapper.getArgTypes());
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
        sqlBuilder.append(String.format("%s %s %s ", joinType, tableName, tableId));
    }

    protected void appendJoin(Consumer<SqlWrapper> consumer, String tableId, String joinType) {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId(tableId);
        tableInfos.add(tableInfo);
        SqlWrapper sqlWrapper = new SqlWrapper();
        consumer.accept(sqlWrapper);
        sqlBuilder.append(String.format("%s (%s) %s ", joinType, sqlWrapper.getSql(), tableId));
    }

    protected void appendOn(String tableId1, String column1, String tableId2, String column2) {
        sqlBuilder.append(String.format("on %s.%s = %s.%s ", tableId1, column1, tableId2, column2));
    }

    protected void formatFullSql() {
        sql = sqlBuilder.toString();
        if (!CollectionUtils.isEmpty(groupColumns)) {
            sql += String.format("group by %s ", String.join(",", groupColumns));
        }
        if (StringUtils.isNotBlank(having)) {
            sql += String.format("HAVING %s ", having);
        }

        String columnString = formatColumns();
        sql = String.format(sql, columnString);
    }

    protected String formatColumns() {
        if (!CollectionUtils.isEmpty(selectAllTableInfos)) {
            for (TableInfo tableInfo : selectAllTableInfos) {
                Integer tableIndex = tableInfo.getTableIndex();
                Boolean formatBeanColumn = tableInfo.getFormatBeanColumn();
                Class<?> clazz = getTableClassByIndex(tableIndex);
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
            String tableId = getTableInfoByIndex(columnInfo.getTableIndex()).getTableId();
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
}
