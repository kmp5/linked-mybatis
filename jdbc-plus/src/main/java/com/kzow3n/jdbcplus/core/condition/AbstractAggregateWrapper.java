package com.kzow3n.jdbcplus.core.condition;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import lombok.Data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * 聚合Column构造器基本方法类
 *
 * @author owen
 * @since 2021/8/10
 */
@Data
public class AbstractAggregateWrapper {
    protected List<TableInfo> tableInfos;
    protected String column;

    private void init() {
        tableInfos = new ArrayList<>();
        column = "";
    }

    public AbstractAggregateWrapper() {
        init();
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

    protected String getColumn(TableInfo tableInfo, Field field) {
        if (tableInfo.getTableClass() != null) {
            return getTableColumnByField(field);
        }
        else {
            return getBeanColumnByField(field);
        }
    }

    protected String getTableColumnByField(Field field) {
        String tableColumn = null;
        TableField annotation = field.getAnnotation(TableField.class);
        if (annotation != null) {
            tableColumn = annotation.value();
        }
        else {
            TableId annotation2 = field.getAnnotation(TableId.class);
            if (annotation2 != null) {
                tableColumn = annotation2.value();
            }
        }
        if (tableColumn == null) {
            tableColumn = field.getName();
        }
        return tableColumn;
    }

    protected String getBeanColumnByField(Field field) {
        return field.getName();
    }
}
