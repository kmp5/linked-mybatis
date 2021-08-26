package com.kzow3n.jdbcplus.core;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.kzow3n.jdbcplus.pojo.TableInfo;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Sql构造器的一些私有方法
 *
 * @author owen
 * @since 2021/8/4
 */
public class SqlWrapperBase {

    protected List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = Arrays.stream(clazz.getDeclaredFields()).collect(Collectors.toList());
        addSuperClassFields(fields, clazz);
        return fields;
    }

    private void addSuperClassFields(List<Field> fields, Class<?> clazz) {
        Class<?> superClazz = clazz.getSuperclass();
        if (superClazz == null) {
            return;
        }
        fields.addAll(Arrays.asList(superClazz.getDeclaredFields()));
        addSuperClassFields(fields, superClazz);
    }

    protected String getTableNameByClass(Class<?> clazz) {
        TableName annotation = clazz.getAnnotation(TableName.class);
        if (annotation == null) {
            return clazz.getName();
        }
        return annotation.value();
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
