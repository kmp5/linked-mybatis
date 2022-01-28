package com.kzow3n.jdbcplus.utils;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 表字段工具类
 *
 * @author owen
 * @since 2021/8/4
 */
public class ColumnUtils {
    public static <T> String getName(SFunction<T, ?> fn) {
        // 从function取出序列化方法
        Method writeReplaceMethod;
        try {
            writeReplaceMethod = fn.getClass().getDeclaredMethod("writeReplace");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        // 从序列化方法取出序列化的lambda信息
        boolean isAccessible = writeReplaceMethod.isAccessible();
        writeReplaceMethod.setAccessible(true);
        SerializedLambda serializedLambda;
        try {
            serializedLambda = (SerializedLambda) writeReplaceMethod.invoke(fn);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        writeReplaceMethod.setAccessible(isAccessible);

        // 从lambda信息取出method、field、class等
        String fieldName = serializedLambda.getImplMethodName().substring("get".length());
        fieldName = fieldName.replaceFirst(fieldName.charAt(0) + "", (fieldName.charAt(0) + "").toLowerCase());
        return fieldName;
    }

    public static <T> Field getField(SFunction<T, ?> fn) {
        // 从function取出序列化方法
        Method writeReplaceMethod;
        try {
            writeReplaceMethod = fn.getClass().getDeclaredMethod("writeReplace");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        // 从序列化方法取出序列化的lambda信息
        boolean isAccessible = writeReplaceMethod.isAccessible();
        writeReplaceMethod.setAccessible(true);
        SerializedLambda serializedLambda;
        try {
            serializedLambda = (SerializedLambda) writeReplaceMethod.invoke(fn);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        writeReplaceMethod.setAccessible(isAccessible);

        // 从lambda信息取出method、field、class等
        String fieldName = serializedLambda.getImplMethodName().substring("get".length());
        fieldName = fieldName.replaceFirst(fieldName.charAt(0) + "", (fieldName.charAt(0) + "").toLowerCase());
        // 获取的Class是字符串，并且包名是“/”分割，需要替换成“.”，才能获取到对应的Class对象
        String declaredClass = serializedLambda.getImplClass().replace("/", ".");
        Class<?> aClass;
        try {
            aClass = Class.forName(declaredClass, false, ClassUtils.getDefaultClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        // Spring 中的反射工具类获取Class中定义的Field
        return ReflectionUtils.findField(aClass, fieldName);
    }

    public static String getColumn(TableInfo tableInfo, Field field, boolean mapUnderscoreToCamelCase) {
        if (tableInfo.getTableClass() != null) {
            return getTableColumnByField(field, mapUnderscoreToCamelCase);
        }
        else {
            return getBeanColumnByField(field);
        }
    }

    public static String getTableColumnByField(Field field, boolean mapUnderscoreToCamelCase) {
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
        if (StringUtils.isBlank(tableColumn)) {
            if (mapUnderscoreToCamelCase) {
                tableColumn = com.baomidou.mybatisplus.core.toolkit.StringUtils.camelToUnderline(field.getName());
            }
            else {
                tableColumn = field.getName();
            }
        }
        return tableColumn;
    }

    public static String getBeanColumnByField(Field field) {
        return field.getName();
    }
}
