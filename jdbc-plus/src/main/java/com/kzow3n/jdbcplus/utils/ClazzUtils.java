package com.kzow3n.jdbcplus.utils;

import com.baomidou.mybatisplus.annotation.TableName;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class相关工具类
 *
 * @author owen
 * @since 2021/8/28
 */
public class ClazzUtils {

    public static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = Arrays.stream(clazz.getDeclaredFields()).collect(Collectors.toList());
        addSuperClassFields(fields, clazz);
        return fields;
    }

    public static String getTableName(Class<?> clazz, boolean mapUnderscoreToCamelCase) {
        TableName annotation = clazz.getAnnotation(TableName.class);
        if (annotation == null) {
            if (mapUnderscoreToCamelCase) {
                return com.baomidou.mybatisplus.core.toolkit.StringUtils.underlineToCamel(clazz.getSimpleName());
            }
            else {
                return clazz.getSimpleName();
            }
        }
        return annotation.value();
    }

    private static void addSuperClassFields(List<Field> fields, Class<?> clazz) {
        Class<?> superClazz = clazz.getSuperclass();
        if (superClazz == null) {
            return;
        }
        fields.addAll(Arrays.asList(superClazz.getDeclaredFields()));
        addSuperClassFields(fields, superClazz);
    }
}
