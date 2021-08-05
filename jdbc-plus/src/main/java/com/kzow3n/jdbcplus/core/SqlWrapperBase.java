package com.kzow3n.jdbcplus.core;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.toolkit.ClassUtils;
import com.kzow3n.jdbcplus.pojo.TableInfo;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Sql构造器的一些私有方法
 *
 * @author owen
 * @since 2021/8/4
 */
public class SqlWrapperBase {

    /**
     * 获取类的全部Field列表
     *
     * @param clazz clazz
     * @return List<Field>
     */
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

    /**
     * 更新Map的Value的类型，避免映射到Bean时异常
     *
     * @param map map
     * @param clazz clazz
     */
    protected <T> void updateMap(Map<String, Object> map, Class<T> clazz) {
        List<Field> fields = getAllFields(clazz);
        Map<String, String> fieldMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, t -> t.getType().getName()));
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object obj = entry.getValue();
            if (obj == null) {
                continue;
            }
            //java.sql.Timestamp格式特殊处理
            if (obj instanceof Timestamp) {
                String className = fieldMap.get(key);
                switch (className) {
                    default:
                        break;
                    case "java.time.LocalDateTime":
                        entry.setValue(((Timestamp) obj).toLocalDateTime());
                        break;
                    case "java.time.LocalDate":
                        entry.setValue(((Timestamp) obj).toLocalDateTime().toLocalDate());
                        break;
                    case "java.time.LocalTime":
                        entry.setValue(((Timestamp) obj).toLocalDateTime().toLocalTime());
                        break;
                }
            }
            //TINYINT会转成Integer
            else if (obj instanceof Integer) {
                String className = fieldMap.get(key);
                if ("java.lang.Boolean".equals(className)) {
                    if ((Integer)obj == 0) {
                        entry.setValue(false);
                    }
                    else {
                        entry.setValue(true);
                    }
                }
            }
        }
    }

    protected <T> void updateMapList(List<Map<String, Object>> mapList, Class<T> clazz) {
        for (Map<String, Object> map : mapList) {
            updateMap(map, clazz);
        }
    }

    protected String getJoinString(String[] arr) {
        if (arr == null) {
            return "";
        }
        return String.join(",", arr);
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

    protected Integer getObjectSqlType(Object object) {
        int type;
        String className = object.getClass().getName();
        switch (className) {
            default:
                type = Types.NULL;
                break;
            case "java.lang.Integer":
                type = Types.INTEGER;
                break;
            case "java.lang.Long":
                type = Types.BIGINT;
                break;
            case "java.lang.Float":
                type = Types.REAL;
                break;
            case "java.math.Double":
                type = Types.DOUBLE;
                break;
            case "java.lang.String":
                type = Types.NVARCHAR;
                break;
            case "java.math.BigDecimal":
                type = Types.DECIMAL;
                break;
            case "java.util.Date":
            case "java.time.LocalDateTime":
                type = Types.TIMESTAMP;
                break;
            case "java.time.LocalDate":
                type = Types.DATE;
                break;
            case "java.time.LocalTime":
                type = Types.TIME;
                break;
            case "java.lang.Boolean":
                type = Types.BIT;
                break;
        }
        return type;
    }

    protected static <T> List<T> mapsToBeans(List<? extends Map<String, ?>> maps, Class<T> clazz) {
        return CollectionUtils.isEmpty(maps) ? Collections.emptyList() : maps.stream().map((e) -> mapToBean(e, clazz)).collect(Collectors.toList());
    }

    protected static <T> T mapToBean(Map<String, ?> map, Class<T> clazz) {
        T bean = ClassUtils.newInstance(clazz);
        BeanMap.create(bean).putAll(map);
        return bean;
    }
}
