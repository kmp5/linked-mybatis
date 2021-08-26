package com.kzow3n.jdbcplus.core.query;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.core.toolkit.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 查询器的一些私有方法
 *
 * @author owen
 * @since 2021/8/26
 */
public class QueryBuilderBase {

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

    protected <T> List<T> mapsToBeans(List<? extends Map<String, ?>> maps, Class<T> clazz) {
        return CollectionUtils.isEmpty(maps) ? Collections.emptyList() : maps.stream().map((e) -> mapToBean(e, clazz)).collect(Collectors.toList());
    }

    protected <T> T mapToBean(Map<String, ?> map, Class<T> clazz) {
        T bean = ClassUtils.newInstance(clazz);
        BeanMap.create(bean).putAll(map);
        return bean;
    }

    protected void updateMapsKeys(List<Map<String, Object>> mapList, Class<?> clazz) {
        List<Field> fields = getAllFields(clazz);
        for (Field field : fields) {
            String tableColumn = getTableColumnByField(field);
            if (StringUtils.isNotBlank(tableColumn)) {
                String beanColumn = field.getName();
                mapList.forEach(map -> {
                    map.put(beanColumn, map.remove(tableColumn));
                });
            }
        }
    }

    protected void updateMapKeys(Map<String, Object> map, Class<?> clazz) {
        List<Field> fields = getAllFields(clazz);
        for (Field field : fields) {
            String tableColumn = getTableColumnByField(field);
            if (StringUtils.isNotBlank(tableColumn)) {
                String beanColumn = field.getName();
                map.put(beanColumn, map.remove(tableColumn));
            }
        }
    }

    private List<Field> getAllFields(Class<?> clazz) {
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

    private String getTableColumnByField(Field field) {
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
}
