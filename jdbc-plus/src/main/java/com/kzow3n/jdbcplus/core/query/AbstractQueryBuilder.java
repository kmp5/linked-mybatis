package com.kzow3n.jdbcplus.core.query;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.core.toolkit.ClassUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.kzow3n.jdbcplus.core.SqlWrapper;
import com.kzow3n.jdbcplus.core.jdbc.MySqlRunner;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 查询器基本方法类
 *
 * @author owen
 * @since 2021/8/26
 */
@Data
@Slf4j
public class AbstractQueryBuilder {

    protected SqlSession sqlSession;
    protected SqlWrapper sqlWrapper;
    protected RedisTemplate<String, Object> redisTemplate;
    protected Boolean cacheable = false;

    protected long selectCount() {
        checkBuilderValid(false);
        sqlWrapper.formatSql();
        String sqlCount = String.format(sqlWrapper.getSqlBuilder().toString(), "count(1) selectCount");
        log.info(sqlCount);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        Map<String, Object> map;
        try {
            map = sqlRunner.selectOne(sqlCount, sqlWrapper.getArgs().toArray());
            return (long) map.get("selectCount");
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return 0;
    }

    protected List<Map<String, Object>> selectList() {
        checkBuilderValid(false);
        sqlWrapper.formatSql();
        String sql = sqlWrapper.getSql() + sqlWrapper.getOrderBy().toString();
        log.info(sql);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        List<Map<String, Object>> mapList = null;
        try {
            mapList = sqlRunner.selectAll(sql, sqlWrapper.getArgs().toArray());
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }

    protected <T> List<Map<String, Object>> selectPage(Page<T> page, int pageIndex, int pageSize) {
        checkBuilderValid(false);
        int total = (int) selectCount();
        int pages = total % pageSize > 0 ? (total / pageSize) + 1 : total / pageSize;
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);

        sqlWrapper.formatSql();
        String sql = sqlWrapper.getSql() + sqlWrapper.getOrderBy().toString();
        sql += String.format(" limit %d,%d", (pageIndex - 1) * pageSize, pageSize);
        log.info(sql);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        List<Map<String, Object>> mapList = null;
        try {
            mapList = sqlRunner.selectAll(sql, sqlWrapper.getArgs().toArray());
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }

    protected List<Map<String, Object>> execPro(String proName, Object... args) {
        checkBuilderValid(true);
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("call %s", proName));
        if (args != null) {
            List<String> formats = new ArrayList<>();
            for (int i = 0;i < args.length;i ++) {
                formats.add("?");
            }
            sqlBuilder.append(String.format("(%s)", String.join(",", formats)));
        }
        String sql = sqlBuilder.toString();
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

    private void checkBuilderValid(boolean blnExecPro) {
        if (sqlSession == null) {
            throw new NullPointerException("SqlSession Could not be null.");
        }
        if (!blnExecPro && sqlWrapper == null) {
            throw new NullPointerException("SqlWrapper Could not be null.");
        }
        if (cacheable && redisTemplate == null) {
            throw new NullPointerException("RedisTemplate Could not be null.");
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
