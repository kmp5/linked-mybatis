package com.kzow3n.jdbcplus.core.executor;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.toolkit.ClassUtils;
import com.kzow3n.jdbcplus.pojo.SqlArg;
import com.kzow3n.jdbcplus.utils.ClazzUtils;
import lombok.Data;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 执行器基类
 *
 * @author owen
 * @since 2021/8/28
 */
@Data
public class BaseExecutor {

    protected SqlSession sqlSession;
    protected SqlSessionFactory sqlSessionFactory;
    protected RedisTemplate<String, Object> redisTemplate;
    protected Boolean cacheable = false;
    protected Long cacheTimeout = 60L;
    protected Integer queryTimeout = 60;
    protected boolean mapUnderscoreToCamelCase = true;

    protected void checkExecutorValid() {
        if (sqlSessionFactory == null) {
            throw new NullPointerException("sqlSessionFactory could not be null.");
        }
        if (cacheable && redisTemplate == null) {
            throw new NullPointerException("redisTemplate could not be null.");
        }
    }

    protected void initConfiguration() {
        Configuration configuration = sqlSessionFactory.getConfiguration();
        mapUnderscoreToCamelCase = configuration.isMapUnderscoreToCamelCase();
    }

    protected String getCacheKey(String sql, List<Object> args) {
        StringBuilder stringBuilder = new StringBuilder();
        List<SqlArg> sqlArgs = new ArrayList<>();
        for (Object arg: args) {
            if (arg == null) {
                SqlArg sqlArg = new SqlArg(null, "null");
                sqlArgs.add(sqlArg);
                continue;
            }
            String className = arg.getClass().getName();
            SqlArg sqlArg = new SqlArg(arg, className);
            sqlArgs.add(sqlArg);
        }
        String json = JSON.toJSONString(sqlArgs);
        stringBuilder.append("linked-mybatis:").append(sql).append(":").append(json);
        return stringBuilder.toString();
    }

    protected <T> List<T> mapsToBeans(List<Map<String, Object>> maps, Class<T> clazz) {
        List<Field> fields = ClazzUtils.getAllFields(clazz);
        Map<String, String> fieldMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, t -> t.getType().getName()));
        return CollectionUtils.isEmpty(maps) ? Collections.emptyList() : maps.stream().map((e) -> mapToBean(e, clazz, fieldMap)).collect(Collectors.toList());
    }

    protected <T> T mapToBean(Map<String, Object> map, Class<T> clazz, Map<String, String> fieldMap) {
        updateMap(map, fieldMap);
        T bean = ClassUtils.newInstance(clazz);
        BeanMap.create(bean).putAll(map);
        return bean;
    }

    protected <T> T mapToBean(Map<String, Object> map, Class<T> clazz) {
        List<Field> fields = ClazzUtils.getAllFields(clazz);
        Map<String, String> fieldMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, t -> t.getType().getName()));
        updateMap(map, fieldMap);
        T bean = ClassUtils.newInstance(clazz);
        BeanMap.create(bean).putAll(map);
        return bean;
    }

    private <T> void updateMap(Map<String, Object> map, Map<String, String> fieldMap) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object obj = entry.getValue();
            if (obj == null) {
                continue;
            }
            //处理Double
            if (obj instanceof Double) {
                String className = fieldMap.get(key);
                switch (className) {
                    default:
                        break;
                    case "java.math.BigDecimal":
                        entry.setValue(BigDecimal.valueOf((double) obj));
                        break;
                }
            }
            //处理Timestamp
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
            //处理Integer
            else if (obj instanceof Integer) {
                String className = fieldMap.get(key);
                switch (className) {
                    default:
                        break;
                    case "java.lang.Boolean":
                        if ((Integer)obj == 0) {
                            entry.setValue(false);
                        }
                        else {
                            entry.setValue(true);
                        }
                        break;
                    case "java.lang.Long":
                        entry.setValue(((Integer) obj).longValue());
                        break;
                }
            }
        }
    }
}
