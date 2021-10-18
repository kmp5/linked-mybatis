package com.kzow3n.jdbcplus.core.executor;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.toolkit.ClassUtils;
import com.kzow3n.jdbcplus.pojo.SqlArg;
import com.kzow3n.jdbcplus.utils.ClazzUtils;
import lombok.Data;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
    protected boolean columnCaseInsensitive = false;

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
        List<Field> fields = ClazzUtils.getAllFields(clazz).stream().filter(t -> !Modifier.isStatic(t.getModifiers())).collect(Collectors.toList());
        Map<String, String> fieldClassNameMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, t -> t.getType().getName()));
        Map<String, String> fieldMethodNameMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, t -> getMethodName(t.getName())));
        return CollectionUtils.isEmpty(maps) ? Collections.emptyList() : maps.stream().map((m) -> {
            T bean = ClassUtils.newInstance(clazz);
            for (Field field : fields) {
                String key = field.getName();
                String className = fieldClassNameMap.get(key);
                String methodName = fieldMethodNameMap.get(key);
                if (columnCaseInsensitive) {
                    key = key.toUpperCase();
                }
                if (!m.containsKey(key)) {
                    continue;
                }
                Object val = m.get(key);
                Object value = updateValue(val, className);
                try {
                    clazz.getMethod(methodName, field.getType()).invoke(bean, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return bean;
        }).collect(Collectors.toList());
    }

    protected <T> T mapToBean(Map<String, Object> map, Class<T> clazz) {
        List<Field> fields = ClazzUtils.getAllFields(clazz).stream().filter(t -> !Modifier.isStatic(t.getModifiers())).collect(Collectors.toList());
        T bean = ClassUtils.newInstance(clazz);
        for (Field field : fields) {
            String key = field.getName();
            String methodName = getMethodName(key);
            String className = field.getType().getName();
            if (columnCaseInsensitive) {
                key = key.toUpperCase();
            }
            if (!map.containsKey(key)) {
                continue;
            }
            Object val = map.get(key);
            Object value = updateValue(val, className);
            try {
                clazz.getMethod(methodName, field.getType()).invoke(bean, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return bean;
    }

    private static String getMethodName(String s) {
        StringBuilder stringBuilder = new StringBuilder("set");
        if(Character.isUpperCase(s.charAt(0)))
        {
            stringBuilder.append(s);
        }
        else {
            stringBuilder.append(Character.toUpperCase(s.charAt(0)))
                        .append(s.substring(1));
        }
        return stringBuilder.toString();
    }

    private Object updateValue(Object val, String className) {
        if (val == null) {
            return null;
        }
        //处理String
        if (!(val instanceof String)) {
            if ("java.lang.String".equals(className))
            {
                return val.toString();
            }
        }

        //处理BigDecimal
        if (val instanceof BigDecimal) {
            switch (className) {
                default:
                    break;
                case "java.lang.Integer":
                    return ((BigDecimal) val).intValue();
            }
        }
        //处理Double
        else if (val instanceof Double) {
            switch (className) {
                default:
                    break;
                case "java.math.BigDecimal":
                    return BigDecimal.valueOf((double) val);
            }
        }
        //处理Timestamp
        else if (val instanceof Timestamp) {
            switch (className) {
                default:
                    break;
                case "java.time.LocalDateTime":
                    return ((Timestamp) val).toLocalDateTime();
                case "java.time.LocalDate":
                    return ((Timestamp) val).toLocalDateTime().toLocalDate();
                case "java.time.LocalTime":
                    return ((Timestamp) val).toLocalDateTime().toLocalTime();
            }
        }
        //处理Integer
        else if (val instanceof Integer) {
            switch (className) {
                default:
                    break;
                case "java.lang.Boolean":
                    return (Integer) val != 0;
                case "java.lang.Long":
                    return ((Integer) val).longValue();
            }
        }
        return val;
    }
}
