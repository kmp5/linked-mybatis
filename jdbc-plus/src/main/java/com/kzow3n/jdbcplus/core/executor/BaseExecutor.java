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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;
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
        Map<String, String> fieldMethodNameMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, t -> getMethodName(t.getName())));
        return CollectionUtils.isEmpty(maps) ? Collections.emptyList() : maps.stream().map((m) -> {
            T bean = ClassUtils.newInstance(clazz);
            for (Field field : fields) {
                String key = field.getName();
                Class<?> fieldType = field.getType();
                String methodName = fieldMethodNameMap.get(key);
                invokeSetMethod(bean, clazz, m, key, methodName, fieldType);
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
            Class<?> fieldType = field.getType();
            invokeSetMethod(bean, clazz, map, key, methodName, fieldType);
        }
        return bean;
    }

    private <T> void invokeSetMethod(T bean, Class<T> clazz, Map<String, Object> map, String key, String methodName, Class<?> fieldType) {
        String realKey;
        if (columnCaseInsensitive) {
            realKey = key.toUpperCase();
        }
        else {
            realKey = key;
        }
        if (!map.containsKey(realKey)) {
            return;
        }
        Object val = map.get(realKey);
        Object value = updateValue(val, fieldType.getName());
        try {
            clazz.getMethod(methodName, fieldType).invoke(bean, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        //处理Clob
        else if (val instanceof Clob) {
            try {
                return ClobToString((Clob) val);
            } catch (SQLException | IOException ignored) {

            }
            return val.toString();
        }
        else if (!(val instanceof String)) {
            if ("java.lang.String".equals(className))
            {
                return val.toString();
            }
        }
        return val;
    }

    private String ClobToString(Clob clob) throws SQLException, IOException {
        String reString = "";
        Reader is = clob.getCharacterStream();
        BufferedReader br = new BufferedReader(is);
        String s = br.readLine();
        StringBuilder sb = new StringBuilder();
        while (s != null) {
            sb.append(s);
            s = br.readLine();
        }
        reString = sb.toString();
        br.close();
        is.close();
        return reString;
    }
}
