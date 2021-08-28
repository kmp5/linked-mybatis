package com.kzow3n.jdbcplus.core.executor;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.toolkit.ClassUtils;
import com.kzow3n.jdbcplus.pojo.SqlArg;
import com.kzow3n.jdbcplus.utils.ClazzUtils;
import lombok.Data;
import org.apache.ibatis.session.SqlSession;
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
public class Executor {

    protected SqlSession sqlSession;
    protected RedisTemplate<String, Object> redisTemplate;
    protected Boolean cacheable = false;
    protected Long timeout = 60L;

    protected void checkExecutorValid() {
        if (sqlSession == null) {
            throw new NullPointerException("SqlSession Could not be null.");
        }
        if (cacheable && redisTemplate == null) {
            throw new NullPointerException("RedisTemplate Could not be null.");
        }
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
        stringBuilder.append("SqlRunner-Plus.").append(sql).append(".").append(json);
        return stringBuilder.toString();
    }

    protected <T> void updateMap(Map<String, Object> map, Class<T> clazz) {
        List<Field> fields = ClazzUtils.getAllFields(clazz);
        Map<String, String> fieldMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, t -> t.getType().getName()));
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object obj = entry.getValue();
            if (obj == null) {
                continue;
            }
            //处理java.lang.Double
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
}
