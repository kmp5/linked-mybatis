package com.kzow3n.jdbcplus.core.executor;

import org.apache.ibatis.session.SqlSession;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * 存储过程执行器
 *
 * @author owen
 * @since 2021/8/28
 */
public class ProcedureExecutor extends BaseProcedureExecutor {

    public ProcedureExecutor(SqlSession sqlSession) {
        this.sqlSession = sqlSession;
    }

    public ProcedureExecutor(SqlSession sqlSession, RedisTemplate<String, Object> redisTemplate) {
        this.sqlSession = sqlSession;
        this.redisTemplate = redisTemplate;
    }

    public ProcedureExecutor(SqlSession sqlSession, RedisTemplate<String, Object> redisTemplate, Boolean cacheable) {
        this.sqlSession = sqlSession;
        this.redisTemplate = redisTemplate;
        this.cacheable = cacheable;
    }

    public ProcedureExecutor(SqlSession sqlSession, RedisTemplate<String, Object> redisTemplate, Boolean cacheable, Long timeout) {
        this.sqlSession = sqlSession;
        this.redisTemplate = redisTemplate;
        this.cacheable = cacheable;
        this.timeout = timeout;
    }

    public List<Map<String, Object>> forMaps(String proName, @Nullable Object... args) {
        return execPro(proName, args);
    }

    public Map<String, Object> forMap(String proName, @Nullable Object... args) {
        List<Map<String, Object>> mapList = execPro(proName, args);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        return mapList.stream().findFirst().orElse(null);
    }

    public <T> List<T> forObjects(Class<T> clazz, String proName, @Nullable Object... args) {
        List<Map<String, Object>> mapList = execPro(proName, args);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        updateMapsKeys(mapList, clazz);
        return mapsToBeans(mapList, clazz);
    }

    public <T> T forObject(Class<T> clazz, String proName, @Nullable Object... args) {
        List<Map<String, Object>> mapList = execPro(proName, args);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        Map<String, Object> map = mapList.stream().findFirst().orElse(null);
        if (map == null) {
            return null;
        }
        updateMapKeys(map, clazz);
        return mapToBean(map, clazz);
    }
}
