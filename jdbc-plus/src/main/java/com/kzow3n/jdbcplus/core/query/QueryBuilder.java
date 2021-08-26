package com.kzow3n.jdbcplus.core.query;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.kzow3n.jdbcplus.core.SqlWrapper;
import org.apache.ibatis.session.SqlSession;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * 查询器
 *
 * @author owen
 * @since 2021/8/26
 */
public class QueryBuilder extends AbstractQueryBuilder {

    public QueryBuilder(SqlSession sqlSession, SqlWrapper sqlWrapper) {
        this.sqlWrapper = sqlWrapper;
        this.sqlSession = sqlSession;
    }

    public QueryBuilder(SqlSession sqlSession, SqlWrapper sqlWrapper, RedisTemplate<String, Object> redisTemplate) {
        this.sqlWrapper = sqlWrapper;
        this.sqlSession = sqlSession;
        this.redisTemplate = redisTemplate;
    }

    public long forCount() {
        return selectCount();
    }

    public List<Map<String, Object>> forMaps() {
        return selectList();
    }

    public Map<String, Object> forMap() {
        List<Map<String, Object>> mapList = selectList();
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        return mapList.stream().findFirst().orElse(null);
    }

    public <T> List<T> forObjects(Class<T> clazz) {
        List<Map<String, Object>> mapList = selectList();
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        updateMapList(mapList, clazz);
        return mapsToBeans(mapList, clazz);
    }

    public <T> T forObject(Class<T> clazz) {
        List<Map<String, Object>> mapList = selectList();
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        Map<String, Object> map = mapList.stream().findFirst().orElse(null);
        if (map == null) {
            return null;
        }
        updateMap(map, clazz);
        return mapToBean(map, clazz);
    }

    public Page<Map<String, Object>> forMapPage(int pageIndex, int pageSize) {
        Page<Map<String, Object>> page = new Page<>();
        List<Map<String, Object>> mapList = selectPage(page, pageIndex, pageSize);
        page.setRecords(mapList);
        return page;
    }

    public <T> Page<T> forObjectPage(Class<T> clazz, int pageIndex, int pageSize) {
        Page<T> page = new Page<>();
        List<Map<String, Object>> mapList = selectPage(page, pageIndex, pageSize);
        if (CollectionUtils.isEmpty(mapList)) {
            return page;
        }
        updateMapList(mapList, clazz);
        List<T> list = mapsToBeans(mapList, clazz);
        page.setRecords(list);
        return page;
    }

    public List<Map<String, Object>> execProForMaps(String proName, @Nullable Object... args) {
        return execPro(proName, args);
    }

    public Map<String, Object> execProForMap(String proName, @Nullable Object... args) {
        List<Map<String, Object>> mapList = execPro(proName, args);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        return mapList.stream().findFirst().orElse(null);
    }

    public <T> List<T> execProForObjects(Class<T> clazz, String proName, @Nullable Object... args) {
        List<Map<String, Object>> mapList = execPro(proName, args);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        updateMapsKeys(mapList, clazz);
        updateMapList(mapList, clazz);
        return mapsToBeans(mapList, clazz);
    }

    public <T> T execProForObject(Class<T> clazz, String proName, @Nullable Object... args) {
        List<Map<String, Object>> mapList = execPro(proName, args);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        Map<String, Object> map = mapList.stream().findFirst().orElse(null);
        if (map == null) {
            return null;
        }
        updateMapKeys(map, clazz);
        updateMap(map, clazz);
        return mapToBean(map, clazz);
    }
}
