package com.kzow3n.jdbcplus.core.executor;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.kzow3n.jdbcplus.core.wrapper.LinkedQueryWrapper;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * 查询执行器
 *
 * @author owen
 * @since 2021/8/26
 */
public class LinkedQueryExecutor extends BaseLinkedQueryExecutor {

    public LinkedQueryExecutor(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
        initConfiguration();
    }

    public LinkedQueryExecutor(SqlSessionFactory sqlSessionFactory, SqlSession sqlSession) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.sqlSession = sqlSession;
        initConfiguration();
    }

    public long forCount(LinkedQueryWrapper linkedQueryWrapper) {
        return queryForCount(linkedQueryWrapper, true);
    }

    public List<Map<String, Object>> forMaps(LinkedQueryWrapper linkedQueryWrapper) {
        return queryForMaps(linkedQueryWrapper);
    }

    public Map<String, Object> forMap(LinkedQueryWrapper linkedQueryWrapper) {
        List<Map<String, Object>> mapList = queryForMaps(linkedQueryWrapper);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        return mapList.stream().findFirst().orElse(null);
    }

    public <T> List<T> forObjects(Class<T> clazz, LinkedQueryWrapper linkedQueryWrapper) {
        return queryForObjects(clazz, linkedQueryWrapper);
    }

    public <T> T forObject(Class<T> clazz, LinkedQueryWrapper linkedQueryWrapper) {
        List<T> list = queryForObjects(clazz, linkedQueryWrapper);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        return list.stream().findFirst().orElse(null);
    }

    public Page<Map<String, Object>> forMapPage(LinkedQueryWrapper linkedQueryWrapper, int pageIndex, int pageSize) {
        Page<Map<String, Object>> page = new Page<>();
        return queryForMapPage(linkedQueryWrapper, page, pageIndex, pageSize);
    }

    public <T> Page<T> forObjectPage(Class<T> clazz, LinkedQueryWrapper linkedQueryWrapper, int pageIndex, int pageSize) {
        Page<T> page = new Page<>();
        return queryForObjectPage(clazz, linkedQueryWrapper, page, pageIndex, pageSize);
    }
}
