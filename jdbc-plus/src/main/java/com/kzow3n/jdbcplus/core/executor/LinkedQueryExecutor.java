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
    }

    public LinkedQueryExecutor(SqlSessionFactory sqlSessionFactory, SqlSession sqlSession) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.sqlSession = sqlSession;
    }

    public long forCount(LinkedQueryWrapper linkedQueryWrapper) {
        return selectCount(linkedQueryWrapper, true);
    }

    public List<Map<String, Object>> forMaps(LinkedQueryWrapper linkedQueryWrapper) {
        return selectList(linkedQueryWrapper);
    }

    public Map<String, Object> forMap(LinkedQueryWrapper linkedQueryWrapper) {
        List<Map<String, Object>> mapList = selectList(linkedQueryWrapper);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        return mapList.stream().findFirst().orElse(null);
    }

    public <T> List<T> forObjects(Class<T> clazz, LinkedQueryWrapper linkedQueryWrapper) {
        List<Map<String, Object>> mapList = selectList(linkedQueryWrapper);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        return mapsToBeans(mapList, clazz);
    }

    public <T> T forObject(Class<T> clazz, LinkedQueryWrapper linkedQueryWrapper) {
        List<Map<String, Object>> mapList = selectList(linkedQueryWrapper);
        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }
        Map<String, Object> map = mapList.stream().findFirst().orElse(null);
        if (map == null) {
            return null;
        }
        return mapToBean(map, clazz);
    }

    public Page<Map<String, Object>> forMapPage(LinkedQueryWrapper linkedQueryWrapper, int pageIndex, int pageSize) {
        Page<Map<String, Object>> page = new Page<>();
        List<Map<String, Object>> mapList = selectPage(linkedQueryWrapper, page, pageIndex, pageSize);
        page.setRecords(mapList);
        return page;
    }

    public <T> Page<T> forObjectPage(Class<T> clazz, LinkedQueryWrapper linkedQueryWrapper, int pageIndex, int pageSize) {
        Page<T> page = new Page<>();
        List<Map<String, Object>> mapList = selectPage(linkedQueryWrapper, page, pageIndex, pageSize);
        if (CollectionUtils.isEmpty(mapList)) {
            return page;
        }
        List<T> list = mapsToBeans(mapList, clazz);
        page.setRecords(list);
        return page;
    }
}
