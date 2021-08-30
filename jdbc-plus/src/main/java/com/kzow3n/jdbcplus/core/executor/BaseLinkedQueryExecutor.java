package com.kzow3n.jdbcplus.core.executor;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.kzow3n.jdbcplus.core.wrapper.LinkedQueryWrapper;
import com.kzow3n.jdbcplus.core.jdbc.MySqlRunner;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 查询执行器基本方法类
 *
 * @author owen
 * @since 2021/8/26
 */
@Slf4j
public class BaseLinkedQueryExecutor extends BaseExecutor {

    protected long selectCount(LinkedQueryWrapper linkedQueryWrapper) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();
        String limit = linkedQueryWrapper.getLimit();
        String sqlCount =
                String.format(linkedQueryWrapper.getSqlBuilder().toString(), "count(1) selectCount")
                + limit;
        List<Object> args = linkedQueryWrapper.getArgs();
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection(), queryTimeout);
        Map<String, Object> map;
        long count = 0L;
        if (cacheable) {
            String cacheKey = getCacheKey(sqlCount, args);
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                return Long.parseLong(Objects.requireNonNull(redisTemplate.opsForValue().get(cacheKey)).toString());
            }
            log.info(sqlCount);
            try {
                map = sqlRunner.selectOne(sqlCount, args.toArray());
                count = Long.parseLong(map.get("selectCount").toString());
                doCache(cacheKey, count);
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        else {
            log.info(sqlCount);
            try {
                map = sqlRunner.selectOne(sqlCount, args.toArray());
                count = Long.parseLong(map.get("selectCount").toString());
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        return count;
    }

    protected List<Map<String, Object>> selectList(LinkedQueryWrapper linkedQueryWrapper) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();
        String sql = linkedQueryWrapper.getSql()
                + linkedQueryWrapper.getOrderBy().toString()
                + linkedQueryWrapper.getLimit();
        List<Object> args = linkedQueryWrapper.getArgs();

        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection(), queryTimeout);
        List<Map<String, Object>> mapList = null;
        if (cacheable) {
            String cacheKey = getCacheKey(sql, args);
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                return (List<Map<String, Object>>)redisTemplate.opsForValue().get(cacheKey);
            }
            log.info(sql);
            try {
                mapList = sqlRunner.selectAll(sql, args.toArray());
                doCache(cacheKey, mapList);
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        else {
            log.info(sql);
            try {
                mapList = sqlRunner.selectAll(sql, args.toArray());
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        return mapList;
    }

    protected <T> List<Map<String, Object>> selectPage(LinkedQueryWrapper linkedQueryWrapper, Page<T> page, long pageIndex, long pageSize) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();
        linkedQueryWrapper.setLimit("");
        long total = selectCount(linkedQueryWrapper);
        long pages = total % pageSize > 0 ? (total / pageSize) + 1L : total / pageSize;
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);

        String sql = linkedQueryWrapper.getSql()
                + linkedQueryWrapper.getOrderBy().toString()
                + String.format(" limit %d,%d", (pageIndex - 1L) * pageSize, pageSize);
        List<Object> args = linkedQueryWrapper.getArgs();
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection(), queryTimeout);
        List<Map<String, Object>> mapList = null;
        if (cacheable) {
            String cacheKey = getCacheKey(sql, args);
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                return (List<Map<String, Object>>)redisTemplate.opsForValue().get(cacheKey);
            }
            log.info(sql);
            try {
                mapList = sqlRunner.selectAll(sql, args.toArray());
                doCache(cacheKey, mapList);
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        else {
            log.info(sql);
            try {
                mapList = sqlRunner.selectAll(sql, args.toArray());
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        return mapList;
    }

    private void doCache(String cacheKey, long count) {
        if (!redisTemplate.hasKey(cacheKey)) {
            redisTemplate.opsForValue().set(cacheKey, count, cacheTimeout, TimeUnit.SECONDS);
            log.info(String.format("cacheKey:%s has bean cached.", cacheKey));
        }
        else {
            log.info(String.format("cacheKey:%s already exists.", cacheKey));
        }
    }

    private void doCache(String cacheKey, List<Map<String, Object>> mapList) {
        if (!redisTemplate.hasKey(cacheKey)) {
            redisTemplate.opsForValue().set(cacheKey, mapList, cacheTimeout, TimeUnit.SECONDS);
            log.info(String.format("cacheKey:%s has bean cached.", cacheKey));
        }
        else {
            log.info(String.format("cacheKey:%s already exists.", cacheKey));
        }
    }

}
