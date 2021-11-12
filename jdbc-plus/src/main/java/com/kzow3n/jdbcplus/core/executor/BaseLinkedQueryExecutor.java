package com.kzow3n.jdbcplus.core.executor;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.kzow3n.jdbcplus.core.jdbc.DbTypeEnum;
import com.kzow3n.jdbcplus.core.wrapper.LinkedQueryWrapper;
import com.kzow3n.jdbcplus.core.jdbc.MySqlRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

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

    protected long queryForCount(LinkedQueryWrapper linkedQueryWrapper, boolean blnLimit) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();
        StringBuilder fullSqlBuilder = new StringBuilder();
        List<String> groupColumns = linkedQueryWrapper.getGroupColumns();
        String limit = linkedQueryWrapper.getLimit();
        boolean blnAppendLimit = blnLimit && StringUtils.isNotBlank(limit);
        if (CollectionUtils.isEmpty(groupColumns) && !blnAppendLimit) {
            fullSqlBuilder.append(String.format(linkedQueryWrapper.getSqlBuilder().toString(), "count(1) SELECT_COUNT"));
        }
        else {
            StringBuilder baseSqlBuilder = new StringBuilder();
            baseSqlBuilder.append(linkedQueryWrapper.getBaseSql());
            if (blnAppendLimit) {
                baseSqlBuilder.append(limit);
            }
            fullSqlBuilder.append(String.format("select count(1) SELECT_COUNT from (%s) t", baseSqlBuilder));
        }
        String sql = fullSqlBuilder.toString();
        List<Object> args = linkedQueryWrapper.getArgs();
        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
        Map<String, Object> map;
        long count = 0L;
        if (cacheable) {
            String cacheKey = getCacheKey(sql, args, "forCount");
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                return Long.parseLong(Objects.requireNonNull(redisTemplate.opsForValue().get(cacheKey)).toString());
            }
            log.info(sql);
            try {
                map = sqlRunner.selectOne(sql, args.toArray());
                count = Long.parseLong(map.get("SELECT_COUNT").toString());
                doCache(cacheKey, count);
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        else {
            log.info(sql);
            try {
                map = sqlRunner.selectOne(sql, args.toArray());
                count = Long.parseLong(map.get("SELECT_COUNT").toString());
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        return count;
    }

    protected List<Map<String, Object>> queryForMaps(LinkedQueryWrapper linkedQueryWrapper) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();
        String sql = linkedQueryWrapper.getFullSql();
        List<Object> args = linkedQueryWrapper.getArgs();

        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
        List<Map<String, Object>> mapList = null;
        if (cacheable) {
            String cacheKey = getCacheKey(sql, args, "forMaps");
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                return (List<Map<String, Object>>) redisTemplate.opsForValue().get(cacheKey);
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

    protected <T> List<T> queryForObjects(Class<T> clazz, LinkedQueryWrapper linkedQueryWrapper) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();
        String sql = linkedQueryWrapper.getFullSql();
        List<Object> args = linkedQueryWrapper.getArgs();

        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
        List<T> list = null;
        if (cacheable) {
            String cacheKey = getCacheKey(sql, args, "forObjects");
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                return (List<T>) redisTemplate.opsForValue().get(cacheKey);
            }
            log.info(sql);
            try {
                list = sqlRunner.selectAll(clazz, sql, args.toArray());
                doCache(cacheKey, list);
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        else {
            log.info(sql);
            try {
                list = sqlRunner.selectAll(clazz, sql, args.toArray());
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        return list;
    }

    protected Page<Map<String, Object>> queryForMapPage(LinkedQueryWrapper linkedQueryWrapper, Page<Map<String, Object>> page, long pageIndex, long pageSize) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();

        long total = queryForCount(linkedQueryWrapper, false);
        long pages = total % pageSize > 0 ? (total / pageSize) + 1L : total / pageSize;
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);

        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
        DbTypeEnum dbTypeEnum = sqlRunner.getDbTypeEnum();
        String sql;
        switch (dbTypeEnum) {
            default:
                sql = linkedQueryWrapper.getBaseSql() +
                        linkedQueryWrapper.getOrderBy().toString() +
                        String.format(" limit %d,%d", (pageIndex - 1L) * pageSize, pageSize);
                break;
            case DM:
                sql = linkedQueryWrapper.getBaseSql() +
                        linkedQueryWrapper.getOrderBy().toString();
                sql = String.format("SELECT * FROM (SELECT TMP.*, ROWNUM ROW_ID FROM (%s) TMP WHERE ROWNUM <= %d) WHERE ROW_ID > %d",
                        sql, pageIndex * pageSize, (pageIndex - 1L) * pageSize);
                break;
            case SQL_SERVER:
                String orderBy = linkedQueryWrapper.getOrderBy().toString();
                if (StringUtils.isBlank(orderBy)) {
                    orderBy = "ORDER BY CURRENT_TIMESTAMP";
                }
                long start = 1L + (pageIndex - 1L) * pageSize;
                long end = pageIndex * pageSize;
                sql = linkedQueryWrapper.getBaseSql().replaceFirst("select ", "");
                sql = String.format("WITH selectTemp AS (SELECT TOP 100 PERCENT ROW_NUMBER() OVER (%s) as __row_number__, %s) SELECT * FROM selectTemp WHERE __row_number__ BETWEEN %d AND %d ORDER BY __row_number__",
                        orderBy, sql, start, end);
                break;
        }

        List<Object> args = linkedQueryWrapper.getArgs();
        List<Map<String, Object>> mapList = null;
        if (cacheable) {
            String cacheKey = getCacheKey(sql, args, "forMapPage");
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                mapList = (List<Map<String, Object>>) redisTemplate.opsForValue().get(cacheKey);
                page.setRecords(mapList);
                return page;
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
        page.setRecords(mapList);
        return page;
    }

    protected <T> Page<T> queryForObjectPage(Class<T> clazz, LinkedQueryWrapper linkedQueryWrapper, Page<T> page, long pageIndex, long pageSize) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();

        long total = queryForCount(linkedQueryWrapper, false);
        long pages = total % pageSize > 0 ? (total / pageSize) + 1L : total / pageSize;
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);

        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
        DbTypeEnum dbTypeEnum = sqlRunner.getDbTypeEnum();
        String sql;
        switch (dbTypeEnum) {
            default:
                sql = linkedQueryWrapper.getBaseSql() +
                        linkedQueryWrapper.getOrderBy().toString() +
                        String.format(" limit %d,%d", (pageIndex - 1L) * pageSize, pageSize);
                break;
            case DM:
                sql = linkedQueryWrapper.getBaseSql() +
                        linkedQueryWrapper.getOrderBy().toString();
                sql = String.format("SELECT * FROM (SELECT TMP.*, ROWNUM ROW_ID FROM (%s) TMP WHERE ROWNUM <= %d) WHERE ROW_ID > %d",
                        sql, pageIndex * pageSize, (pageIndex - 1L) * pageSize);
                break;
            case SQL_SERVER:
                String orderBy = linkedQueryWrapper.getOrderBy().toString();
                if (StringUtils.isBlank(orderBy)) {
                    orderBy = "ORDER BY CURRENT_TIMESTAMP";
                }
                long start = 1L + (pageIndex - 1L) * pageSize;
                long end = pageIndex * pageSize;
                sql = linkedQueryWrapper.getBaseSql().replaceFirst("select ", "");
                sql = String.format("WITH selectTemp AS (SELECT TOP 100 PERCENT ROW_NUMBER() OVER (%s) as __row_number__, %s) SELECT * FROM selectTemp WHERE __row_number__ BETWEEN %d AND %d ORDER BY __row_number__",
                        orderBy, sql, start, end);
                break;
        }

        List<Object> args = linkedQueryWrapper.getArgs();
        List<T> list = null;
        if (cacheable) {
            String cacheKey = getCacheKey(sql, args, "forObjectPage");
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                list = (List<T>) redisTemplate.opsForValue().get(cacheKey);
                page.setRecords(list);
                return page;
            }
            log.info(sql);
            try {
                list = sqlRunner.selectAll(clazz, sql, args.toArray());
                doCache(cacheKey, list);
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        else {
            log.info(sql);
            try {
                list = sqlRunner.selectAll(clazz, sql, args.toArray());
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        page.setRecords(list);
        return page;
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

    private void doCache(String cacheKey, List<?> list) {
        if (!redisTemplate.hasKey(cacheKey)) {
            redisTemplate.opsForValue().set(cacheKey, list, cacheTimeout, TimeUnit.SECONDS);
            log.info(String.format("cacheKey:%s has bean cached.", cacheKey));
        }
        else {
            log.info(String.format("cacheKey:%s already exists.", cacheKey));
        }
    }

}
