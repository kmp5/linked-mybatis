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

    protected long selectCount(LinkedQueryWrapper linkedQueryWrapper, boolean blnLimit) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();
        StringBuilder fullSqlBuilder = new StringBuilder();
        List<String> groupColumns = linkedQueryWrapper.getGroupColumns();
        String limit = linkedQueryWrapper.getLimit();
        boolean blnAppendLimit = blnLimit && StringUtils.isNotBlank(limit);
        if (CollectionUtils.isEmpty(groupColumns) && !blnAppendLimit) {
            fullSqlBuilder.append(String.format(linkedQueryWrapper.getSqlBuilder().toString(), "count(1) selectCount"));
        }
        else {
            StringBuilder baseSqlBuilder = new StringBuilder();
            baseSqlBuilder.append(linkedQueryWrapper.getBaseSql());
            if (blnAppendLimit) {
                baseSqlBuilder.append(limit);
            }
            fullSqlBuilder.append(String.format("select count(1) selectCount from (%s) t", baseSqlBuilder));
        }
        String sql = fullSqlBuilder.toString();
        List<Object> args = linkedQueryWrapper.getArgs();
        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
        Map<String, Object> map;
        long count = 0L;
        if (cacheable) {
            String cacheKey = getCacheKey(sql, args);
            if (redisTemplate.hasKey(cacheKey)) {
                log.info("get cache from redis.");
                return Long.parseLong(Objects.requireNonNull(redisTemplate.opsForValue().get(cacheKey)).toString());
            }
            log.info(sql);
            try {
                map = sqlRunner.selectOne(sql, args.toArray());
                if (columnCaseInsensitive) {
                    count = Long.parseLong(map.get("SELECTCOUNT").toString());
                }
                else {
                    count = Long.parseLong(map.get("selectCount").toString());
                }
                doCache(cacheKey, count);
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        else {
            log.info(sql);
            try {
                map = sqlRunner.selectOne(sql, args.toArray());
                if (columnCaseInsensitive) {
                    count = Long.parseLong(map.get("SELECTCOUNT").toString());
                }
                else {
                    count = Long.parseLong(map.get("selectCount").toString());
                }
            } catch (SQLException sqlException) {
                log.error(sqlException.getMessage());
            }
        }
        return count;
    }

    protected List<Map<String, Object>> selectList(LinkedQueryWrapper linkedQueryWrapper) {
        checkExecutorValid();
        linkedQueryWrapper.formatSql();
        String sql = linkedQueryWrapper.getFullSql();
        List<Object> args = linkedQueryWrapper.getArgs();

        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
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

        long total = selectCount(linkedQueryWrapper, false);
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
