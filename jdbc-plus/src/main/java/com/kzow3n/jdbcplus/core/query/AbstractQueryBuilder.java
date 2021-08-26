package com.kzow3n.jdbcplus.core.query;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.kzow3n.jdbcplus.core.SqlWrapper;
import com.kzow3n.jdbcplus.core.jdbc.MySqlRunner;
import com.kzow3n.jdbcplus.pojo.SqlArg;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSession;
import org.springframework.data.redis.core.RedisTemplate;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 查询器基本方法类
 *
 * @author owen
 * @since 2021/8/26
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class AbstractQueryBuilder extends QueryBuilderBase {

    protected SqlSession sqlSession;
    protected RedisTemplate<String, Object> redisTemplate;
    protected Boolean cacheable = false;
    protected Long timeout = 60L;

    protected long selectCount(SqlWrapper sqlWrapper) {
        checkBuilderValid();
        sqlWrapper.formatSql();
        String sqlCount = String.format(sqlWrapper.getSqlBuilder().toString(), "count(1) selectCount");
        List<Object> args = sqlWrapper.getArgs();
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
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
                redisTemplate.opsForValue().set(cacheKey, count, timeout, TimeUnit.SECONDS);
                log.info(String.format("cacheKey:%s", cacheKey));
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

    protected List<Map<String, Object>> selectList(SqlWrapper sqlWrapper) {
        checkBuilderValid();
        sqlWrapper.formatSql();
        String sql = sqlWrapper.getSql() + sqlWrapper.getOrderBy().toString();
        List<Object> args = sqlWrapper.getArgs();
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
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
                redisTemplate.opsForValue().set(cacheKey, mapList, timeout, TimeUnit.SECONDS);
                log.info(String.format("cacheKey:%s", cacheKey));
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

    protected <T> List<Map<String, Object>> selectPage(SqlWrapper sqlWrapper, Page<T> page, long pageIndex, long pageSize) {
        checkBuilderValid();
        long total = selectCount(sqlWrapper);
        long pages = total % pageSize > 0 ? (total / pageSize) + 1L : total / pageSize;
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);

        sqlWrapper.formatSql();
        String sql = sqlWrapper.getSql() + sqlWrapper.getOrderBy().toString();
        sql += String.format(" limit %d,%d", (pageIndex - 1L) * pageSize, pageSize);
        List<Object> args = sqlWrapper.getArgs();
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
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
                redisTemplate.opsForValue().set(cacheKey, mapList, timeout, TimeUnit.SECONDS);
                log.info(String.format("cacheKey:%s", cacheKey));
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

    protected List<Map<String, Object>> execPro(String proName, Object... args) {
        checkBuilderValid();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("call %s", proName));
        if (args != null) {
            List<String> formats = new ArrayList<>();
            for (int i = 0;i < args.length;i ++) {
                formats.add("?");
            }
            sqlBuilder.append(String.format("(%s)", String.join(",", formats)));
        }
        String sql = sqlBuilder.toString();
        log.info(sql);
        List<Map<String, Object>> mapList = null;
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        try {
            mapList = sqlRunner.selectAll(sql, args);
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }

    private void checkBuilderValid() {
        if (sqlSession == null) {
            throw new NullPointerException("SqlSession Could not be null.");
        }
        if (cacheable && redisTemplate == null) {
            throw new NullPointerException("RedisTemplate Could not be null.");
        }
    }

    private String getCacheKey(String sql, List<Object> args) {
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
}
