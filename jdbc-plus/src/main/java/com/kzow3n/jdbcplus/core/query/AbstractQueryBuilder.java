package com.kzow3n.jdbcplus.core.query;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.kzow3n.jdbcplus.core.SqlWrapper;
import com.kzow3n.jdbcplus.core.jdbc.MySqlRunner;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSession;
import org.springframework.data.redis.core.RedisTemplate;

import java.sql.SQLException;
import java.util.*;

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

    protected long selectCount(SqlWrapper sqlWrapper) {
        checkBuilderValid();
        sqlWrapper.formatSql();
        String sqlCount = String.format(sqlWrapper.getSqlBuilder().toString(), "count(1) selectCount");
        log.info(sqlCount);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        Map<String, Object> map;
        try {
            map = sqlRunner.selectOne(sqlCount, sqlWrapper.getArgs().toArray());
            return (long) map.get("selectCount");
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return 0;
    }

    protected List<Map<String, Object>> selectList(SqlWrapper sqlWrapper) {
        checkBuilderValid();
        sqlWrapper.formatSql();
        String sql = sqlWrapper.getSql() + sqlWrapper.getOrderBy().toString();
        log.info(sql);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        List<Map<String, Object>> mapList = null;
        try {
            mapList = sqlRunner.selectAll(sql, sqlWrapper.getArgs().toArray());
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }

    protected <T> List<Map<String, Object>> selectPage(SqlWrapper sqlWrapper, Page<T> page, int pageIndex, int pageSize) {
        checkBuilderValid();
        int total = (int) selectCount(sqlWrapper);
        int pages = total % pageSize > 0 ? (total / pageSize) + 1 : total / pageSize;
        page.setTotal(total).setPages(pages).setCurrent(pageIndex).setSize(pageSize);

        sqlWrapper.formatSql();
        String sql = sqlWrapper.getSql() + sqlWrapper.getOrderBy().toString();
        sql += String.format(" limit %d,%d", (pageIndex - 1) * pageSize, pageSize);
        log.info(sql);
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        List<Map<String, Object>> mapList = null;
        try {
            mapList = sqlRunner.selectAll(sql, sqlWrapper.getArgs().toArray());
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
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
}
