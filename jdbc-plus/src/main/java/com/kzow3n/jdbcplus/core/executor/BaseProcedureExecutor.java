package com.kzow3n.jdbcplus.core.executor;

import com.kzow3n.jdbcplus.core.jdbc.MySqlRunner;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 存储过程执行器基本方法类
 *
 * @author owen
 * @since 2021/8/28
 */
@Slf4j
public class BaseProcedureExecutor extends BaseExecutor {

    protected List<Map<String, Object>> execPro(String proName, Object... args) {
        checkExecutorValid();
        String sql = getSql(proName, args);
        log.info(sql);
        List<Map<String, Object>> mapList = null;
        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
        try {
            mapList = sqlRunner.selectAll(sql, args);
            //若存储过程中有执行update操作，按需传入redisTemplate清空缓存
            clearCache();
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }

    protected <T> List<T> execPro(Class<T> clazz, String proName, Object... args) {
        checkExecutorValid();
        String sql = getSql(proName, args);
        log.info(sql);
        List<T> list = null;
        MySqlRunner sqlRunner = new MySqlRunner(sqlSessionFactory, sqlSession, queryTimeout);
        try {
            list = sqlRunner.selectAll(clazz, sql, args);
            //若存储过程中有执行update操作，按需传入redisTemplate清空缓存
            clearCache();
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return list;
    }

    private String getSql(String proName, Object... args) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("call %s", proName));
        if (args != null) {
            List<String> formats = new ArrayList<>();
            for (int i = 0;i < args.length;i ++) {
                formats.add("?");
            }
            sqlBuilder.append(String.format("(%s)", String.join(",", formats)));
        }
        return sqlBuilder.toString();
    }

    private void clearCache() {
        if (redisTemplate == null) {
            return;
        }
        Set<String> keys = redisTemplate.keys("linked-mybatis:*");
        if (keys == null) {
            return;
        }
        redisTemplate.delete(keys);
    }
}
