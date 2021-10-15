package com.kzow3n.jdbcplus.core.executor;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * 查询执行器的构造器
 *
 * @author owen
 * @since 2021/9/22
 */
public class LinkedQueryExecutorBuilder {

    private SqlSession sqlSession;
    private final SqlSessionFactory sqlSessionFactory;
    private RedisTemplate<String, Object> redisTemplate;
    private boolean cacheable = false;
    private long cacheTimeout = 60L;
    private int queryTimeout = 60;
    protected boolean columnCaseInsensitive = false;

    public LinkedQueryExecutorBuilder(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    public LinkedQueryExecutorBuilder(SqlSessionFactory sqlSessionFactory, SqlSession sqlSession) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.sqlSession = sqlSession;
    }

    public LinkedQueryExecutorBuilder queryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
        return this;
    }

    public LinkedQueryExecutorBuilder cacheable(boolean cacheable) {
        this.cacheable = cacheable;
        return this;
    }

    public LinkedQueryExecutorBuilder redisTemplate(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
        return this;
    }

    public LinkedQueryExecutorBuilder cacheTimeout(long cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
        return this;
    }

    public LinkedQueryExecutorBuilder columnCaseInsensitive(boolean columnCaseInsensitive) {
        this.columnCaseInsensitive = columnCaseInsensitive;
        return this;
    }

    public LinkedQueryExecutor build() {
        LinkedQueryExecutor executor = new LinkedQueryExecutor(sqlSessionFactory);
        if (sqlSession != null) {
            executor.setSqlSession(sqlSession);
        }
        executor.setQueryTimeout(queryTimeout);
        executor.setCacheable(cacheable);
        if (redisTemplate != null) {
            executor.setRedisTemplate(redisTemplate);
        }
        executor.setCacheTimeout(cacheTimeout);
        executor.setColumnCaseInsensitive(columnCaseInsensitive);
        return executor;
    }
}
