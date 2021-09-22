package com.kzow3n.jdbcplus.core.executor;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * 存储过程执行器的构造器
 *
 * @author owen
 * @since 2021/9/22
 */
public class ProcedureExecutorBuilder {

    private SqlSession sqlSession;
    private final SqlSessionFactory sqlSessionFactory;
    private RedisTemplate<String, Object> redisTemplate;
    private int queryTimeout = 60;

    public ProcedureExecutorBuilder(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    public ProcedureExecutorBuilder(SqlSessionFactory sqlSessionFactory, SqlSession sqlSession) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.sqlSession = sqlSession;
    }

    public ProcedureExecutorBuilder queryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
        return this;
    }

    public ProcedureExecutorBuilder redisTemplate(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
        return this;
    }

    public ProcedureExecutor build() {
        ProcedureExecutor executor = new ProcedureExecutor(sqlSessionFactory);
        if (sqlSession != null) {
            executor.setSqlSession(sqlSession);
        }
        executor.setQueryTimeout(queryTimeout);
        if (redisTemplate != null) {
            executor.setRedisTemplate(redisTemplate);
        }
        return executor;
    }
}
