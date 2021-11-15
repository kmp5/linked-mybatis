package com.kzow3n.jdbcplus.core.executor;

import com.alibaba.fastjson.JSON;
import com.kzow3n.jdbcplus.pojo.SqlArg;
import lombok.Data;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * 执行器基类
 *
 * @author owen
 * @since 2021/8/28
 */
@Data
public class BaseExecutor {

    protected SqlSession sqlSession;
    protected SqlSessionFactory sqlSessionFactory;
    protected RedisTemplate<String, Object> redisTemplate;
    protected Boolean cacheable = false;
    protected Long cacheTimeout = 60L;
    protected Integer queryTimeout = 60;

    protected boolean mapUnderscoreToCamelCase = true;

    protected void checkExecutorValid() {
        if (sqlSessionFactory == null) {
            throw new NullPointerException("sqlSessionFactory could not be null.");
        }
        if (cacheable && redisTemplate == null) {
            throw new NullPointerException("redisTemplate could not be null.");
        }
    }

    protected void initConfiguration() {
        Configuration configuration = sqlSessionFactory.getConfiguration();
        mapUnderscoreToCamelCase = configuration.isMapUnderscoreToCamelCase();
    }

    protected String getCacheKey(String sql, List<Object> args, String type) {
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
        stringBuilder.append("linked-mybatis:").append(type).append(":").append(sql).append(":").append(json);
        return stringBuilder.toString();
    }
}
