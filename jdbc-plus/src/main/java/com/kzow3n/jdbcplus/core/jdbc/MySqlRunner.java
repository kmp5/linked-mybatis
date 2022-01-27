package com.kzow3n.jdbcplus.core.jdbc;

import com.baomidou.mybatisplus.annotation.DbType;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.executor.resultset.DefaultResultSetHandler;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.Null;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.session.*;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.sql.*;
import java.util.*;

/**
 * 自定义SqlRunner
 *
 * @author owen
 * @since 2021/8/13
 */
@Slf4j
public class MySqlRunner {
    private SqlSession sqlSession;
    private final SqlSessionFactory sqlSessionFactory;
    private Configuration configuration;
    private final TypeHandlerRegistry typeHandlerRegistry;
    private int queryTimeout = 60;
    private DbTypeEnum dbTypeEnum = DbTypeEnum.DEFAULT;
    private boolean blnOwnSqlSession = false;

    public MySqlRunner(SqlSessionFactory sqlSessionFactory, SqlSession sqlSession) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.sqlSession = sqlSession;
        this.typeHandlerRegistry = new TypeHandlerRegistry();
        try {
            initSqlSession();
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
    }

    public MySqlRunner(SqlSessionFactory sqlSessionFactory, SqlSession sqlSession, int queryTimeout) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.sqlSession = sqlSession;
        this.queryTimeout = queryTimeout;
        this.typeHandlerRegistry = new TypeHandlerRegistry();
        try {
            initSqlSession();
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
    }

    private void initSqlSession() throws SQLException {
        if (sqlSession == null) {
            this.sqlSession = sqlSessionFactory.openSession();
            blnOwnSqlSession = true;
        }
        else {
            Connection connection = this.sqlSession.getConnection();
            if (connection.isClosed()) {
                this.sqlSession = sqlSessionFactory.openSession();
                blnOwnSqlSession = true;
            }
        }
        configuration = this.sqlSession.getConfiguration();
        DatabaseMetaData md = this.sqlSession.getConnection().getMetaData();
        String dbType = md.getDatabaseProductName().toLowerCase();
        if (dbType.contains("dm")) {
            dbTypeEnum = DbTypeEnum.DM;
        }
        else if (dbType.contains("sqlserver")) {
            dbTypeEnum = DbTypeEnum.SQL_SERVER;
        }
        else if (dbType.contains("postgresql")) {
            dbTypeEnum = DbTypeEnum.POSTGRE_SQL;
        }
        else if (dbType.contains("oracle")) {
            dbTypeEnum = DbTypeEnum.ORACLE;
        }
    }

    public DbTypeEnum getDbTypeEnum() {
        return dbTypeEnum;
    }

    public Map<String, Object> selectOne(String sql, Object... args) throws SQLException {
        List<Map<String, Object>> results = this.selectAll(sql, args);
        if (results.size() != 1) {
            throw new SQLException("Statement returned " + results.size() + " results where exactly one (1) was expected.");
        } else {
            return results.get(0);
        }
    }

    public List<Map<String, Object>> selectAll(String sql, Object... args) throws SQLException {
        List<Map<String, Object>> result;
        Connection connection = sqlSession.getConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setQueryTimeout(queryTimeout);
        try {
            this.setParameters(ps, args);
            ResultSet rs = ps.executeQuery();
            result = this.getResults(rs);
        }
        finally {
            try {
                if (blnOwnSqlSession) {
                    connection.close();
                    sqlSession.close();
                }
                ps.close();
            } catch (SQLException ignored) {
            }
        }
        return result;
    }

    public <T> List<T> selectAll(Class<T> type, String sql, Object... args) throws SQLException {
        List<T> result;
        Connection connection = sqlSession.getConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setQueryTimeout(queryTimeout);
        try {
            this.setParameters(ps, args);
            ps.executeQuery();
            DefaultResultSetHandler resultSetHandler = getResultSetHandler(type);
            result = (List<T>) resultSetHandler.handleResultSets(ps);
        }
        finally {
            try {
                if (blnOwnSqlSession) {
                    connection.close();
                    sqlSession.close();
                }
                ps.close();
            } catch (SQLException ignored) {
            }
        }
        return result;
    }

    private void setParameters(PreparedStatement ps, Object... args) throws SQLException {
        int i = 0;

        for(int n = args.length; i < n; ++i) {
            if (args[i] == null) {
                throw new SQLException("SqlRunner requires an instance of Null to represent typed null values for JDBC compatibility");
            }

            if (args[i] instanceof Null) {
                ((Null)args[i]).getTypeHandler().setParameter(ps, i + 1, null, ((Null)args[i]).getJdbcType());
            } else {
                TypeHandler typeHandler = this.typeHandlerRegistry.getTypeHandler(args[i].getClass());
                if (typeHandler == null) {
                    throw new SQLException("SqlRunner could not find a TypeHandler instance for " + args[i].getClass());
                }

                typeHandler.setParameter(ps, i + 1, args[i], null);
            }
        }
    }

    private List<Map<String, Object>> getResults(ResultSet rs) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        List<TypeHandler<?>> typeHandlers = new ArrayList<>();
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 0, n = rsmd.getColumnCount(); i < n; i++) {
            columns.add(rsmd.getColumnLabel(i + 1));
            try {
                Class<?> type = Resources.classForName(rsmd.getColumnClassName(i + 1));
                TypeHandler<?> typeHandler = typeHandlerRegistry.getTypeHandler(type);
                if (typeHandler == null) {
                    typeHandler = typeHandlerRegistry.getTypeHandler(Object.class);
                }
                typeHandlers.add(typeHandler);
            } catch (Exception e) {
                typeHandlers.add(typeHandlerRegistry.getTypeHandler(Object.class));
            }
        }
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 0, n = columns.size(); i < n; i++) {
                String name = columns.get(i);
                TypeHandler<?> handler = typeHandlers.get(i);
                row.put(name, handler.getResult(rs, name));
            }
            list.add(row);
        }
        return list;
    }

    private DefaultResultSetHandler getResultSetHandler(Class<?> type) {
        final MappedStatement ms = getMappedStatement(type);
        final RowBounds rowBounds = new RowBounds();
        return new DefaultResultSetHandler(null, ms, null, null, null, rowBounds);
    }

    private MappedStatement getMappedStatement(Class<?> type) {
        return new MappedStatement.Builder(configuration, "linkedSelect", new StaticSqlSource(configuration, "some select statement"), SqlCommandType.SELECT).resultMaps(
            new ArrayList<ResultMap>() {
                {
                    add(new ResultMap.Builder(configuration, "linkedMap", type, new ArrayList<>()).build());
                }
            }).build();
    }
}
