package com.kzow3n.jdbcplus.core.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.Null;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
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
    protected SqlSession sqlSession;
    private final SqlSessionFactory sqlSessionFactory;
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
            Connection connection = sqlSession.getConnection();
            if (connection.isClosed()) {
                this.sqlSession = sqlSessionFactory.openSession();
                blnOwnSqlSession = true;
            }
        }
        Connection connection = this.sqlSession.getConnection();
        DatabaseMetaData md = connection.getMetaData();
        String dbType = md.getDatabaseProductName().toLowerCase();
        if (dbType.contains("dm")) {
            dbTypeEnum = DbTypeEnum.DM;
        }
        else if (dbType.contains("sqlserver") || dbType.contains("sql server")) {
            dbTypeEnum = DbTypeEnum.SQL_SERVER;
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
        // 得到所有数据
        ResultSetMetaData rmd = rs.getMetaData();
        // 得到列名总数
        int columnCount = rmd.getColumnCount();
        while (rs.next()) {
            Map<String, Object> rowMap = new HashMap<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                rowMap.put(rmd.getColumnLabel(i), rs.getObject(i));
            }
            list.add(rowMap);
        }
        return list;
    }
}
