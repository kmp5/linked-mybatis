package com.kzow3n.jdbcplus.core.jdbc;

import org.apache.ibatis.jdbc.Null;
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
public class MySqlRunner {
    private final Connection connection;
    private final TypeHandlerRegistry typeHandlerRegistry;
    private boolean useGeneratedKeySupport;

    public MySqlRunner(Connection connection) {
        this.connection = connection;
        this.typeHandlerRegistry = new TypeHandlerRegistry();
    }

    public void setUseGeneratedKeySupport(boolean useGeneratedKeySupport) {
        this.useGeneratedKeySupport = useGeneratedKeySupport;
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
        PreparedStatement ps = this.connection.prepareStatement(sql);

        List<Map<String, Object>> result;
        try {
            this.setParameters(ps, args);
            ResultSet rs = ps.executeQuery();
            result = this.getResults(rs);
        } finally {
            try {
                ps.close();
            } catch (SQLException ignored) {
            }
        }
        return result;
    }

    public int insert(String sql, Object... args) throws SQLException {
        PreparedStatement ps;
        if (this.useGeneratedKeySupport) {
            ps = this.connection.prepareStatement(sql, 1);
        } else {
            ps = this.connection.prepareStatement(sql);
        }

        int var20;
        try {
            this.setParameters(ps, args);
            ps.executeUpdate();
            if (this.useGeneratedKeySupport) {
                List<Map<String, Object>> keys = this.getResults(ps.getGeneratedKeys());
                if (keys.size() == 1) {
                    Map<String, Object> key = keys.get(0);
                    Iterator<Object> i = key.values().iterator();
                    if (i.hasNext()) {
                        Object genkey = i.next();
                        if (genkey != null) {
                            try {
                                return Integer.parseInt(genkey.toString());
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                }
            }

            var20 = -2147482647;
        } finally {
            try {
                ps.close();
            } catch (SQLException ignored) {
            }

        }

        return var20;
    }

    public int update(String sql, Object... args) throws SQLException {
        PreparedStatement ps = this.connection.prepareStatement(sql);

        int var4;
        try {
            this.setParameters(ps, args);
            var4 = ps.executeUpdate();
        } finally {
            try {
                ps.close();
            } catch (SQLException ignored) {
            }

        }

        return var4;
    }

    public int delete(String sql, Object... args) throws SQLException {
        return this.update(sql, args);
    }

    public void run(String sql) throws SQLException {

        try (Statement stmt = this.connection.createStatement()) {
            stmt.execute(sql);
        }

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
                rowMap.put(rmd.getColumnName(i), rs.getObject(i));
            }
            list.add(rowMap);
        }
        return list;
    }
}
