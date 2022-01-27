package com.kzow3n.jdbcplus.core.jdbc;

/**
 * 数据库类型枚举
 *
 * @author owen
 * @since 2021/10/13
 */
public enum DbTypeEnum {

    /**
     * 默认:mysql
     */
    DEFAULT(1, "默认:mysql"),
    /**
     * sqlserver
     */
    SQL_SERVER(2, "sqlserver"),
    /**
     * 达梦
     */
    DM(3, "达梦"),
    /**
     * postgresql
     */
    POSTGRE_SQL(4, "postgresql"),
    /**
     * oracle
     */
    ORACLE(5, "oracle")
    ;

    private Integer key;
    private String value;

    DbTypeEnum(Integer key, String value) {
        this.key = key;
        this.value = value;
    }

    public Integer getKey() {
        return key;
    }
    public void setKey(Integer key) {
        this.key = key;
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
