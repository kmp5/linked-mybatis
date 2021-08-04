package com.kzow3n.jdbcplus.pojo;

import lombok.Data;

/**
 * 表字段信息
 *
 * @author owen
 * @since 2021/8/4
 */
@Data
public class ColumnInfo {

    private Integer tableIndex;

    private String tableColumns;

    private String beanColumns;

    private String columnFormat;
}
