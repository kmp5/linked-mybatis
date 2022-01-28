package com.kzow3n.jdbcplus.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 表信息
 *
 * @author owen
 * @since 2021/8/4
 */
@Data
@NoArgsConstructor
public class TableInfo {

    public TableInfo(Integer tableIndex, Boolean formatBeanColumn) {
        this.tableIndex = tableIndex;
        this.formatBeanColumn = formatBeanColumn;
    }

    public TableInfo(String tableId, Class<?> tableClass) {
        this.tableId = tableId;
        this.tableClass = tableClass;
    }

    private Integer tableIndex;
    private Boolean formatBeanColumn = true;
    private String tableId;
    private Class<?> tableClass;
}
