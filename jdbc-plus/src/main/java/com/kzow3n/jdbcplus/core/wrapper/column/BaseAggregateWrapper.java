package com.kzow3n.jdbcplus.core.wrapper.column;

import com.kzow3n.jdbcplus.pojo.TableInfo;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 聚合Column构造器基本方法类
 *
 * @author owen
 * @since 2021/8/10
 */
@Data
public class BaseAggregateWrapper {
    protected List<TableInfo> tableInfos;
    protected String column;

    private void init() {
        tableInfos = new ArrayList<>();
        column = "";
    }

    public BaseAggregateWrapper() {
        init();
    }

    protected TableInfo getTableInfoByIndex(Integer tableIndex) {
        if (tableIndex == null) {
            return null;
        }
        if (tableIndex < 1) {
            return null;
        }
        return tableInfos.get(tableIndex - 1);
    }
}
