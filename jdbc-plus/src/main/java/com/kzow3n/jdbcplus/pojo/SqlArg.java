package com.kzow3n.jdbcplus.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 查询参数属性
 *
 * @author owen
 * @since 2021/8/26
 */
@Data
@AllArgsConstructor
public class SqlArg {

    private Object value;
    private String className;
}
