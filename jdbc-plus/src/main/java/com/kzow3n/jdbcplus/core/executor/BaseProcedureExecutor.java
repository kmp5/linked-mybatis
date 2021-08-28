package com.kzow3n.jdbcplus.core.executor;

import com.kzow3n.jdbcplus.core.jdbc.MySqlRunner;
import com.kzow3n.jdbcplus.utils.ClazzUtils;
import com.kzow3n.jdbcplus.utils.ColumnUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 存储过程执行器基本方法类
 *
 * @author owen
 * @since 2021/8/28
 */
@Slf4j
public class BaseProcedureExecutor extends Executor {

    protected List<Map<String, Object>> execPro(String proName, Object... args) {
        checkExecutorValid();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("call %s", proName));
        if (args != null) {
            List<String> formats = new ArrayList<>();
            for (int i = 0;i < args.length;i ++) {
                formats.add("?");
            }
            sqlBuilder.append(String.format("(%s)", String.join(",", formats)));
        }
        String sql = sqlBuilder.toString();
        log.info(sql);
        List<Map<String, Object>> mapList = null;
        MySqlRunner sqlRunner = new MySqlRunner(sqlSession.getConnection());
        try {
            mapList = sqlRunner.selectAll(sql, args);
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage());
        }
        return mapList;
    }

    protected void updateMapsKeys(List<Map<String, Object>> mapList, Class<?> clazz) {
        List<Field> fields = ClazzUtils.getAllFields(clazz);
        for (Field field : fields) {
            String tableColumn = ColumnUtils.getTableColumnByField(field);
            if (StringUtils.isNotBlank(tableColumn)) {
                String beanColumn = field.getName();
                mapList.forEach(map -> {
                    map.put(beanColumn, map.remove(tableColumn));
                });
            }
        }
    }

    protected void updateMapKeys(Map<String, Object> map, Class<?> clazz) {
        List<Field> fields = ClazzUtils.getAllFields(clazz);
        for (Field field : fields) {
            String tableColumn = ColumnUtils.getTableColumnByField(field);
            if (StringUtils.isNotBlank(tableColumn)) {
                String beanColumn = field.getName();
                map.put(beanColumn, map.remove(tableColumn));
            }
        }
    }
}
