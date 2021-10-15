package com.kzow3n.jdbcplus.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * 字符串工具类
 *
 * @author owen
 * @since 2021/10/14
 */
public class MyStringUtils {

    public static String mapUnderscoreToCamelCase(String str) {
        if (StringUtils.isBlank(str)) {
            return str;
        }
        StringBuilder stringBuilder = new StringBuilder();
        char[] charArray = str.toCharArray();
        for (int i = 0; i < charArray.length; i ++) {
            char charVal = charArray[i];
            if (i == 0) {
                stringBuilder.append(charVal);
                continue;
            }
            if (Character.isUpperCase(charVal)) {
                stringBuilder.append('_');
            }
            stringBuilder.append(charVal);
        }
        return stringBuilder.toString().toLowerCase();
    }
}
