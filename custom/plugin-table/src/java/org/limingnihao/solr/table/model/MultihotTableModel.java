package org.limingnihao.solr.table.model;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * multi hot 查表
 * 使用variables查询相同key，对应的value
 * 当variables为空返回def。
 * 当variables长度大于1，只取第一个元素。
 * 当取值为多值，将匹配的结果按位相加
 * <pre>
 * "variables": {
 *     "id": "{!func} ${id:0}",
 * },
 * tables: [
 *     { "key": "1", "value": "1000" },
 *     { "key": "2", "value": "0100" },
 *     { "key": "3", "value": "0010" },
 *     { "key": "4", "value": "0001" }
 * ]</pre>
 *
 * @author shiming.li
 */
public class MultihotTableModel extends OnehotTableModel {

    public MultihotTableModel(String name, Map<String, Object> datas) {
        super(name, datas);
    }

    @Override
    public float[] floatVals(String[] variables) {
        String result = "";
        if (variables != null && variables.length > 0) {
            for (int i = 0; i < variables.length; i++) {
                List<String> vList = Arrays.asList(variables[i].split(FLAG_INPUT_SPLIT)).stream().filter(v -> StringUtils.isNotBlank(v)).collect(Collectors.toList());
                for (String v : vList) {
                    String data = this.tables.get(v);
                    if (data != null) {
                        result = operationAnd(result, data);
                    }
                }
            }
        }
        return complement(result, this.getSize(), this.getDef());
    }

    /**
     * 两个字符串做与运算
     *
     * @param a 第一个字符串
     * @param b 第二个字符串
     * @return 返回按位进行或运算结果
     */
    public static String operationAnd(String a, String b) {
        if (StringUtils.isBlank(a) && StringUtils.isBlank(b)) {
            return "";
        }
        if (StringUtils.isBlank(a)) {
            return b;
        }
        if (StringUtils.isBlank(b)) {
            return a;
        }
        int length = Math.max(a.toCharArray().length, b.toCharArray().length);
        String result = "";
        for (int i = 0; i < length; i++) {
            char c1, c2;
            if (i >= a.toCharArray().length) {
                c1 = '0';
            } else {
                c1 = a.toCharArray()[i];
            }
            if (i >= b.toCharArray().length) {
                c2 = '0';
            } else {
                c2 = b.toCharArray()[i];
            }
            result += String.valueOf((char) (c1 | c2));
        }
        return result;
    }
}
