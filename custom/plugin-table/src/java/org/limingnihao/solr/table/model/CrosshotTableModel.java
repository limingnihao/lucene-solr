package org.limingnihao.solr.table.model;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 交叉查表
 * key的查询为variables中定义的所有变量的笛卡尔积
 * <pre>
 * "variables": {
 *     "id_1": "{!func} ${id:0}",
 *     "id_2": "{!func} id"
 * },
 * "tables":[
 *     { "key": "1_1", "value": "0.1" },
 *     { "key": "1_2", "value": "0.2" },
 *     { "key": "3_3", "value": "0.3" },
 *     { "key": "4_4", "value": "0.4" }
 * ] </pre>
 *
 * @author shiming.li
 */
public class CrosshotTableModel extends OnehotTableModel {
    public CrosshotTableModel(String name, Map<String, Object> datas) {
        super(name, datas);
    }

    @Override
    public float[] floatVals(String[] variables) {
        if (variables != null && variables.length > 0) {
            List<String> keyList = new ArrayList<>();
            for (int i = 0; i < variables.length; i++) {
                keyList = getCrossKey(keyList, variables[i]);
            }
            for (String key : keyList) {
                String data = this.tables.get(key);
                if (StringUtils.isNotBlank(data)) {
                    return new float[]{Float.parseFloat(data)};
                }
            }
        }
        return null;
    }

    /**
     * 多个list的笛卡尔积组合
     *
     * @param listA 原始list
     * @param val   链接的字符串
     * @return 链接后的key
     */
    public static List<String> getCrossKey(List<String> listA, String val) {
        List<String> listB = Arrays.asList(val.split(FLAG_INPUT_SPLIT)).stream().filter(v -> StringUtils.isNotBlank(v)).collect(Collectors.toList());
        List<String> result = new ArrayList<>();
        if (listA == null || listA.isEmpty()) {
            return listB;
        }
        if (listB == null || listB.isEmpty()) {
            return listA;
        }
        for (String a : listA) {
            for (String b : listB) {
                result.add(a + FLAG_KEY_JOIN + b);
            }
        }
        return result;
    }

}
