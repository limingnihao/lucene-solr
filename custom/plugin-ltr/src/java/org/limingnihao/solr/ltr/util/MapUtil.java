package org.limingnihao.solr.ltr.util;


import org.eclipse.jetty.util.StringUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * mapValue  查询工具类
 * 用于解析map返回值，和根据offset获取对应的值
 *
 * @author shiming.li
 * @date 2020-04-30
 */
public class MapUtil {

    // 组装key的链接字符
    public static final String FLAG_KEY_JOIN = "_";

    // 返回结果多值链接字符
    public static final String FLAG_RESULT_JOIN = ",";

    /**
     * 解析计算后结果为map
     * <p>
     * key为：使用 name + FLAG_KEY_JOIN + [数组下标]
     * value为：结果使用 FLAG_RESULT_JOIN 解析为数组的每个元素。
     *
     * @param name  featrue名称
     * @param value 函数计算得值
     * @return k-v的值
     */
    public static Map<String, Float> analysis(String name, String value) {
        Map<String, Float> result = new HashMap<>();
        if (StringUtil.isNotBlank(value)) {
            String[] values = value.split(FLAG_RESULT_JOIN);
            for (int i = 0; i < values.length; i++) {
                Float number = 0f;
                try {
                    number = Float.parseFloat(values[i]);
                } catch (Exception e) {
                }
                result.put(name + FLAG_KEY_JOIN + i, number);
            }
        }
        return result;
    }


    /**
     * 解析计算后结果为map
     * <p>
     * key为：使用 name + FLAG_KEY_JOIN + [数组下标]
     * value为：结果使用 FLAG_RESULT_JOIN 解析为数组的每个元素。
     *
     * @param name  featrue名称
     * @param value 函数计算得值
     */
    public static void analysis(String name, String value, Map<String, Float> data) {
        if (StringUtil.isNotBlank(value)) {
            String[] values = value.split(FLAG_RESULT_JOIN);
            for (int i = 0; i < values.length; i++) {
                float number = 0f;
                try {
                    number = Float.parseFloat(values[i]);
                } catch (Exception e) {
                }
                data.put(name + FLAG_KEY_JOIN + i, number);
            }
        }
    }

    /**
     * 根据指定的key和offset，在map中获取对应的float的值
     *
     * @param key    需要的map的name
     * @param offset 偏移量
     * @param map    map数据
     * @return 获取具体的值
     */
    public static float getFloat(String key, int offset, Map<String, Float> map) {
        if (map == null || map.isEmpty()) {
            return 0f;
        }
        String map_key = key + FLAG_KEY_JOIN + offset;
        if (!map.containsKey(map_key)) {
            return 0f;
        }
        return map.get(map_key);
    }
}
