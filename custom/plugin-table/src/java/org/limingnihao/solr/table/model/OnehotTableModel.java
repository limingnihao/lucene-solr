package org.limingnihao.solr.table.model;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.request.SolrQueryRequest;

import java.util.*;
import java.util.stream.Collectors;

/**
 * one hot 查表
 * 使用variables查询相同key，对应的value
 * 当variables为空返回def。
 * 当variables长度大于1，只取第一个元素
 * <pre>
 * "variables": {
 * "id": "{!func} ${id:0}",
 * },
 * tables: [
 * { "key": "1", "value": "1000" },
 * { "key": "2", "value": "0100" },
 * { "key": "3", "value": "0010" },
 * { "key": "4", "value": "0001" }
 * ]</pre>
 *
 * @author shiming.li
 */
public class OnehotTableModel extends TableModel {

    protected Map<String, String> tables;

    public OnehotTableModel(String name, Map<String, Object> datas) {
        super(name, datas);
    }

    @Override
    public void setTables(List<Map> tableList) {
        this.tables = new HashMap<>(tableList.size());
        for (Map r : tableList) {
            Object keyObj = r.get("key");
            Object valueObj = r.get("value");
            if (keyObj != null && valueObj != null) {
                tables.put(keyObj.toString(), valueObj.toString());
            }
        }
    }

    @Override
    public float[] floatVals(String[] variables) {
        this.variableValues = variables;
        String result = "";
        if (variables != null && variables.length > 0) {
            result = this.tables.get(variables[0]);
        }
        return complement(result, this.getSize(), this.getDef());
    }

    @Override
    public Explanation explain(String[] variables, int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext) {
        final List<Explanation> variableExplanations = new ArrayList<>();
        for (int i = 0; i < this.variableNames.length; i++) {
            this.variableValues[i] = variables[i];
            String description = "[" + i + "]: {name=" + this.variableNames[i] + ", value=" + this.variableValues[i] + "}";
            Explanation explanation = Explanation.match(Float.parseFloat(this.variableValues[i]), description);
            variableExplanations.add(explanation);
        }
        float[] floatVal = floatVals(variables);
        String description = "OnehotTableModel:{variableValues: " + Arrays.toString(this.variableValues) + ", floatVal: " + Arrays.toString(floatVal) + ", tables: " + this.tables.toString() + "}, variables of: ";
        Explanation explanation = Explanation.match(0f, description, variableExplanations);
        return explanation;
    }

    @Override
    public String toString(String[] variables, int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext) {
        float[] floatVal = floatVals(variables);
        String description = "OnehotTableModel:{variableValues: " + Arrays.toString(this.variableValues) + ", floatVal: " + Arrays.toString(floatVal) + ", tables: " + this.tables.toString() + "}";
        return description;
    }

    /**
     * 补位字符串，并用joining间隔开
     *
     * @param val  原始字符串
     * @param size 字符串需要的大小
     * @param def  默认补位的字符
     * @return 根据def补位后的字符串
     */
    public static float[] complement(String val, Integer size, float def) {
        if (size == null || val == null || "".equals(val)) {
            return null;
        }
        char[] val_char = val.toCharArray();
        int current = val_char.length;
        float[] results = new float[size];
        for (int i = 0; i < current; i++) {
            results[i] = Float.parseFloat(String.valueOf(val_char[i]));
        }
        for (int i = current; i < size; i++) {
            results[i] = def;
        }
        return results;
    }

}
