package org.limingnihao.solr.table.model;

import org.limingnihao.solr.table.util.EvalExp4jUtil;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.request.SolrQueryRequest;

import java.util.Map;

/**
 * evaluate 表达式查表
 * variables 后续表达式需要的变量
 * tables查表逻辑
 * 当配置condition时，必须满足condition的条件
 * <pre>
 * "variables": {
 *     "a": "{!func} ${a:0}",
 *     "b": "{!func} ${b:0}",
 *     "c": "{!func} ${c:0}",
 *     "d": "{!func} ${d:0}"
 *     },
 * "tables": [
 *     {
 *     "condition": "a 大于 b",
 *         "values": [
 *         "(a - b) / a ",
 *         "0",
 *         "0"
 *     ]
 * }]</pre>
 */
public class EvalExp4jTableModel extends EvaluateTableModel {

    public EvalExp4jTableModel(String name, Map<String, Object> datas) {
        super(name, datas);
    }

    @Override
    boolean evalBoolean(int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext, String expression, String[] keys, float[] values) {
        return EvalExp4jUtil.eval(expression, keys, values) > 0;
    }

    @Override
    float evalFloat(int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext, String expression, String[] keys, float[] values) {
        return EvalExp4jUtil.eval(expression, keys, values);
    }

}
