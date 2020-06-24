package org.limingnihao.solr.table.model;

import org.limingnihao.solr.table.util.EvalSolrUtil;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.request.SolrQueryRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * evaluate 表达式查表
 * variables 后续表达式需要的变量Ø
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
 *     "condition": "lt(${a}, ${b})",
 *         "values": [
 *         "{!func} div(sub(${b}, ${a}), ${b})",
 *         "0",
 *         "0"
 *     ]
 * }]</pre>
 */
public class EvalSolrTableModel extends EvaluateTableModel {

    public EvalSolrTableModel(String name, Map<String, Object> datas) {
        super(name, datas);
    }

    @Override
    boolean evalBoolean(int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext, String expression, String[] keys, float[] values) {
        Map<String, String[]> orig = extracFeatureParams(req.getOriginalParams());
        return EvalSolrUtil.evalBoolean(doc, req, context, readerContext, expression, orig);
    }

    @Override
    float evalFloat(int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext, String expression, String[] keys, float[] values) {
        Map<String, String[]> orig = extracFeatureParams(req.getOriginalParams());
        return EvalSolrUtil.evalFloat(doc, req, context, readerContext, expression, orig);
    }

}
