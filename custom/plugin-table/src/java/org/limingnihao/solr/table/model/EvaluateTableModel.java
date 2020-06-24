package org.limingnihao.solr.table.model;

import org.limingnihao.solr.table.util.EvalExp4jUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.request.SolrQueryRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;

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
public class EvaluateTableModel extends TableModel {

    private List<Evaluate> tables;

    public EvaluateTableModel(String name, Map<String, Object> datas) {
        super(name, datas);
    }

    @Override
    public void setTables(List<Map> tableList) {
        this.tables = new ArrayList<>();
        for (Map map : tableList) {
            Object conditionObj = map.get("condition");
            Object valuesObj = map.get("values");
            Evaluate evaluate = new Evaluate();
            if (conditionObj != null) {
                evaluate.setCondition(conditionObj.toString());
            }
            if (valuesObj instanceof List) {
                evaluate.setValues((List<String>) valuesObj);
            }
            this.tables.add(evaluate);
        }
    }

    @Override
    public float[] floatVals(String[] variables) {
        return null;
    }

    @Override
    public float[] floatVals(String[] variables, int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext) {
        float[] variableFloats = new float[variables.length];
        for (int i = 0; i < variables.length; i++) {
            this.variableValues[i] = variables[i];
            variableFloats[i] = Float.parseFloat(variables[i]);
        }
        for (Evaluate eval : this.tables) {
            if (StringUtils.isBlank(eval.getCondition()) || (evalBoolean(doc, req, context, readerContext, eval.getCondition(), this.variableNames, variableFloats))) {
                float[] floatVals = new float[eval.getValues().size()];
                for (int i = 0; i < eval.getValues().size(); i++) {
                    float floatVal = evalFloat(doc, req, context, readerContext, eval.getValues().get(i), this.variableNames, variableFloats);
                    floatVals[i] = floatVal;
                }
                return floatVals;
            }
        }
        return null;
    }

    boolean evalBoolean(int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext, String expression, String[] keys, float[] values) {
        return EvalExp4jUtil.eval(expression, keys, values) > 0;
    }

    float evalFloat(int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext, String expression, String[] keys, float[] values) {
        return EvalExp4jUtil.eval(expression, keys, values);
    }

    @Override
    public Explanation explain(String[] variables, int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext) {
        float[] variableFloats = new float[variables.length];

        final List<Explanation> variableExplanations = new ArrayList<>();
        for (int i = 0; i < this.variableNames.length; i++) {
            this.variableValues[i] = variables[i];
            variableFloats[i] = Float.parseFloat(variables[i]);

            String description = "[" + i + "]: {name=" + this.variableNames[i] + ", value=" + this.variableValues[i] + "}";
            Explanation explanation = Explanation.match(Float.parseFloat(this.variableValues[i]), description);
            variableExplanations.add(explanation);
        }
        Explanation conditionExplanation = null;
        for (Evaluate eval : this.tables) {
            final List<Explanation> conditionValueExplanations = new ArrayList<>();
            if (StringUtils.isBlank(eval.getCondition()) || (evalBoolean(doc, req, context, readerContext, eval.getCondition(), this.variableNames, variableFloats))) {
                float[] floatVals = new float[eval.getValues().size()];
                for (int i = 0; i < eval.getValues().size(); i++) {
                    float floatVal = evalFloat(doc, req, context, readerContext, eval.getValues().get(i), this.variableNames, variableFloats);
                    floatVals[i] = floatVal;
                    conditionValueExplanations.add(Explanation.match(floatVals[i], "vParser=" + eval.getValues().get(i) + ", floatVal=" + floatVals[i]));
                }
                conditionExplanation = Explanation.match(0f, "condition=" + eval.getCondition() + ", floatVals:" + Arrays.toString(floatVals) + ", values of: ", conditionValueExplanations);
                break;
            }
        }

        Explanation variableExplanation = Explanation.match(0f, "variable details of: ", variableExplanations);

        // 将feature和model的explanation进行合并
        final List<Explanation> functionExplanations = new ArrayList<>();
        functionExplanations.add(variableExplanation);
        functionExplanations.add(conditionExplanation);
        Explanation explanation = Explanation.match(0f, "EvaluateTableModel:{tables: " + this.tables.toString() + "}, details of: ", functionExplanations);
        return explanation;
    }

    @Override
    public String toString(String[] variables, int doc, SolrQueryRequest req, Map context, LeafReaderContext readerContext) {
        float[] floatVal = floatVals(variables, doc, req, context, readerContext);
        String description = "EvaluateTableModel:{variableValues: " + Arrays.toString(this.variableValues) + ", floatVal: " + Arrays.toString(floatVal) + ", tables: " + this.tables.toString() + "}";
        return description;
    }

    /**
     * 计算表达式
     */
    class Evaluate {
        protected String condition;
        protected List<String> values;

        @Override
        public String toString() {
            return "Evaluate{" +
                    "condition='" + condition + '\'' +
                    ", values=" + Arrays.toString(values.toArray()) +
                    '}';
        }

        public String getCondition() {
            return condition;
        }

        public void setCondition(String condition) {
            this.condition = condition;
        }

        public List<String> getValues() {
            return values;
        }

        public void setValues(List<String> values) {
            this.values = values;
        }
    }
}
