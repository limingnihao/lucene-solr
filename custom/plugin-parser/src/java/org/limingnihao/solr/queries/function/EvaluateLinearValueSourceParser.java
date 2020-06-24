package org.limingnihao.solr.queries.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.request.macro.MacroExpander;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.util.RTimerTree;
import org.limingnihao.solr.util.EvaluateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * 线性公式计算函数
 * <p>
 * 第一个参数计算公式， 例如：f1+f2
 * 入参已$开头将从参数中获取，否则直接使用取值
 * <p>
 * 第二个参数feature的json字符串， 例如：
 * [
 * {
 * "name": "f1",
 * "query": "{!func}div(1,2)"
 * },
 * {
 * "name": "f3",
 * "query": "{!func}query({!edismax df='subject' v='计算机 物理'})"
 * }
 * ]
 *
 * @author shiming.li
 * @date 2020-04-15
 */
public class EvaluateLinearValueSourceParser extends ValueSourceParser {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    protected final String name = "evaluate";
    private boolean isDebugTimings = false;
    private long[] timings;
    private int count = 0;

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String expressionParam = fp.parseArg();
        String featuresParam = fp.parseArg();
        if (StringUtils.isBlank(expressionParam)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `features` is empty!");
        }
        if (StringUtils.isBlank(featuresParam)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `expression` is empty!");
        }
        expressionParam = expressionParam.trim();
        featuresParam = featuresParam.trim();
        String expression = expressionParam;
        String features = featuresParam;
        if (expressionParam.startsWith("$")) {
            expression = fp.getParam(expressionParam.substring(1));
        }
        if (featuresParam.startsWith("$")) {
            features = fp.getParam(featuresParam.substring(1));
        }
        if (StringUtils.isBlank(features)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `features` is empty!");
        }
        if (StringUtils.isBlank(expression)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `expression` is empty!");
        }

        // 解析feature
        List<FeatureInfo> featuerList = new ArrayList();
        JSONArray jsonArray = JSONArray.parseArray(features);
        if (jsonArray == null || jsonArray.isEmpty()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the param of `features` is empty!");
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            if (jsonObject.containsKey("name")) {
                featuerList.add(new FeatureInfo(jsonObject.getString("name"), jsonObject.getString("query")));
            } else {
                featuerList.add(new FeatureInfo(jsonObject.getString("n"), jsonObject.getString("q")));
            }
        }

        // 解析所有feature为 valueSource
        final List<ValueSource> valueSources = Lists.newArrayList();
        final List<String> featureNames = Lists.newArrayList();
        if (featuerList != null) {
            for (final FeatureInfo feature : featuerList) {
                valueSources.add(getValueSource(feature.getQuery(), fp.getReq()));
                featureNames.add(feature.getName());
            }
        }
        this.isDebugTimings = SolrRequestInfo.getRequestInfo().getResponseBuilder().isDebugTimings();
        if (this.isDebugTimings) {
            this.timings = new long[featureNames.size() + 1];
            for (int i = 0; i < timings.length; i++) {
                this.timings[i] = 0;
            }
        }
        count = 0;
        return new EvaluateLinearValueSource(expression, valueSources, featureNames);
    }

    public class EvaluateLinearValueSource extends ValueSource {
        private final String expression;
        private final List<ValueSource> valueSources;
        private final List<String> featureNames;

        public EvaluateLinearValueSource(String expression, List<ValueSource> valueSources, List<String> featureNames) throws SyntaxError {
            this.expression = expression;
            this.valueSources = valueSources;
            this.featureNames = featureNames;
        }

        @Override
        public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            FunctionValues[] valFunctions = new FunctionValues[valueSources.size()];
            for (int i = 0, len = valueSources.size(); i < len; i++) {
                valFunctions[i] = this.valueSources.get(i).getValues(context, readerContext);
            }
            return new EvaluateLinearFunctionValues(expression, valFunctions, featureNames.toArray(new String[]{}));
        }

        @Override
        public boolean equals(Object o) {
            if (this.getClass() != o.getClass()) {
                return false;
            }
            EvaluateLinearValueSource other = (EvaluateLinearValueSource) o;
            return this.valueSources.equals(other.valueSources);
        }

        @Override
        public int hashCode() {
            return valueSources.hashCode() + name.hashCode();
        }

        @Override
        public String description() {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append('(');
            boolean firstTime = true;
            for (ValueSource source : valueSources) {
                if (firstTime) {
                    firstTime = false;
                } else {
                    sb.append(',');
                }
                sb.append(source);
            }
            sb.append(')');
            return sb.toString();
        }
    }

    public class EvaluateLinearFunctionValues extends FunctionValues {
        private final String expression;
        private final FunctionValues[] valFunctions;
        private final String[] featureNames;

        public EvaluateLinearFunctionValues(String expression, FunctionValues[] valFunctions, String[] featureNames) {
            this.expression = expression;
            this.valFunctions = valFunctions;
            this.featureNames = featureNames;
        }

        @Override
        public float floatVal(int doc) throws IOException {
            count++;
            log.info("doc: {}, count: {}", doc, count);
            if (isDebugTimings) {
                float[] floatValues = this.getValuesWithTiming(doc);
                long current = System.nanoTime();
                float score = this.evaluate(floatValues);
                timings[timings.length - 1] += (System.nanoTime() - current);
                return score;
            } else {
                return this.evaluate(this.getValues(doc));
            }
        }

        /**
         * 获取feature的得分
         */
        public float[] getValues(int doc) throws IOException {
            float[] functionValues = new float[valFunctions.length];
            for (int i = 0; i < valFunctions.length; i++) {
                functionValues[i] = valFunctions[i].floatVal(doc);
            }
            return functionValues;
        }

        /**
         * 获取feature的得分
         */
        public float[] getValuesWithTiming(int doc) throws IOException {
            float[] functionValues = new float[valFunctions.length];
            for (int i = 0; i < valFunctions.length; i++) {
                long current = System.nanoTime();
                functionValues[i] = valFunctions[i].floatVal(doc);
                timings[i] += (System.nanoTime() - current);
            }
            return functionValues;
        }

        /**
         * 计算表达式
         */
        public float evaluate(float[] floatValues) throws IOException {
            if (floatValues == null || floatValues.length == 0) {
                return 0f;
            }
            float score = EvaluateUtil.eval(this.expression, this.featureNames, floatValues);
            if (log.isDebugEnabled()) {
                log.debug("score: {}, calc: {}, keys: {}, values: {}", score, this.expression, Arrays.toString(this.featureNames), Arrays.toString(floatValues));
            }
            return score;
        }

        @Override
        public String toString(int doc) throws IOException {
            float[] floatValues = getValues(doc);
            Map<String, Float> featureScore = new LinkedHashMap<>();
            for (int i = 0; i < floatValues.length; i++) {
                featureScore.put(featureNames[i], floatValues[i]);
            }
            return "EvaluateLinearModel{" +
                    " expression='" + expression + '\'' +
                    " features=" + featureScore.toString() +
                    '}';
        }


        @Override
        public Explanation explain(int doc) throws IOException {
            final List<Explanation> featureExplanations = new ArrayList<>();
            float[] floatValues = getValues(doc);
            for (int i = 0; i < valFunctions.length; i++) {
                FunctionValues source = valFunctions[i];
                float value = floatValues[i];
                String description = this.featureNames[i] + ": " + source.toString(doc);
                Explanation explanation = Explanation.match(value, description);
                featureExplanations.add(explanation);
            }
            float finalScore = this.evaluate(floatValues);
            return Explanation.match(finalScore, toString(doc) + " model applied to features, sum of:", featureExplanations);
        }
    }


    /**
     * 根据query获取valueSource
     *
     * @param solrQuery query
     * @param req       请求
     * @return 返回ValueSource
     * @throws SyntaxError error
     */
    public static ValueSource getValueSource(String solrQuery, SolrQueryRequest req) throws SyntaxError {
        if (StringUtils.isEmpty(solrQuery)) {
            throw new IllegalStateException("the param of `ValueSourceFeature` is empty!");
        }
        if (req.getOriginalParams() instanceof MultiMapSolrParams) {
            Map map = ((MultiMapSolrParams) req.getOriginalParams()).getMap();
            MacroExpander macroExpander = new MacroExpander(map, true);
            solrQuery = macroExpander.expand(solrQuery);
        }

        QParser reRankParser = QParser.getParser(solrQuery, req);
        Query query = reRankParser.parse();
        if (query == null) {
            return new ConstValueSource(0);
        } else if (query instanceof FunctionQuery) {
            return ((FunctionQuery) query).getValueSource();
        }
        return new QueryValueSource(query, 0);
    }


    public static class FeatureInfo {
        private String name;
        private String query;

        public FeatureInfo(String name, String query) {
            this.name = name;
            this.query = query;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }
    }

}
