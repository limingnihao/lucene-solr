package org.limingnihao.solr.ltr.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.StrDocValues;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.ltr.store.rest.ManagedModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.macro.MacroExpander;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SyntaxError;
import org.limingnihao.solr.ltr.feature.MapValueFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LtrUtil {

    // 保存加载的ltr的model和feature，用于其他function引用
    public static ManagedFeatureStore fr = null;
    public static ManagedModelStore mr = null;

    protected static final String EXTERNAL_FEATURE_INFO = "efi.";

    /**
     * 获取feature的得分
     * 需要解析StrDocValues
     * 需要解析MapValueFunction
     */
    public static float[] getValues(int doc, FunctionValues[] valFunctions, String[] featureNames) throws IOException {
        float[] functionValues = new float[valFunctions.length];
        Map<String, Float> mapGetData = new HashMap<>();
        for (int i = 0; i < valFunctions.length; i++) {
            FunctionValues vals = valFunctions[i];
            String name = featureNames[i];
            float value = 0f;
            // 如果是map查询的function，获取字符串，并解析成map为后面使用
            if (vals instanceof StrDocValues) {
                String mapValue = vals.strVal(doc);
                MapUtil.analysis(name, mapValue, mapGetData);
            }
            // 如果是MapValueFunction，解析出key和offset，从存在的map中获取
            else if (vals instanceof MapValueFeature.MapValueFunction) {
                String key = ((MapValueFeature.MapValueFunction) vals).getKey();
                int offset = ((MapValueFeature.MapValueFunction) vals).getOffset();
                value = MapUtil.getFloat(key, offset, mapGetData);
            }
            // 其他类型，获取 float 的值
            else {
                value = vals.floatVal(doc);
            }
            functionValues[i] = value;
        }
        return functionValues;
    }


    /**
     * 获取feature的得分.
     * 统计feature耗时
     * 需要解析StrDocValues
     * 需要解析MapValueFunction
     */
    public static float[] getValues(int doc, FunctionValues[] valFunctions, String[] featureNames, long[] total, long[] timing, float[] cost) throws IOException {
        float[] functionValues = new float[valFunctions.length];
        Map<String, Float> mapGetData = new HashMap<>();
        for (int i = 0; i < valFunctions.length; i++) {
            long time = System.nanoTime();
            FunctionValues vals = valFunctions[i];
            String name = featureNames[i];
            float value = 0f;
            // 如果是map查询的function，获取字符串，并解析成map为后面使用
            if (vals instanceof StrDocValues) {
                String mapValue = vals.strVal(doc);
                MapUtil.analysis(name, mapValue, mapGetData);
            }
            // 如果是MapValueFunction，解析出key和offset，从存在的map中获取
            else if (vals instanceof MapValueFeature.MapValueFunction) {
                String key = ((MapValueFeature.MapValueFunction) vals).getKey();
                int offset = ((MapValueFeature.MapValueFunction) vals).getOffset();
                value = MapUtil.getFloat(key, offset, mapGetData);
            }
            // 其他类型，获取 float 的值
            else {
                value = vals.floatVal(doc);
            }
            functionValues[i] = value;
            timing[i] += System.nanoTime() - time;
            cost[i] = vals.cost();
            total[i] += 1;
        }
        return functionValues;
    }

    /**
     * 根据 solrQuery 获取valueSource
     *
     * @param solrQuery solr语法
     * @param req       请求
     * @return 返回ValueSource
     * @throws SyntaxError error
     */
    public static ValueSource getValueSource(String solrQuery, SolrQueryRequest req) throws SyntaxError {
        if (StringUtils.isEmpty(solrQuery)) {
            throw new IllegalStateException("the param of `ValueSourceFeature` is empty!");
        }
        if (req.getOriginalParams() instanceof MultiMapSolrParams) {
            Map map = extracFeatureParams(req.getOriginalParams());
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

    public static MacroExpander getMacroExpander(SolrQueryRequest req) {
        Map map = extracFeatureParams(req.getOriginalParams());
        MacroExpander macroExpander = new MacroExpander(map, true);
        return macroExpander;
    }

    public static Map<String, String[]> extracFeatureParams(SolrParams localParams) {
        final Map<String, String[]> externalFeatureInfo = new HashMap<>();
        for (final Iterator<String> it = localParams.getParameterNamesIterator(); it.hasNext(); ) {
            final String name = it.next();
            if (name.startsWith(EXTERNAL_FEATURE_INFO)) {
                externalFeatureInfo.put(name.substring(EXTERNAL_FEATURE_INFO.length()), new String[]{localParams.get(name)});
            } else {
                externalFeatureInfo.put(name, new String[]{localParams.get(name)});
            }
        }
        return externalFeatureInfo;
    }
}
