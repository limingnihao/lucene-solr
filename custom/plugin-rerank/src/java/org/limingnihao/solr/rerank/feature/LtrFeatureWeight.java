package org.limingnihao.solr.rerank.feature;

import com.google.common.collect.Lists;
import org.apache.lucene.queries.function.docvalues.StrDocValues;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.solr.ltr.feature.FeatureException;
import org.apache.solr.request.macro.MacroExpander;
import org.apache.solr.search.*;
import org.limingnihao.solr.ltr.feature.MapValueFeature;
import org.limingnihao.solr.ltr.util.LtrUtil;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.search.LTRQParserPlugin;
import org.apache.solr.request.SolrQueryRequest;
import org.limingnihao.solr.ltr.util.MapUtil;

import java.io.IOException;
import java.util.*;

public class LtrFeatureWeight extends FeatureWeight {

    private final Map context;
    private final String modelName;

    private final LTRScoringModel ltrScoringModel;

    private List<ValueSource> valueSources = Lists.newArrayList();
    private String[] featureNames;

    /**
     * Sole constructor, typically invoked by sub-classes.
     */
    public LtrFeatureWeight(IndexSearcher searcher, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi, LtrFeature feature) {
        super(feature, searcher, request, originalQuery, efi, feature);
        this.context = ValueSource.newContext(searcher);
        this.modelName = feature.getModel();
        // ReRanking Model
        if ((modelName == null) || modelName.isEmpty()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Must provide model in the request");
        }
        if (LtrUtil.mr == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Model store is null");
        }
        this.ltrScoringModel = LtrUtil.mr.getModel(modelName);
        if (this.ltrScoringModel == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "cannot find " + LTRQParserPlugin.MODEL + " " + modelName);
        }
        if (this.ltrScoringModel.getFeatures() == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "cannot find " + LTRQParserPlugin.MODEL + ": " + modelName + " features");
        }
        try {
            this.loadLtrFeatures((SolrIndexSearcher) searcher, request);
        } catch (SyntaxError | IOException syntaxError) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "cannot load " + LTRQParserPlugin.MODEL + ": " + modelName + " features. " + syntaxError.getMessage());
        }
    }

    public void loadLtrFeatures(SolrIndexSearcher searcher, SolrQueryRequest req) throws SyntaxError, IOException {
        int i = 0;
        this.featureNames = new String[ltrScoringModel.getFeatures().size()];
        for (final Feature feature : ltrScoringModel.getFeatures()) {
            ValueSource vs = null;
            if (feature instanceof SolrFeature) {
                SolrFeature solrFeature = (SolrFeature) feature;
                Query scoreQuery = QParser.getParser(macroExpander.expand(solrFeature.getQ()), req).getQuery();
                // Build the filter queries
                Query filterDocSetQuery = null;
                if (solrFeature.getFq() != null) {
                    List<Query> filterQueries = new ArrayList<>(); // If there are no fqs we just want an empty list
                    for (String fqStr : solrFeature.getFq()) {
                        if (fqStr != null) {
                            fqStr = macroExpander.expand(fqStr);
                            if (fqStr == null) {
                                throw new FeatureException(this.getClass().getSimpleName() + " requires efi parameter that was not passed in request.");
                            }
                            final Query filterQuery = QParser.getParser(fqStr, req).getQuery();
                            if (filterQuery != null) {
                                filterQueries.add(filterQuery);
                            }
                        }
                    }
                    if (filterQueries.isEmpty() == false) { // TODO optimize getDocSet to make this check unnecessary SOLR-14376
                        DocSet filtersDocSet = searcher.getDocSet(filterQueries); // execute
                        if (filtersDocSet != searcher.getLiveDocSet()) {
                            filterDocSetQuery = filtersDocSet.getTopFilter();
                        }
                    }
                }
                Query query = QueryUtils.combineQueryAndFilter(scoreQuery, filterDocSetQuery);
                vs = new QueryValueSource(query, 0);
            }
            // map value feature
            else if (feature instanceof org.limingnihao.solr.ltr.feature.MapValueFeature) {
                vs = ((org.limingnihao.solr.ltr.feature.MapValueFeature) feature).toValueSource();
            }
            valueSources.add(vs);
            featureNames[i++] = feature.getName();
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    public String[] getFeatureNames() {
        return featureNames;
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        FunctionValues[] valFunctions = new FunctionValues[valueSources.size()];
        for (int i = 0, len = valueSources.size(); i < len; i++) {
            valFunctions[i] = this.valueSources.get(i).getValues(this.context, context);
        }
        return new LtrFeatureScorer(context, this, valFunctions);
    }

    @Override
    public Explanation explain(LeafReaderContext readerContext, int doc) throws IOException {
        FunctionValues[] valFunctions = new FunctionValues[valueSources.size()];
        for (int i = 0, len = valueSources.size(); i < len; i++) {
            valFunctions[i] = this.valueSources.get(i).getValues(this.context, readerContext);
        }

        final List<Explanation> featureExplanations = new ArrayList<>();
        float[] floatValues = LtrUtil.getValues(doc, valFunctions, featureNames);
        for (int i = 0; i < valFunctions.length; i++) {
            FunctionValues source = valFunctions[i];
            float value = floatValues[i];
            String description = "";
            if (source instanceof MapValueFeature.MapValueFunction) {
                description = featureNames[i] + ", " + source.toString(doc) + "=" + value;
            } else {
                description = featureNames[i] + ", " + source.toString(doc);
            }
            Explanation explanation = Explanation.match(value, description);
            featureExplanations.add(explanation);
        }
        float finalScore = ltrScoringModel.score(floatValues);

        Explanation modelExplanation = ltrScoringModel.explain(readerContext, doc, finalScore, featureExplanations);

        Explanation featureExplanation = Explanation.match(finalScore, "features details of: ", featureExplanations);

        // 将feature和model的explanation进行合并
        final List<Explanation> functionExplanations = new ArrayList<>();
        functionExplanations.add(featureExplanation);
        functionExplanations.add(modelExplanation);
        return Explanation.match(finalScore, "ltr feature details of: ", functionExplanations);
    }

    /**
     * A 'recipe' for computing a feature
     */
    public class LtrFeatureScorer extends Scorer {

        final String name;
        final FunctionValues[] valFunctions;
        final DocIdSetIterator itr;

        public LtrFeatureScorer(LeafReaderContext readerContext, LtrFeatureWeight weight, FunctionValues[] valFunctions) {
            super(weight);
            this.name = weight.getName();
            this.valFunctions = valFunctions;
            this.itr = DocIdSetIterator.all(readerContext.reader().maxDoc());
        }

        @Override
        public float score() throws IOException {
            int doc = docID();
            float[] floatVal = new float[valFunctions.length];
            Map<String, Float> mapGetData = new HashMap<>();
            for (int i = 0; i < valFunctions.length; i++) {
                FunctionValues vals = valFunctions[i];
                float value = 0f;
                // 如果是map查询的function，获取字符串，并解析成map为后面使用
                if (vals instanceof StrDocValues) {
                    String mapValue = vals.strVal(doc);
                    MapUtil.analysis(featureNames[i], mapValue, mapGetData);
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
                floatVal[i] = value;
            }
            return ltrScoringModel.score(floatVal);
        }

        /**
         * 计算得分，debug
         */
        public float scoreDebug(FeatureDebug[] debug) throws IOException {
            int doc = docID();
            float[] floatVal = new float[valFunctions.length];
            Map<String, Float> mapGetData = new HashMap<>();
            for (int i = 0; i < valFunctions.length; i++) {
                long time = System.nanoTime();
                FunctionValues vals = valFunctions[i];
                float value = 0f;
                // 如果是map查询的function，获取字符串，并解析成map为后面使用
                if (vals instanceof StrDocValues) {
                    String mapValue = vals.strVal(doc);
                    MapUtil.analysis(featureNames[i], mapValue, mapGetData);
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
                floatVal[i] = value;
                debug[i].inc(System.nanoTime() - time, 1);
                debug[i].incCost((long) vals.cost());
            }

            long current = System.nanoTime();
            float score = ltrScoringModel.score(floatVal);
            debug[debug.length - 1].inc((System.nanoTime() - current), 1);
            return score;
        }

        @Override
        public int docID() {
            return itr.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return itr;
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return 0;
        }

    }

}