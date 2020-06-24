package org.limingnihao.solr.ltr.function;

import com.google.common.collect.Lists;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.solr.common.SolrException;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.search.LTRQParserPlugin;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.limingnihao.solr.ltr.feature.MapValueFeature;
import org.limingnihao.solr.ltr.util.LtrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LtrExtendValueSourceParser extends ValueSourceParser {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    protected static final String name = "ltr_extend";

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String modelName = fp.parseArg();
        // ReRanking Model
        if ((modelName == null) || modelName.isEmpty()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Must provide model in the request");
        }
        if (LtrUtil.mr == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Model store is null");
        }
        final LTRScoringModel ltrScoringModel = LtrUtil.mr.getModel(modelName);
        if (ltrScoringModel == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "cannot find " + LTRQParserPlugin.MODEL + " " + modelName);
        }

        // 解析所有feature为 valueSource
        // mapget函数，也是解析为valueSource
        // 使用mapget结果的feature，是独立的MapValueFeature，解析出key和offset，封装为 MapValueSource
        final List<ValueSource> valueSources = Lists.newArrayList();
        final List<String> featureNames = Lists.newArrayList();
        if (ltrScoringModel.getFeatures() != null) {
            for (final Feature feature : ltrScoringModel.getFeatures()) {
                ValueSource vs = null;
                if (feature instanceof SolrFeature) {
                    vs = LtrUtil.getValueSource(((SolrFeature) feature).getQ(), fp.getReq());
                }
                // map value feature
                else if (feature instanceof MapValueFeature) {
                    vs = ((MapValueFeature) feature).toValueSource();
                }
                valueSources.add(vs);
                featureNames.add(feature.getName());
            }
        }
        return new LtrExtendValueSource(ltrScoringModel, valueSources, featureNames);
    }


    /**
     * value source
     */
    public class LtrExtendValueSource extends ValueSource {
        private final LTRScoringModel ltrScoringModel;
        private final List<ValueSource> valueSources;
        private final List<String> featureNames;

        public LtrExtendValueSource(LTRScoringModel ltrScoringModel, List<ValueSource> valueSources, List<String> featureNames) {
            this.ltrScoringModel = ltrScoringModel;
            this.valueSources = valueSources;
            this.featureNames = featureNames;
        }

        @Override
        public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            FunctionValues[] valFunctions = new FunctionValues[valueSources.size()];
            for (int i = 0, len = valueSources.size(); i < len; i++) {
                valFunctions[i] = this.valueSources.get(i).getValues(context, readerContext);
            }
            return new LtrExtendFunction(readerContext, ltrScoringModel, valFunctions, featureNames.toArray(new String[]{}));
        }

        @Override
        public boolean equals(Object o) {
            if (this.getClass() != o.getClass()) {
                return false;
            }
            LtrExtendValueSource other = (LtrExtendValueSource) o;
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

    /**
     * function 计算取值
     */
    public class LtrExtendFunction extends FunctionValues {
        private final LeafReaderContext readerContext;
        private final LTRScoringModel ltrScoringModel;
        private final FunctionValues[] valFunctions;
        private final String[] featureNames;

        public LtrExtendFunction(LeafReaderContext readerContext, LTRScoringModel ltrScoringModel, FunctionValues[] valFunctions, String[] featureNames) {
            this.readerContext = readerContext;
            this.ltrScoringModel = ltrScoringModel;
            this.valFunctions = valFunctions;
            this.featureNames = featureNames;
        }

        @Override
        public float floatVal(int doc) throws IOException {
            return ltrScoringModel.score(LtrUtil.getValues(doc, valFunctions, featureNames));
        }

        @Override
        public String toString(int doc) throws IOException {
            float[] floatValues = LtrUtil.getValues(doc, valFunctions, featureNames);
            StringBuilder sb = new StringBuilder();
            sb.append(name).append('(');
            for (int i = 0; i < valFunctions.length; i++) {
                FunctionValues source = valFunctions[i];
                float value = floatValues[i];
                if (source instanceof MapValueFeature.MapValueFunction) {
                    sb.append(source.toString(doc) + "=" + value);
                } else {
                    sb.append(source.toString(doc));
                }
                if (i < valFunctions.length - 1) {
                    sb.append(',');
                }
            }
            sb.append(')');
            return sb.toString();
        }

        @Override
        public Explanation explain(int doc) throws IOException {
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
            return Explanation.match(finalScore, name + " details of: ", functionExplanations);
        }
    }

}
