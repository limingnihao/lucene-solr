package org.limingnihao.solr.rerank.model;

import org.limingnihao.solr.ltr.util.LtrUtil;
import org.limingnihao.solr.rerank.feature.*;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.common.SolrException;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.search.LTRQParserPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;

public class ReRankLTRModel extends ReRankScoringModel {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private LTRScoringModel ltrScoringModel;

    @Override
    public void loading(String modelName) {
        this.modelName = modelName;

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
        if (this.ltrScoringModel.getFeatures() != null) {
            this.features = new Feature[this.ltrScoringModel.getFeatures().size()];
            for (int i = 0; i < this.ltrScoringModel.getFeatures().size(); i++) {
                org.apache.solr.ltr.feature.Feature feature = this.ltrScoringModel.getFeatures().get(i);
                if (feature instanceof org.apache.solr.ltr.feature.SolrFeature) {
                    org.apache.solr.ltr.feature.SolrFeature ltrSolrFeature = (org.apache.solr.ltr.feature.SolrFeature) feature;
                    String q = ltrSolrFeature.getQ().replaceAll("\\$\\{", "\\$\\{efi.");
                    List<String> filterList = null;
                    if (ltrSolrFeature.getFq() != null) {
                        filterList = new ArrayList<>(ltrSolrFeature.getFq().size());
                        for (String fq : ltrSolrFeature.getFq()) {
                            filterList.add(fq.replaceAll("\\$\\{", "\\$\\{efi."));
                        }
                    }
                    this.features[i] = new SolrFeature(ltrSolrFeature.getName(), q, filterList);
                } else if (feature instanceof org.limingnihao.solr.ltr.feature.MapValueFeature) {
                    org.limingnihao.solr.ltr.feature.MapValueFeature mapValueFeature = (org.limingnihao.solr.ltr.feature.MapValueFeature) feature;
                    String name = mapValueFeature.getName();
                    String key = mapValueFeature.getKey();
                    int offset = mapValueFeature.getOffset();
                    this.features[i] = new MapFeature(name, key, offset);
                }
                features[i].setIndex(i);
            }
        }
    }

    @Override
    public float score(FeatureInfo[] features) {
        float[] values = new float[features.length];
        for (int i = 0; i < features.length; i++) {
            this.features[features[i].getIndex()].setValue(features[i].getValue());
            values[features[i].getIndex()] = features[i].getValue();
        }
        return ltrScoringModel.score(values);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc, float finalScore, List<Explanation> featureExplanations) {
        Explanation modelExplanation = ltrScoringModel.explain(context, doc, finalScore, featureExplanations);

        Explanation featureExplanation = Explanation.match(finalScore, "features details of: ", featureExplanations);

        // 将feature和model的explanation进行合并
        final List<Explanation> functionExplanations = new ArrayList<>();
        functionExplanations.add(featureExplanation);
        functionExplanations.add(modelExplanation);
        return Explanation.match(finalScore, toString() + " details of: ", functionExplanations);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ReRankLTRModel other = (ReRankLTRModel) obj;
        if (modelName == null) {
            if (other.modelName != null) {
                return false;
            }
        } else if (!modelName.equals(other.modelName)) {
            return false;
        }
        if (features == null && other.features != null) {
            return false;
        } else if (features != null && other.features == null) {
            return false;
        } else if (features != null && other.features != null) {
            if (features.length != other.features.length) {
                return false;
            }
            for (int i = 0; i < features.length; i++) {
                if (!features[i].equals(other.features[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = calculateHashCode();
        }
        return hashCode;
    }

    final private int calculateHashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + Objects.hashCode(modelName);
        if (features != null) {
            for (Feature f : features) {
                result = (prime * result) + Objects.hashCode(f);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        Map<String, Float> featureScore = new LinkedHashMap<>();
        for (int i = 0; i < features.length; i++) {
            featureScore.put(features[i].getName(), features[i].getValue());
        }
        return "ReRankLTRModel{" +
                " modelName='" + modelName + '\'' +
                " features=" + featureScore.toString() +
                '}';
    }
}
