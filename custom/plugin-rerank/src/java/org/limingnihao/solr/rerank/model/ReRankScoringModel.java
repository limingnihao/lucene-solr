package org.limingnihao.solr.rerank.model;

import org.limingnihao.solr.rerank.feature.Feature;
import org.limingnihao.solr.rerank.feature.FeatureInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;

import java.io.Serializable;
import java.util.List;

/**
 * rerank model
 *
 * @author shiming.li
 * @date 2020-04-01
 */
public abstract class ReRankScoringModel implements Serializable {

    protected String modelName;

    protected Feature[] features;

    protected Integer hashCode;

    /**
     * load model
     */
    public abstract void loading(String source);

    /**
     * score
     */
    public abstract float score(FeatureInfo[] features);

    /**
     * Similar to the score() function, except it returns an explanation of how
     * the features were used to calculate the score.
     *
     * @param context             Context the document is in
     * @param doc                 Document to explain
     * @param finalScore          Original score
     * @param featureExplanations Explanations for each feature calculation
     * @return Explanation for the scoring of a document
     */
    public abstract Explanation explain(LeafReaderContext context, int doc, float finalScore, List<Explanation> featureExplanations);

    /**
     * @return the name
     */
    public String getModelName() {
        return modelName;
    }

    /**
     * @return the features
     */
    public Feature[] getFeatures() {
        return features;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(modelName=" + getModelName() + ")";
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
        final ReRankScoringModel other = (ReRankScoringModel) obj;
        if (modelName == null) {
            if (other.modelName != null) {
                return false;
            }
        } else if (!modelName.equals(other.modelName)) {
            return false;
        }
        return true;
    }

}
