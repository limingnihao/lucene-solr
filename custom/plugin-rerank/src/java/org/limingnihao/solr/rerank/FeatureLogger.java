package org.limingnihao.solr.rerank;

import org.limingnihao.solr.rerank.feature.FeatureInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * feature 保存和获取
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class FeatureLogger {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final char DEFAULT_KEY_VALUE_SEPARATOR = '=';
    public static final char DEFAULT_FEATURE_SEPARATOR = ',';
    public static final String FIRST_SCORE_NAME = "Q";
    public static final String MODEL_SCORE_NAME = "M";

    private final char keyValueSep;
    private final char featureSep;

    /**
     * the name of the cache using for storing the feature value
     **/
    private final String fvCacheName;

    /**
     * 保存到cache的feature name
     */
    private String featureNames;

    /**
     * 保存到cache的doc数量，每个shard
     */
    private int featureDocs = 0;

    /**
     * returns a FeatureLogger that logs the features
     *
     * @return a feature logger for the format specified.
     */
    public static FeatureLogger createFeatureLogger(String fv) {
        if (fv == null) {
            throw new IllegalArgumentException("a fvCacheName must be configured");
        }
        return new FeatureLogger(fv);
    }

    public FeatureLogger(String fvCacheName) {
        this.fvCacheName = fvCacheName;
        this.keyValueSep = DEFAULT_KEY_VALUE_SEPARATOR;
        this.featureSep = DEFAULT_FEATURE_SEPARATOR;
    }

    /**
     * Log will be called every time that the model generates the feature values
     * for a document and a query.
     *
     * @param docid        Solr document id whose features we are saving
     * @param featuresInfo List of all the objects which contain name and value
     *                     for all the features triggered by the result set
     */
    public void log(int docid, ReRankScoringQuery scoringQuery, SolrIndexSearcher searcher, FeatureInfo[] featuresInfo, Float firstScore, Float modelSocre, int hitUpto) {
        if (this.featureDocs == -1 || hitUpto < this.featureDocs) {
            final String featureVector = makeFeatureVector(featuresInfo, firstScore, modelSocre);
            int key = -1;
            Object result = null;
            if (featureVector != null) {
                key = fvCacheKey(scoringQuery, docid);
                result = searcher.cacheInsert(fvCacheName, key, featureVector);
            }
            if (log.isDebugEnabled()) {
                log.debug("log - insert dockid: {}, key: {}, result: {}, featureVector: {}, hitUpto: {}", docid, key, result, featureVector, hitUpto);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("log - stop dockid: {}, hitUpto: {}", docid, hitUpto);
            }
        }
    }

    /**
     * Log will be called every time that the model generates the feature values
     * for a document and a query.
     *
     * @param docid        Solr document id whose features we are saving
     * @param featuresInfo List of all the objects which contain name and value
     *                     for all the features triggered by the result set
     * @return feature values
     */
    public String logAndGet(int docid, ReRankScoringQuery scoringQuery, SolrIndexSearcher searcher, FeatureInfo[] featuresInfo, Float firstScore, Float modelScore) {
        final String featureVector = makeFeatureVector(featuresInfo, firstScore, modelScore);
        int key = -1;
        Object result = false;
        if (featureVector != null) {
            key = fvCacheKey(scoringQuery, docid);
            result = searcher.cacheInsert(fvCacheName, key, featureVector);
        }
        if (log.isDebugEnabled()) {
            log.debug("logAndGet - dockid: {}, key: {}, result: {}, featureVector: {}", docid, key, result, featureVector);
        }
        return featureVector;
    }

    /**
     * populate the document with its feature vector
     *
     * @param docid Solr document id
     * @return String representation of the list of features calculated for docid
     */

    public String getFeatureVector(int docid, ReRankScoringQuery scoringQuery, SolrIndexSearcher searcher) {
        int key = fvCacheKey(scoringQuery, docid);
        String result = (String) searcher.cacheLookup(fvCacheName, key);
        if (log.isDebugEnabled()) {
            log.debug("get - dockid: {}, key: {}, result: {}", docid, key, result);
        }
        return result;
    }

    public String makeFeatureVector(FeatureInfo[] featuresInfo, Float fistScore, Float modelScore) {
        // Allocate the buffer to a size based on the number of features instead of the
        // default 16.  You need space for the name, value, and two separators per feature,
        // but not all the features are expected to fire, so this is just a naive estimate.
        // TODO 根据featureName，进行生成最后保存到cache的内容
        Map<String, Float> featureMap = new LinkedHashMap<>();
        for (FeatureInfo featInfo : featuresInfo) {
            if (featInfo.isUsed()) {
                featureMap.put(featInfo.getName(), featInfo.getValue());
            }
        }
        if (fistScore != null) {
            featureMap.put(FIRST_SCORE_NAME, fistScore);
        }
        if (modelScore != null) {
            featureMap.put(MODEL_SCORE_NAME, modelScore);
        }

        StringBuilder sb = new StringBuilder();
        if (StringUtils.isEmpty(featureNames) || "*".equals(featureNames)) {
            for (Map.Entry<String, Float> entry : featureMap.entrySet()) {
                sb.append(entry.getKey())
                        .append(keyValueSep)
                        .append(entry.getValue())
                        .append(featureSep);
            }
        } else {
            String[] fields = featureNames.split(",");
            for (String field : fields) {
                field = field.trim();
                if (featureMap.containsKey(field)) {
                    sb.append(field)
                            .append(keyValueSep)
                            .append(featureMap.get(field))
                            .append(featureSep);
                }
            }
        }

        final String features = (sb.length() > 0 ?
                sb.substring(0, sb.length() - 1) : "");
        return features;
    }


    private static int fvCacheKey(ReRankScoringQuery scoringQuery, int docid) {
        return scoringQuery.hashCode() + (31 * docid);
    }

    public String getFeatureNames() {
        return featureNames;
    }

    public void setFeatureNames(String featureNames) {
        this.featureNames = featureNames;
    }

    public int getFeatureDocs() {
        return featureDocs;
    }

    public void setFeatureDocs(int featureDocs) {
        this.featureDocs = featureDocs;
    }

    @Override
    public boolean equals(Object other) {
        return (other != null && getClass() == other.getClass()) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(FeatureLogger other) {
        if (!this.fvCacheName.equals(other.fvCacheName)) {
            return false;
        }
        if (this.featureDocs != other.featureDocs) {
            return false;
        }
        if (!this.featureNames.equals(other.featureNames)) {
            return false;
        }
        return true;
    }

}
