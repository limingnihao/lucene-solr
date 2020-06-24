package org.limingnihao.solr.rerank.model;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.limingnihao.solr.rerank.feature.*;
import org.limingnihao.solr.util.EvaluateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * 线性模式解析和执行
 * 支持两种配置方式
 * 第一种：list object
 * 第二种：object
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class ReRankLinearModel extends ReRankScoringModel {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String calculate = "";

    @Override
    public void loading(String source) {
        JSONObject jsonObject = JSONObject.parseObject(source);
        this.modelName = jsonObject.getString("name");
        this.calculate = jsonObject.getString("calculate");
        this.calculate = this.calculate.replaceAll("[ ]", "");

        Object featuresObj = jsonObject.get("features");
        // [
        //    {
        //      "name": "c1",
        //      "query": "{!func}div(1,2)"
        //    },
        //    {
        //      "name": "c2",
        //      "query": "{!func}div(1,2)"
        //    }
        //    {
        //       "name": "c3"
        //       "key": "",
        //       "offset": 1
        //    }
        //  ]
        if (featuresObj instanceof JSONArray) {
            JSONArray featureJsonArray = (JSONArray) featuresObj;
            this.features = new Feature[featureJsonArray.size()];
            for (int i = 0; i < featureJsonArray.size(); i++) {
                JSONObject featureJsonObj = featureJsonArray.getJSONObject(i);
                String name = featureJsonObj.containsKey("n") ? featureJsonObj.getString("n") : featureJsonObj.getString("name");
                if (StringUtils.isBlank(name)) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "feature name is null");
                }
                // ltr feature. model
                if (featureJsonObj.containsKey("model")) {
                    this.features[i] = new LtrFeature(name, featureJsonObj.getString("model"));
                }
                // map feature. key offset
                else if (featureJsonObj.containsKey("key") && featureJsonObj.containsKey("offset")) {
                    this.features[i] = new MapFeature(name, featureJsonObj.getString("key"), featureJsonObj.getInteger("offset"));
                }
                // table feature
                else if (featureJsonObj.containsKey("table")) {
                    this.features[i] = new TableFeature(name, featureJsonObj.getString("table"));
                }
                // solr feature. name query
                else if (featureJsonObj.containsKey("query")) {
                    this.features[i] = new SolrFeature(name, featureJsonObj.getString("query"), featureJsonObj.getString("filterQuery"));
                }
                // solr feature. n q
                else if (featureJsonObj.containsKey("q")) {
                    this.features[i] = new SolrFeature(name, featureJsonObj.getString("q"), featureJsonObj.getString("fq"));
                }
                this.features[i].setIndex(i);
            }
        }
        // {
        //  "features": {
        //    "c1": "{!func}div(1,2)",
        //    "c2": "{!func}div(3,4)"
        //  },
        //  "calculate": "c1+c2"
        // }
        else {
            JSONObject featureJsonObj = (JSONObject) featuresObj;
            SolrFeature[] features = new SolrFeature[featureJsonObj.size()];
            int i = 0;
            for (Map.Entry<String, Object> entry : featureJsonObj.getInnerMap().entrySet()) {
                features[i++] = new SolrFeature(entry.getKey(), entry.getValue().toString());
            }
            this.features = features;
        }
    }

    @Override
    public float score(FeatureInfo[] featureInfos) {
        String[] keys = new String[featureInfos.length];
        float[] values = new float[featureInfos.length];
        for (int i = 0; i < featureInfos.length; i++) {
            this.features[i].setValue(featureInfos[i].getValue());
            keys[i] = featureInfos[i].getName();
            values[i] = featureInfos[i].getValue();
        }
        float score = EvaluateUtil.eval(this.calculate, keys, values);
        return score;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc, float finalScore, List<Explanation> featureExplanations) {
        return Explanation.match(finalScore, toString() + " model applied to features, sum of:", featureExplanations);
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
        final ReRankLinearModel other = (ReRankLinearModel) obj;
        if (modelName == null) {
            if (other.modelName != null) {
                return false;
            }
        } else if (!modelName.equals(other.modelName)) {
            return false;
        }
        if (calculate == null) {
            if (other.calculate != null) {
                return false;
            }
        } else if (!calculate.equals(other.calculate)) {
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
        result = (prime * result) + Objects.hashCode(calculate);
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
        return "ReRankLinearModel{" +
                " modelName='" + modelName + '\'' +
                " calculate='" + calculate + '\'' +
                " features=" + featureScore.toString() +
                '}';
    }
}
