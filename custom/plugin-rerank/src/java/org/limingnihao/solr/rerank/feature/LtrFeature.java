package org.limingnihao.solr.rerank.feature;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;

import java.util.LinkedHashMap;
import java.util.Map;

public class LtrFeature extends Feature {

    private String model;

    public LtrFeature(String name, String model) {
        this.name = name;
        this.model = model;
    }

    @Override
    public LtrFeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi) {
        return new LtrFeatureWeight(searcher, request, originalQuery, efi, this);
    }

    @Override
    protected void validate() throws FeatureException {
        if (model == null || "".equals(model)) {
            throw new FeatureException(getClass().getSimpleName() + ": model must be provided");
        }
    }

    @Override
    public LinkedHashMap<String, Object> paramsToMap() {
        final LinkedHashMap<String, Object> params = new LinkedHashMap<>(1, 1.0f);
        if (model != null) {
            params.put("model", model);
        }
        return params;
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
        final LtrFeature other = (LtrFeature) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (model == null) {
            if (other.model != null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = getClass().getName().hashCode();
        result = (prime * result) + ((name == null) ? 0 : name.hashCode());
        result = (prime * result) + ((model == null) ? 0 : model.hashCode());
        return result;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }
}
