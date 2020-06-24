package org.limingnihao.solr.rerank.feature;

import org.apache.lucene.search.*;
import org.apache.solr.ltr.DocInfo;
import org.apache.solr.request.SolrQueryRequest;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * rerank feature
 */
public abstract class Feature extends Query {
    protected String name;
    protected float value = 0.0f;
    protected float defaultValue = 0.0f;
    protected boolean used;
    protected int index;

    @Override
    public String toString(String field) {
        final StringBuilder sb = new StringBuilder(64); // default initialCapacity of 16 won't be enough
        sb.append(getClass().getSimpleName());
        sb.append(" [name=").append(name);
        final LinkedHashMap<String, Object> params = paramsToMap();
        if (params != null) {
            sb.append(", params=").append(params);
        }
        sb.append(']');
        return sb.toString();
    }

    protected abstract LinkedHashMap<String, Object> paramsToMap();

    protected abstract void validate() throws FeatureException;

    public abstract FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi) throws IOException;

    @Override
    public void visit(QueryVisitor visitor) {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public float getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(float defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isUsed() {
        return used;
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
