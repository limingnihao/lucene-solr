package org.limingnihao.solr.rerank.feature;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.solr.request.SolrQueryRequest;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * map feature info
 *
 * <pre>{
 *   "n": "c1",
 *   "key": "key_map",
 *   "offset": 1
 * }</pre>
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class MapFeature extends Feature {

    private String key;
    private int offset = -1;

    public MapFeature(String name, String key, int offset) {
        this.name = name;
        this.key = key;
        this.offset = offset;
    }

    @Override
    public MapFeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi) {
        return new MapFeatureWeight(searcher, request, originalQuery, efi, this);
    }

    @Override
    protected void validate() throws FeatureException {
        if (key == null || "".equals(key) || offset < 0) {
            throw new FeatureException(getClass().getSimpleName() + ": key or offset must be provided");
        }
    }

    @Override
    public LinkedHashMap<String, Object> paramsToMap() {
        final LinkedHashMap<String, Object> params = new LinkedHashMap<>(3, 1.0f);
        if (key != null) {
            params.put("key", key);
        }
        params.put("offset", offset);
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
        final MapFeature other = (MapFeature) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        return offset == other.offset;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = getClass().getName().hashCode();
        result = (prime * result) + ((name == null) ? 0 : name.hashCode());
        result = (prime * result) + ((key == null) ? 0 : key.hashCode());
        result = (prime * result) + offset;
        return result;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }


}
