package org.limingnihao.solr.rerank.feature;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * solr feature info
 * 
 * <pre>{
 *   "n": "c1",
 *   "q": "{!func}query({!edismax q.op=or df='SOU_CONTENT' v='${efi.keyword:-}'}"
 * }</pre>
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class SolrFeature extends Feature {

    protected String q;
    protected List<String> fq;

    public SolrFeature(String name, String q) {
        this.name = name;
        this.q = q;
    }

    public SolrFeature(String name, String q, List<String> fq) {
        this.name = name;
        this.q = q;
        this.fq = fq;
    }

    public SolrFeature(String name, String q, String fq) {
        this.name = name;
        this.q = q;
        this.fq = new ArrayList<>();
        if (StringUtils.isNoneBlank(fq)) {
            Object fqObj = JSONObject.parse(fq);
            if (fqObj != null && fqObj instanceof String) {
                this.fq.add(fqObj.toString());
            } else if (fqObj != null && fqObj instanceof JSONArray) {
                for (int j = 0; j < ((JSONArray) fqObj).size(); j++) {
                    this.fq.add(((JSONArray) fqObj).getString(j));
                }
            }
        }
    }

    @Override
    public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi) throws IOException {
        return new SolrFeatureWeight((SolrIndexSearcher) searcher, request, originalQuery, efi, this);
    }

    @Override
    protected void validate() throws FeatureException {
        if ((q == null || q.isEmpty()) && ((fq == null) || fq.isEmpty())) {
            throw new FeatureException(getClass().getSimpleName() + ": Q or FQ must be provided");
        }
    }

    @Override
    public LinkedHashMap<String, Object> paramsToMap() {
        final LinkedHashMap<String, Object> params = new LinkedHashMap<>(3, 1.0f);
        if (q != null) {
            params.put("q", q);
        }
        if (fq != null) {
            params.put("fq", fq);
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
        final SolrFeature other = (SolrFeature) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (q == null) {
            if (other.q != null) {
                return false;
            }
        } else if (!q.equals(other.q)) {
            return false;
        }
        if (fq == null) {
            if (other.fq != null) {
                return false;
            }
        } else if (fq.size() != other.fq.size()) {
            return false;
        } else {
            for (int i = 0; i < fq.size(); i++) {
                if (!fq.get(i).equals(other.fq.get(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = getClass().getName().hashCode();
        result = (prime * result) + ((name == null) ? 0 : name.hashCode());
        result = (prime * result) + ((q == null) ? 0 : q.hashCode());
        result = (prime * result) + ((fq == null) ? 0 : fq.hashCode());
        return result;
    }

    public String getQ() {
        return q;
    }

    public void setQ(String q) {
        this.q = q;
    }

    public List<String> getFq() {
        return fq;
    }

    public void setFq(List<String> fq) {
        this.fq = fq;
    }
}
