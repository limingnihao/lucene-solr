package org.limingnihao.solr.rerank.feature;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TableFeature extends SolrFeature {

    public TableFeature(String name, String q) {
        super(name, q);
    }

    public TableFeature(String name, String q, List<String> fq) {
        super(name, q, fq);
    }

    public TableFeature(String name, String q, String fq) {
        super(name, q, fq);
    }

    @Override
    public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi) throws IOException {
        return new TableFeatureWeight((SolrIndexSearcher) searcher, request, originalQuery, efi, this);
    }
}
