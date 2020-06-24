package org.limingnihao.solr.rerank.feature;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.solr.request.SolrQueryRequest;

import java.io.IOException;
import java.util.Map;

public class MapFeatureWeight extends FeatureWeight {

    private final String key;
    private final int offset;

    /**
     * Sole constructor, typically invoked by sub-classes.
     */
    public MapFeatureWeight(IndexSearcher searcher, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi, MapFeature feature) {
        super(feature, searcher, request, originalQuery, efi, feature);
        this.key = feature.getKey();
        this.offset = feature.getOffset();
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Explanation explanation = Explanation.match(0f, "MapFeature(key=" + key + ", offset=" + this.offset + ")");
        return explanation;
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        Float featureValue = this.getDefaultValue();
        return new ValueFeatureScorer(this, DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS), featureValue);
    }

    public String getKey() {
        return key;
    }

    public int getOffset() {
        return offset;
    }

}