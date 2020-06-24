package org.limingnihao.solr.rerank.feature;

import org.apache.lucene.search.*;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.macro.MacroExpander;

import java.io.IOException;
import java.util.Map;

/**
 * Weight for a feature
 */
public abstract class FeatureWeight extends Weight {
    final protected IndexSearcher searcher;
    final protected SolrQueryRequest request;
    final protected Map<String, String[]> efi;
    final protected MacroExpander macroExpander;
    final protected Query originalQuery;

    final protected Feature feature;
    protected int count = 0;

    public FeatureWeight(Query q, IndexSearcher searcher, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi, Feature feature) {
        super(q);
        this.searcher = searcher;
        this.request = request;
        this.originalQuery = originalQuery;
        this.efi = efi;
        macroExpander = new MacroExpander(efi, true);
        this.feature = feature;
    }

    public String getName() {
        return this.feature.getName();
    }

    public int getIndex() {
        return this.feature.getIndex();
    }

    public float getDefaultValue() {
        return this.feature.getDefaultValue();
    }

    public int getCount() {
        return count;
    }


    /**
     * 固定值的feature
     */
    public class ValueFeatureScorer extends Scorer {
        final protected String name;
        final protected DocIdSetIterator itr;
        float constScore;

        public ValueFeatureScorer(FeatureWeight weight, DocIdSetIterator itr, float constScore) {
            super(weight);
            this.name = weight.getName();
            this.itr = itr;
            this.constScore = constScore;
        }

        @Override
        public float score() {
            return constScore;
        }

        @Override
        public int docID() {
            return itr.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return itr;
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return constScore;
        }
    }
}
