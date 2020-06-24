package org.limingnihao.solr.rerank.feature;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.*;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

public class TableFeatureWeight extends SolrFeatureWeight {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Map context;
    private final ValueSource func;
    private FunctionValues vals;

    public TableFeatureWeight(SolrIndexSearcher searcher, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi, SolrFeature feature) throws IOException {
        super(searcher, request, originalQuery, efi, feature);
        this.context = ValueSource.newContext(searcher);
        this.func = ((FunctionQuery) this.solrQueryWeight.getQuery()).getValueSource();
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        this.vals = func.getValues(this.context, context);
        return new TableFeatureScorer(context, this, vals);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return this.vals.explain(doc);
    }

    /**
     * A 'recipe' for computing a feature
     */
    public class TableFeatureScorer extends Scorer {

        final IndexReader reader;
        final int maxDoc;
        final DocIdSetIterator iterator;
        final FunctionValues vals;

        public TableFeatureScorer(LeafReaderContext context, TableFeatureWeight w, FunctionValues vals) throws IOException {
            super(w);
            this.vals = vals;
            this.reader = context.reader();
            this.maxDoc = reader.maxDoc();
            iterator = DocIdSetIterator.all(context.reader().maxDoc());
        }

        @Override
        public float score() throws IOException {
            return 0f;
        }

        public String value() throws IOException {
            return this.vals.strVal(docID());
        }

        @Override
        public DocIdSetIterator iterator() {
            return iterator;
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return 0;
        }

        @Override
        public int docID() {
            return iterator.docID();
        }

    }
}
