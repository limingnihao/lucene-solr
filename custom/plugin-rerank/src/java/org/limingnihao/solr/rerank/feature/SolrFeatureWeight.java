package org.limingnihao.solr.rerank.feature;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * solr feature weight
 */
public class SolrFeatureWeight extends FeatureWeight {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    final protected Weight solrQueryWeight;

    public SolrFeatureWeight(SolrIndexSearcher searcher, SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi, SolrFeature feature) throws IOException {
        super(feature, searcher, request, originalQuery, efi, feature);
        try {
            final SolrQueryRequest req = makeRequest(request.getCore(), feature.getQ(), feature.getFq(), efi);
            if (req == null) {
                throw new IOException("ERROR: No parameters provided");
            }

            // Build the scoring query
            Query scoreQuery;
            String qStr = feature.getQ();
            if (qStr == null || qStr.isEmpty()) {
                scoreQuery = null; // ultimately behaves like MatchAllDocsQuery
            } else {
                qStr = macroExpander.expand(qStr);
                if (qStr == null) {
                    throw new FeatureException(this.getClass().getSimpleName() + " requires efi parameter that was not passed in request.");
                }
                scoreQuery = QParser.getParser(qStr, req).getQuery();
                // note: QParser can return a null Query sometimes, such as if the query is a stopword or just symbols
                if (scoreQuery == null) {
                    scoreQuery = new MatchNoDocsQuery(); // debatable; all or none?
                }
            }

            // Build the filter queries
            Query filterDocSetQuery = null;
            if (feature.getFq() != null) {
                List<Query> filterQueries = new ArrayList<>(); // If there are no fqs we just want an empty list
                for (String fqStr : feature.getFq()) {
                    if (fqStr != null) {
                        fqStr = macroExpander.expand(fqStr);
                        if (fqStr == null) {
                            throw new FeatureException(this.getClass().getSimpleName() + " requires efi parameter that was not passed in request.");
                        }
                        final Query filterQuery = QParser.getParser(fqStr, req).getQuery();
                        if (filterQuery != null) {
                            filterQueries.add(filterQuery);
                        }
                    }
                }

                if (filterQueries.isEmpty() == false) { // TODO optimize getDocSet to make this check unnecessary SOLR-14376
                    DocSet filtersDocSet = searcher.getDocSet(filterQueries); // execute
                    if (filtersDocSet != searcher.getLiveDocSet()) {
                        filterDocSetQuery = filtersDocSet.getTopFilter();
                    }
                }
            }

            Query query = combineQueryAndFilter(scoreQuery, filterDocSetQuery);
            this.solrQueryWeight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
        } catch (final SyntaxError e) {
            throw new FeatureException("Failed to parse feature query." + feature, e);
        }
    }

    private LocalSolrQueryRequest makeRequest(SolrCore core, String solrQuery, List<String> fqs, Map<String, String[]> params) {
        final NamedList<String> returnList = new NamedList<String>();
        if ((solrQuery != null) && !solrQuery.isEmpty()) {
            returnList.add(CommonParams.Q, solrQuery);
        }
        if (fqs != null) {
            for (final String fq : fqs) {
                returnList.add(CommonParams.FQ, fq);
            }
        }
        for (Map.Entry<String, String[]> entry : params.entrySet()) {
            returnList.add(entry.getKey(), entry.getValue()[0]);
        }
        if (returnList.size() > 0) {
            LocalSolrQueryRequest solrQueryRequest = new LocalSolrQueryRequest(core, returnList);
            return solrQueryRequest;
        } else {
            return null;
        }
    }

    /**
     * Combines a scoring query with a non-scoring (filter) query.
     * If both parameters are null then return a {@link MatchAllDocsQuery}.
     * If only {@code scoreQuery} is present then return it.
     * If only {@code filterQuery} is present then return it wrapped with constant scoring.
     * If neither are null then we combine with a BooleanQuery.
     */
    public Query combineQueryAndFilter(Query scoreQuery, Query filterQuery) {
        // check for *:* is simple and avoids needless BooleanQuery wrapper even though BQ.rewrite optimizes this away
        if (scoreQuery == null || scoreQuery instanceof MatchAllDocsQuery) {
            if (filterQuery == null) {
                return new MatchAllDocsQuery(); // default if nothing -- match everything
            } else {
                return new ConstantScoreQuery(filterQuery);
            }
        } else {
            if (filterQuery == null || filterQuery instanceof MatchAllDocsQuery) {
                return scoreQuery;
            } else {
                return new BooleanQuery.Builder()
                        .add(scoreQuery, BooleanClause.Occur.MUST)
                        .add(filterQuery, BooleanClause.Occur.FILTER)
                        .build();
            }
        }
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return this.solrQueryWeight.explain(context, doc);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        Scorer solrScorer = null;
        if (this.solrQueryWeight != null) {
            solrScorer = this.solrQueryWeight.scorer(context);
        }

        if (solrScorer != null) {
            return new SolrFeatureScorer(this, solrScorer);
        } else {
            return new ValueFeatureScorer(this, DocIdSetIterator.empty(), this.getDefaultValue());
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    /**
     * A 'recipe' for computing a feature
     */
    public class SolrFeatureScorer extends Scorer {

        final protected String name;
        final private Scorer in;

        public SolrFeatureScorer(SolrFeatureWeight weight, Scorer solrScorer) {
            super(weight);
            this.in = solrScorer;
            name = weight.getName();
        }

        @Override
        public float score() throws IOException {
            count++;
            return in.score();
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return in.iterator();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return in.getMaxScore(upTo);
        }
    }

}
