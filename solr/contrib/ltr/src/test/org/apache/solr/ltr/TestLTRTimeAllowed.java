package org.apache.solr.ltr;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.solr.SolrTestCase;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.TestLinearModel;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.ltr.search.LTRQParserPlugin;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.RankQuery;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestLTRTimeAllowed extends SolrTestCase {

    private static final SolrResourceLoader solrResourceLoader = new SolrResourceLoader();

    private IndexSearcher searcher;
    private Directory directory;
    private IndexReader reader;

    private Query query;
    private Counter counter;
    private TimeLimitingCollector.TimerThread counterThread;

    /**
     * initializes
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        counter = Counter.newCounter(true);
        counterThread = new TimeLimitingCollector.TimerThread(counter);
        counterThread.start();

        directory = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), directory);
        Document doc = new Document();
        doc.add(newStringField("id", "0", Field.Store.YES));
        doc.add(newTextField("field", "wizard the the the the the oz",
                Field.Store.NO));
        doc.add(newStringField("final-score", "F", Field.Store.YES)); // TODO: change to numeric field
        w.addDocument(doc);

        doc = new Document();
        doc.add(newStringField("id", "1", Field.Store.YES));
        doc.add(newTextField("field", "wizard",
                Field.Store.NO));
        doc.add(newStringField("final-score", "T", Field.Store.YES)); // TODO: change to numeric field
        w.addDocument(doc);

        reader = w.getReader();
        w.close();

        searcher = newSearcher(reader, false, false);

        // Do ordinary BooleanQuery:
        final BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
        bqBuilder.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
        bqBuilder.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
        query = bqBuilder.build();
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        directory.close();
        counterThread.stopTimer();
        counterThread.join();
        super.tearDown();
    }

    private static List<Feature> makeFieldValueFeatures(int[] featureIds,
                                                        String field) {
        final List<Feature> features = new ArrayList<>();
        for (final int i : featureIds) {
            final Map<String, Object> params = new HashMap<String, Object>();
            params.put("field", field);
            final Feature f = Feature.getInstance(solrResourceLoader,
                    FieldValueFeature.class.getName(),
                    "f" + i, params);
            f.setIndex(i);
            features.add(f);
        }
        return features;
    }


    @Test
    public void testSearch() throws Exception {

        // first run the standard query
        MyHitCollector myHc = new MyHitCollector();
        searcher.search(query, myHc);

        assertEquals(2, myHc.hitCount());
        assertEquals("0", searcher.doc(myHc.hits.get(0).doc).get("id"));
        assertEquals("1", searcher.doc(myHc.hits.get(1).doc).get("id"));

    }

    public void testLTRScoringQueryTimeAllowed() throws Exception {
        MyHitCollector myHc = new MyHitCollector();

        final List<Feature> features = makeFieldValueFeatures(new int[]{0, 1, 2},
                "final-score");
        final List<Normalizer> norms =
                new ArrayList<Normalizer>(
                        Collections.nCopies(features.size(), IdentityNormalizer.INSTANCE));
        final List<Feature> allFeatures = makeFieldValueFeatures(new int[]{0, 1,
                2, 3, 4, 5, 6, 7, 8, 9}, "final-score");
        final LTRScoringModel ltrScoringModel = TestLinearModel.createLinearModel("test",
                features, norms, "test", allFeatures, TestLinearModel.makeFeatureWeights(features));

        // Not set timeAllowed
        LTRScoringQuery scoringQuery = new LTRScoringQuery(ltrScoringModel);
        LTRQuery ltrQuery = new LTRQuery(scoringQuery, 100);
        TopDocsCollector topDocsCollector = ltrQuery.getTopDocsCollector(100, null, searcher);
        searcher.search(ltrQuery, topDocsCollector);

        assertEquals(2, myHc.hitCount());
    }


    private Collector createTimedCollector(MyHitCollector hc, long timeAllowed, boolean greedy) {
        TimeLimitingCollector res = new TimeLimitingCollector(hc, counter, timeAllowed);
        res.setGreedy(greedy); // set to true to make sure at least one doc is collected.
        return res;
    }


    // counting collector that can slow down at collect().
    private static class MyHitCollector extends SimpleCollector {
        final List<ScoreDoc> hits = new ArrayList<>();
        private int slowdown = 0;
        private int lastDocCollected = -1;
        private int docBase = 0;
        private int endDoc = 0;
        private Scorable scorer;

        /**
         * amount of time to wait on each collect to simulate a long iteration
         */
        public void setSlowDown(int milliseconds) {
            slowdown = milliseconds;
        }

        public int hitCount() {
            return hits.size();
        }

        public int getLastDocCollected() {
            return lastDocCollected;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(final int doc) throws IOException {
            int docId = doc + docBase;
            if(docId >= endDoc ){
                return;
            }
            if (slowdown > 0) {
                try {
                    Thread.sleep(slowdown);
                } catch (InterruptedException ie) {
                    throw new ThreadInterruptedException(ie);
                }
            }
            hits.add(new ScoreDoc(docId, scorer.score()));
            assert docId >= 0 : " base=" + docBase + " doc=" + doc;
            lastDocCollected = docId;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
            docBase = context.docBase;
            endDoc = context.docBase + context.reader().maxDoc();
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }

    }


    /**
     * A learning to rank Query, will incapsulate a learning to rank model, and delegate to it the rescoring
     * of the documents.
     **/
    public class LTRQuery extends AbstractReRankQuery {
        private final LTRScoringQuery scoringQuery;

        public LTRQuery(LTRScoringQuery scoringQuery, int reRankDocs) {
            super(query, reRankDocs, new LTRRescorer(scoringQuery));
            this.scoringQuery = scoringQuery;
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + (mainQuery.hashCode() + scoringQuery.hashCode() + reRankDocs);
        }

        @Override
        public boolean equals(Object o) {
            return false;
        }

        @Override
        public RankQuery wrap(Query _mainQuery) {
            super.wrap(_mainQuery);
            scoringQuery.setOriginalQuery(_mainQuery);
            return this;
        }

        @Override
        public String toString(String field) {
            return "{!ltr mainQuery='" + mainQuery.toString() + "' scoringQuery='"
                    + scoringQuery.toString() + "' reRankDocs=" + reRankDocs + "}";
        }

        @Override
        protected Query rewrite(Query rewrittenMainQuery) throws IOException {
            return new LTRQuery(scoringQuery, reRankDocs).wrap(rewrittenMainQuery);
        }
    }
}
