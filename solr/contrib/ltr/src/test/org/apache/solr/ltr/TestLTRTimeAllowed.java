package org.apache.solr.ltr;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Counter;
import org.apache.solr.SolrTestCase;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FeatureException;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.TestLinearModel;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrQueryTimeoutImpl;
import org.junit.Test;

import java.io.IOException;
import java.util.*;


public class TestLTRTimeAllowed extends SolrTestCase {

    private static final SolrResourceLoader solrResourceLoader = new SolrResourceLoader();

    private Directory directory;
    private DirectoryReader reader;

    private Query query;
    private LTRScoringQuery ltrScoringQuery;
    private static int total = 0;

    /**
     * initializes
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();

        directory = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), directory);

        Document doc = new Document();
        doc.add(newStringField("id", "0", Field.Store.YES));
        doc.add(newTextField("field", "wizard the the the the the oz",
                Field.Store.NO));
        doc.add(new StoredField("final-score", 1));
        w.addDocument(doc);

        doc = new Document();
        doc.add(newStringField("id", "1", Field.Store.YES));
        // 1 extra token, but wizard and oz are close;
        doc.add(newTextField("field", "wizard the the oz the the the the",
                Field.Store.NO));
        doc.add(new StoredField("final-score", 2));
        w.addDocument(doc);

        doc = new Document();
        doc.add(newStringField("id", "2", Field.Store.YES));
        // 1 extra token, but wizard and oz are close;
        doc.add(newTextField("field", "oz wizard the the the the the the",
                Field.Store.NO));
        doc.add(new StoredField("final-score", 3));
        w.addDocument(doc);

        reader = w.getReader();
        w.close();

        // Do ordinary BooleanQuery:
        final BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
        bqBuilder.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
        bqBuilder.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
        query = bqBuilder.build();

        final List<Feature> features = makeFieldValueFeatures(new int[]{0, 1, 2},
                "final-score");
        final List<Normalizer> norms =
                new ArrayList<Normalizer>(
                        Collections.nCopies(features.size(), IdentityNormalizer.INSTANCE));
        final List<Feature> allFeatures = makeFieldValueFeatures(new int[]{0, 1,
                2, 3, 4, 5, 6, 7, 8, 9}, "final-score");
        final LTRScoringModel ltrScoringModel = TestLinearModel.createLinearModel("test",
                features, norms, "test", allFeatures, TestLinearModel.makeFeatureWeights(features));

        ltrScoringQuery = new LTRScoringQuery(ltrScoringModel);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        directory.close();
        super.tearDown();
    }

    private IndexSearcher getSearcher(IndexReader r) {
        final IndexSearcher searcher = newSearcher(r, false, false);
        return searcher;
    }

    private static List<Feature> makeFieldValueFeatures(int[] featureIds,
                                                        String field) {
        final List<Feature> features = new ArrayList<>();
        for (final int i : featureIds) {
            final Map<String, Object> params = new HashMap<String, Object>();
            params.put("field", field);
            final Feature f = new FieldValueFeature("f" + i, params, field);
            f.setIndex(i);
            features.add(f);
        }
        return features;
    }

    @Test
    public void testRescorer() throws IOException {
        SolrQueryTimeoutImpl.set(1100L);
        IndexSearcher searcher = getSearcher(new ExitableDirectoryReader(reader, SolrQueryTimeoutImpl.getInstance()));

        TopDocs hits = searcher.search(query, 10);
        final LTRRescorer rescorer = new LTRRescorer(ltrScoringQuery);
        hits = rescorer.rescore(searcher, hits, 3);

        // rerank using the field final-score
        assertEquals("1", searcher.doc(hits.scoreDocs[0].doc).get("id"));
        assertEquals("0", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    }

    public static class FieldValueFeature extends Feature {

        private String field;

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        @Override
        public LinkedHashMap<String, Object> paramsToMap() {
            final LinkedHashMap<String, Object> params = defaultParamsToMap();
            params.put("field", field);
            return params;
        }

        @Override
        protected void validate() throws FeatureException {
        }

        public FieldValueFeature(String name, Map<String, Object> params, String field) {
            super(name, params);
            this.field = field;
        }

        @Override
        public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores,
                                          SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi)
                throws IOException {
            return new FieldValueFeatureWeight(searcher, request, originalQuery, efi);
        }

        public class FieldValueFeatureWeight extends FeatureWeight {

            public FieldValueFeatureWeight(IndexSearcher searcher,
                                           SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi) {
                super(FieldValueFeature.this, searcher, request, originalQuery, efi);
            }

            @Override
            public FeatureScorer scorer(LeafReaderContext context) throws IOException {
                total++;
                return new FieldValueFeatureScorer(this, context,
                        DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
            }

            public class FieldValueFeatureScorer extends FeatureScorer {
                LeafReaderContext context = null;

                public FieldValueFeatureScorer(FeatureWeight weight,
                                               LeafReaderContext context, DocIdSetIterator itr) {
                    super(weight, itr);
                    this.context = context;
                }

                @Override
                public float score() throws IOException {
                    final Document document = context.reader().document(itr.docID());
                    final IndexableField indexableField = document.getField(field);
                    if (indexableField == null) {
                        return getDefaultValue();
                    }
                    final Number number = indexableField.numericValue();
                    if (number != null) {
//                        if (number.floatValue() == 3) {
//                            throw new ExitableDirectoryReader.ExitingReaderException("The request took too long to iterate over doc values");
//                        }
                        return number.floatValue();
                    }
                    return getDefaultValue();
                }

                @Override
                public float getMaxScore(int upTo) throws IOException {
                    return Float.POSITIVE_INFINITY;
                }
            }
        }
    }

}
