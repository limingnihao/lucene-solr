package org.limingnihao.solr.queries.function;


import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.search.*;
import org.limingnihao.solr.component.DebugExtendComponent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 重写FunctionQuery、weight、scorer，用于计算 EvaluateValueSource 每个feature的耗时
 *
 * @see ValueSourceScorer
 */
public class FunctionQueryDebug extends Query {

    final ValueSource func;

    /**
     * @param func defines the function to be used for scoring
     */
    public FunctionQueryDebug(ValueSource func) {
        this.func = func;
    }

    /**
     * @return The associated ValueSource
     */
    public ValueSource getValueSource() {
        return func;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public FunctionWeightDebug createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new FunctionWeightDebug(searcher, boost);
    }

    /**
     * Prints a user-readable version of this query.
     */
    @Override
    public String toString(String field) {
        return func.toString();
    }


    /**
     * Returns true if <code>o</code> is equal to this.
     */
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
                func.equals(((FunctionQueryDebug) other).func);
    }

    @Override
    public int hashCode() {
        return classHash() ^ func.hashCode();
    }

    // weight
    public class FunctionWeightDebug extends Weight {
        protected final IndexSearcher searcher;
        protected final float boost;
        protected final Map<Object, Object> context;

        public FunctionWeightDebug(IndexSearcher searcher, float boost) throws IOException {
            super(FunctionQueryDebug.this);
            this.searcher = searcher;
            this.context = ValueSource.newContext(searcher);
            func.createWeight(context, searcher);
            this.boost = boost;
        }

        @Override
        public FunctionScorerDebug scorer(LeafReaderContext context) throws IOException {
            return new FunctionScorerDebug(context, this, boost);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return ((FunctionScorerDebug) scorer(context)).explain(doc);
        }
    }

    // scorer
    public class FunctionScorerDebug extends Scorer {
        final IndexReader reader;
        final FunctionWeightDebug weight;
        final int maxDoc;
        final float boost;
        final DocIdSetIterator iterator;
        final EvaluateLinearValueSourceParser.EvaluateFunctionValues vals;
        final long[] times;//统计耗时

        public FunctionScorerDebug(LeafReaderContext context, FunctionWeightDebug w, float boost) throws IOException {
            super(w);
            this.weight = w;
            this.boost = boost;
            this.reader = context.reader();
            this.maxDoc = reader.maxDoc();
            iterator = DocIdSetIterator.all(context.reader().maxDoc());
            vals = (EvaluateLinearValueSourceParser.EvaluateFunctionValues) func.getValues(weight.context, context);
            times = new long[vals.getFeatureNames().length + 1];
        }

        @Override
        public DocIdSetIterator iterator() {
            return iterator;
        }

        @Override
        public int docID() {
            return iterator.docID();
        }

        @Override
        public float score() throws IOException {
            float[] functionValues = new float[vals.getValFunctions().length];
            for (int i = 0; i < vals.getValFunctions().length; i++) {
                long current = System.nanoTime();
                functionValues[i] = vals.getValFunctions()[i].floatVal(docID());
                times[i] += System.nanoTime() - current;
            }
            long current = System.nanoTime();
            float val = vals.evaluate(functionValues);
            times[times.length - 1] += System.nanoTime() - current;

            // 返回
            if (val >= 0 == false) {
                return 0;
            } else {
                return boost * val;
            }
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return Float.POSITIVE_INFINITY;
        }

        public Explanation explain(int doc) throws IOException {
            Explanation expl = vals.explain(doc);
            if (expl.getValue().floatValue() < 0) {
                expl = Explanation.match(0, "truncated score, max of:", Explanation.match(0f, "minimum score"), expl);
            } else if (Float.isNaN(expl.getValue().floatValue())) {
                expl = Explanation.match(0, "score, computed as (score == NaN ? 0 : score) since NaN is an illegal score from:", expl);
            }

            return Explanation.match(boost * expl.getValue().floatValue(), "FunctionQuery(" + func + "), product of:",
                    vals.explain(doc),
                    Explanation.match(weight.boost, "boost"));
        }

        public long[] getTimes() {
            return times;
        }

        public String[] getNames() {
            return vals.getFeatureNames();
        }

        public FunctionValues[] getFuncs() {
            return vals.getValFunctions();
        }
    }

    // search
    public DebugExtendComponent.ScoreDebugInfo search(List<LeafReaderContext> leaves, FunctionWeightDebug weight) throws IOException {
        DebugExtendComponent.DebugSimpleCollector collector = new DebugExtendComponent.DebugSimpleCollector();
        DebugExtendComponent.ScoreDebugInfo debugInfo = new DebugExtendComponent.ScoreDebugInfo();
        for (LeafReaderContext ctx : leaves) { // search each subreader
            final LeafCollector leafCollector;
            try {
                leafCollector = collector.getLeafCollector(ctx);
            } catch (CollectionTerminatedException e) {
                // there is no doc of interest in this reader context
                // continue with the following leaf
                continue;
            }
            BulkScorer scorer = weight.bulkScorer(ctx);
            if (scorer != null) {
                try {
                    scorer.score(leafCollector, ctx.reader().getLiveDocs());
                    debugInfo.incLeaf();
                    debugInfo.incCost(scorer.cost());
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }

        debugInfo.incTotal(collector.getCount());
        FunctionScorerDebug score = (FunctionScorerDebug) collector.getScorer();
        long[] times = score.getTimes();
        String[] names = score.getNames();
        DebugExtendComponent.ScoreDebugInfo[] childrenDebug = new DebugExtendComponent.ScoreDebugInfo[times.length];
        for (int i = 0; i < times.length; i++) {
            childrenDebug[i] = new DebugExtendComponent.ScoreDebugInfo();
            childrenDebug[i].incTime(times[i]);
            if (i < names.length) {
                childrenDebug[i].setName(names[i]);
                childrenDebug[i].setType("functionValues");
            } else {
                childrenDebug[i].setName("model");
                childrenDebug[i].setType("expression");
            }
        }
        debugInfo.setChildren(childrenDebug);
        return debugInfo;
    }

}
