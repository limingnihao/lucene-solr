package org.limingnihao.solr.rerank;

import org.limingnihao.solr.ltr.util.MapUtil;
import org.limingnihao.solr.rerank.feature.*;
import org.limingnihao.solr.rerank.model.ReRankScoringModel;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * rerank weight
 *
 * @author shiming.li
 * @date 2020-04-03
 */
public class ReRankWeight extends Weight {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ReRankScoringModel reRankModel;

    private final FeatureWeight[] featureWeights;

    private final FeatureInfo[] featureInfos;

    private final FeatureDebug featureDebugs[];

    private boolean isDebugRerank;

    public ReRankWeight(Query query, ReRankScoringModel reRankModel, FeatureWeight[] featureWeights, int allFeaturesSize) {
        super(query);
        this.reRankModel = reRankModel;
        this.featureWeights = featureWeights;
        this.featureInfos = new FeatureInfo[allFeaturesSize];
        this.featureDebugs = new FeatureDebug[allFeaturesSize + 1];

        this.isDebugRerank = ReRankContextUtils.isDebugRerank();
        this.setFeaturesInfo();
    }

    private void setFeaturesInfo() {
        for (int i = 0; i < featureWeights.length; ++i) {
            String featName = featureWeights[i].getName();
            int featId = featureWeights[i].getIndex();
            featureInfos[featId] = new FeatureInfo(featName, 0, false, featId);
        }
        if (this.isDebugRerank) {
            this.featureDebugs[this.featureDebugs.length - 1] = new FeatureDebug("eval");
            for (int i = 0; i < featureWeights.length; ++i) {
                this.featureDebugs[i] = new FeatureDebug(featureWeights[i].getName());

                // ltr feature 内部耗时
                if (featureWeights[i] instanceof LtrFeatureWeight) {
                    LtrFeatureWeight ltrFW = (LtrFeatureWeight) featureWeights[i];
                    FeatureDebug[] debugChildren = new FeatureDebug[ltrFW.getFeatureNames().length + 1];
                    for (int j = 0; j < ltrFW.getFeatureNames().length; j++) {
                        debugChildren[j] = new FeatureDebug(ltrFW.getFeatureNames()[j]);
                    }
                    debugChildren[debugChildren.length - 1] = new FeatureDebug("eval");
                    this.featureDebugs[i].setChildren(debugChildren);
                }
            }
        }
    }

    public FeatureInfo[] getFeatureInfos() {
        return featureInfos;
    }

    public FeatureDebug[] getFeatureDebugs() {
        return featureDebugs;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        final ReRankScorer bs = scorer(context);
        bs.iterator().advance(doc);
        final float finalScore = bs.score();

        final List<Explanation> featureExplanations = new ArrayList<>();
        for (int idx = 0; idx < this.featureWeights.length; ++idx) {
            final FeatureWeight f = this.featureWeights[idx];
            if (featureInfos[f.getIndex()].isUsed()) {
                Explanation fe = f.explain(context, doc);
                Explanation e = Explanation.match(featureInfos[f.getIndex()].getValue(), featureInfos[f.getIndex()].getName() + ": " + fe.getDescription(), fe.getDetails());
                featureExplanations.add(e);
            } else {
                Explanation fe = f.explain(context, doc);
                Explanation e = Explanation.match(0, featureInfos[f.getIndex()].getName() + "(unused): " + fe.getDescription());
                featureExplanations.add(e);
            }
        }
        return reRankModel.explain(context, doc, finalScore, featureExplanations);
    }

    @Override
    public ReRankScorer scorer(LeafReaderContext context) throws IOException {
        List<Scorer> scorerList = new ArrayList<>();
        if (isDebugRerank) {
            for (FeatureWeight featureWeight : this.featureWeights) {
                Scorer scorer = featureWeight.scorer(context);
                scorerList.add(scorer);
                featureDebugs[featureWeight.getIndex()].incCost(scorer.iterator().cost());
            }
        } else {
            for (FeatureWeight featureWeight : this.featureWeights) {
                Scorer scorer = featureWeight.scorer(context);
                scorerList.add(scorer);
            }
        }
        return new ReRankScorer(this, scorerList);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    protected void reset() {
        for (int i = 0; i < featureWeights.length; ++i) {
            int featId = featureWeights[i].getIndex();
            float value = featureWeights[i].getDefaultValue();
            featureInfos[featId].setValue(value);
            featureInfos[featId].setUsed(false);
        }
    }

    /**
     * reRank scorer
     * 参考ltr LTRScoringQuery.SparseModelScorer
     */
    public class ReRankScorer extends Scorer {

        private int activeDoc = -1; // The doc that our scorer's are actually at
        private int targetDoc = -1; // The doc we were most recently told to go to

        final private DisiPriorityQueue subScorers;
        final private ScoringQuerySparseIterator itr;

        /**
         * Constructs a Scorer
         */
        protected ReRankScorer(Weight weight, List<Scorer> featureScorers) {
            super(weight);

            subScorers = new DisiPriorityQueue(featureScorers.size());
            for (final Scorer scorer : featureScorers) {
                final DisiWrapper w = new DisiWrapper(scorer);
                subScorers.add(w);
            }

            itr = new ScoringQuerySparseIterator(subScorers);
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
            return 0;
        }

        @Override
        public float score() throws IOException {
            if (isDebugRerank) {
                return featureScoreDebug();
            } else {
                return featureScore();
            }
        }

        /**
         * feature score
         */
        public float featureScore() throws IOException {
            final DisiWrapper topList = subScorers.topList();
            reset();
            if (activeDoc == targetDoc) {
                Map<String, Float> mapGetData = new HashMap<>();
                List<MapFeatureWeight> mapFeatureWeights = new ArrayList<>();
                for (DisiWrapper w = topList; w != null; w = w.next) {
                    final Scorer subScorer = w.scorer;
                    FeatureWeight scFW = (FeatureWeight) subScorer.getWeight();
                    final int featureId = scFW.getIndex();
                    if (scFW instanceof TableFeatureWeight) {
                        String value = ((TableFeatureWeight.TableFeatureScorer) subScorer).value();
                        featureInfos[featureId].setValue(0f);
                        featureInfos[featureId].setUsed(true);
                        MapUtil.analysis(scFW.getName(), value, mapGetData);
                    } else if (scFW instanceof MapFeatureWeight) {
                        mapFeatureWeights.add((MapFeatureWeight) scFW);
                    } else if (scFW instanceof LtrFeatureWeight) {
                        final float score = subScorer.score();
                        featureInfos[featureId].setValue(score);
                        featureInfos[featureId].setUsed(true);
                    } else if (scFW instanceof SolrFeatureWeight) {
                        final float score = subScorer.score();
                        featureInfos[featureId].setValue(score);
                        featureInfos[featureId].setUsed(true);
                    } else {
                        featureInfos[featureId].setValue(0f);
                        featureInfos[featureId].setUsed(false);
                    }
                }
                for (MapFeatureWeight mfw : mapFeatureWeights) {
                    String key = mfw.getKey();
                    int offset = mfw.getOffset();
                    final int featureId = mfw.getIndex();
                    final float score = MapUtil.getFloat(key, offset, mapGetData);
                    featureInfos[featureId].setValue(score);
                    featureInfos[featureId].setUsed(true);
                }
            }
            return reRankModel.score(featureInfos);
        }

        /**
         * feature score, 带统计feature耗时统计
         */
        public float featureScoreDebug() throws IOException {
            final DisiWrapper topList = subScorers.topList();
            reset();
            if (activeDoc == targetDoc) {
                Map<String, Float> mapGetData = new HashMap<>();
                List<MapFeatureWeight> mapFeatureWeights = new ArrayList<>();
                for (DisiWrapper w = topList; w != null; w = w.next) {
                    long current = System.nanoTime();
                    final Scorer subScorer = w.scorer;
                    FeatureWeight scFW = (FeatureWeight) subScorer.getWeight();
                    final int featureId = scFW.getIndex();
                    if (scFW instanceof TableFeatureWeight) {
                        String value = ((TableFeatureWeight.TableFeatureScorer) subScorer).value();
                        featureInfos[featureId].setValue(0f);
                        featureInfos[featureId].setUsed(true);
                        mapGetData.putAll(MapUtil.analysis(scFW.getName(), value));
                    } else if (scFW instanceof MapFeatureWeight) {
                        mapFeatureWeights.add((MapFeatureWeight) scFW);
                    } else if (scFW instanceof LtrFeatureWeight) {
                        final float score = ((LtrFeatureWeight.LtrFeatureScorer) subScorer).scoreDebug(featureDebugs[featureId].getChildren());
                        featureInfos[featureId].setValue(score);
                        featureInfos[featureId].setUsed(true);
                    } else if (scFW instanceof SolrFeatureWeight) {
                        final float score = subScorer.score();
                        featureInfos[featureId].setValue(score);
                        featureInfos[featureId].setUsed(true);
                    } else {
                        featureInfos[featureId].setValue(0f);
                        featureInfos[featureId].setUsed(false);
                    }
                    featureDebugs[featureId].inc((System.nanoTime() - current), 1);
                }
                for (MapFeatureWeight mfw : mapFeatureWeights) {
                    long current = System.nanoTime();

                    String key = mfw.getKey();
                    int offset = mfw.getOffset();
                    final int featureId = mfw.getIndex();
                    final float score = MapUtil.getFloat(key, offset, mapGetData);
                    featureInfos[featureId].setValue(score);
                    featureInfos[featureId].setUsed(true);
                    featureDebugs[featureId].inc((System.nanoTime() - current));
                }
            }
            long current = System.nanoTime();
            float score = reRankModel.score(featureInfos);
            featureDebugs[featureDebugs.length - 1].inc((System.nanoTime() - current), 1);
            return score;
        }

        /**
         * 参考ltr LTRScoringQuery.ScoringQuerySparseIterator
         */
        private class ScoringQuerySparseIterator extends DisjunctionDISIApproximation {

            public ScoringQuerySparseIterator(DisiPriorityQueue subIterators) {
                super(subIterators);
            }

            @Override
            public final int nextDoc() throws IOException {
                if (activeDoc == targetDoc) {
                    activeDoc = super.nextDoc();
                } else if (activeDoc < targetDoc) {
                    activeDoc = super.advance(targetDoc + 1);
                }
                return ++targetDoc;
            }

            @Override
            public final int advance(int target) throws IOException {
                // If target doc we wanted to advance to matches the actual doc
                // the underlying features advanced to, perform the feature
                // calculations,
                // otherwise just continue with the model's scoring process with
                // empty features.
                if (activeDoc < target) {
                    activeDoc = super.advance(target);
                }
                targetDoc = target;
                return targetDoc;
            }
        }

    }

}
